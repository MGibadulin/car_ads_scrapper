import os

from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StringType, StructType
from pyspark.sql.functions import input_file_name

DEBUG_MODE = True        #instead of a hardcoded value, in future it'll become a console argument
DEBUG_TOKENIZED_ATTRS = True

spark = SparkSession.builder.master("local[*]").appName('car ads batch ETL (source to DL)') \
    .config('spark.driver.extraJavaOptions', '-Duser.timezone=GMT') \
    .config('spark.executor.extraJavaOptions', '-Duser.timezone=GMT') \
    .config('spark.sql.session.timeZone', 'UTC') \
    .config("spark.ui.port", "4041") \
    .config("spark.sql.parser.escapedStringLiterals", True) \
    .getOrCreate()
# sc.setLogLevel(newLevel)

def make_folder(start_folder, subfolders_chain):
    folder = start_folder
    for subfolder in subfolders_chain:
        folder += "/" + subfolder
        if not os.path.isdir(folder):
            os.mkdir(folder)

    return folder


def create_input_df():
    # schema of the source json files
    user_schema = StructType() \
        .add("gallery", ArrayType(StringType())) \
        .add("card_id", StringType(), False) \
        .add("url", StringType(), False) \
        .add("title", StringType(), False) \
        .add("price_primary", StringType(), False) \
        .add("price_history", StringType(), False) \
        .add("options", ArrayType( \
        StructType() \
            .add("category", StringType(), False) \
            .add("items", ArrayType(StringType())) \
        )) \
        .add("vehicle_history", StringType(), False) \
        .add("comment", StringType(), False) \
        .add("location", StringType(), False) \
        .add("labels", StringType(), False) \
        .add("description", StringType(), False) \
        .add("scrap_date", StringType(), False)

    # print(os.getcwd() + "/scrapped_cards/CARS_COM/JSON/*/*/*/")
    #read source files - go through all the existing files
    source_df = spark \
        .read \
        .format("json") \
        .schema(user_schema) \
        .option("encoding", "UTF-8") \
        .option("multiLine", True) \
        .option("path", os.path.abspath(os.getcwd()) + "/scrapped_cards/CARS_COM/JSON/*/*/*/") \
        .load() \
        .withColumn("input_file_name", input_file_name())

    return source_df

def tokenize_data(df):
    # title: 2023 Hyundai Elantra SEL
    # description: 2023, Automatic, 3.8L V6 24V GDI DOHC, Gasoline, 7 mi. | pickup truck, Rear-wheel Drive, Tactical Green
    # price_primary: $19,995
    # price_history: 2/21/23: $22,987 | 3/23/23: $21,987 | 4/07/23: $20,931 | 4/21/23: $19,971
    # labels: Great Deal | $788 under|CPO Warrantied|Home Delivery|Virtual Appointments|VIN: 3KPA25AD0PE512839|Included warranty

    df.createOrReplaceTempView("source_df")

    transformed_df = spark.sql("""
        select card_id,
               title,
               substring(title, -length(title)+5) as vehicle,
               cast(substring(title, 1, 4) as int) as year,
               /*concat(
                  cast((cast(regexp_replace(price_primary, '[$,]', '') as int) div 10000)*10000 as string),
                  '-',
                  cast((cast(regexp_replace(price_primary, '[$,]', '') as int) div 10000)*10000 + 9999 as string)
               ) as price,*/
               concat(
                  cast((to_number(price_primary, '$999,999') div 10000)*10000 as string),
                  '-',
                  cast((to_number(price_primary, '$999,999') div 10000)*10000 + 9999 as string)
               ) as price_range,
               price_primary,
               to_number(price_primary, '$999,999') as price_usd,
               price_history,
               location,
               labels,
               case 
                   when instr(labels, 'Home Delivery') <> 0 then 'Y'
                   else 'N'
               end as home_delivery,
               case 
                   when instr(labels, 'Virtual Appointments') <> 0 then 'Y'
                   else 'N'
               end as virtual_appointments,
               case 
                   when instr(labels, 'Included warranty') <> 0 then 'Y'
                   else 'N'
               end as included_warranty,               
               case 
                   when instr(labels, 'VIN: ') <> 0 then split(substring(labels, -length(labels)+instr(labels, 'VIN: ')+4), '[|]')[0]
                   else ''
               end as VIN,
               description,
               split(description, ', ')[1] as transmission,
               case 
                   when instr(split(description, ', ')[1], 'IVT') <> 0 then 'IVT'
                   when instr(split(description, ', ')[1], 'CVT') <> 0 then 'CVT'
                   when instr(split(description, ', ')[1], 'DCT') <> 0 then 'DCT'
                   when instr(split(description, ', ')[1], 'DSG') <> 0 then 'DSG'
                   when instr(split(description, ', ')[1], 'Manual') <> 0 then 'Manual'
                   when instr(split(description, ', ')[1], 'Automatic') <> 0 then 'Automatic'
                   else ''
               end as transmission_type,
               split(description, ', ')[2] as engine, 
               round(cast(regexp_extract(split(description, ', ')[2], '(\d+[.]?\d?)[ ]?L', 1) as float)*1000, 0) as engine_vol,             
               split(split(description, ', ')[3], ' ')[0] as fuel,
               regexp_extract(split(description, ', ')[3], '[(](.*)[)]$', 1) as mpg,
               regexp_extract(replace(split(split(description, ', ')[4], '[|]')[0], ' ', ''), '(\d+)', 1) as milage,
               'mi.' as milage_unit,
               split(split(description, ', ')[4], ' [|] ')[1] as body,
               split(description, ', ')[5] as drive,
               split(description, ', ')[6] as color,
               replace(comment, '\n', ' | ') as comment,
               vehicle_history,
               map_from_arrays(
                   regexp_extract_all(vehicle_history, '([^:]+): ([^|]+)[ ]?[|]?[ ]?', 1),
                   transform(regexp_extract_all(vehicle_history, '([^:]+): ([^|]+)[ ]?[|]?[ ]?', 2), x -> trim(x))
               ) as vehicle_history_map,
               options,
               gallery,
               url,
               scrap_date,
               regexp_replace(input_file_name, 'file:[/]+', '') as input_file_name,
               replace(regexp_replace(input_file_name, 'file:[/]+', ''), 'scrapped_cards/', 'archived_cards/') as source_file_name,
               current_timestamp() as loaded_date
        from source_df
    """)

    return transformed_df


def clean_data(df):
    return df.where("""
            trim(transmission) not in ('', '–') and
            trim(engine) not in ('', '–') and
            trim(drive) not in ('', '–') and
            milage not in ('', '–')
    """)


def save_data(df):
    df = df.repartition(1)
    df.persist()

    ETL_configs = [
        {
            "ETL_desc": "debug info",
            "format": "console",
            "attr_list": #"card_id;title;price_primary;price_history;location;labels;description;vehicle_history;comment;scrap_date;loaded_date" \
                    "card_id;vehicle;year;price_range;price_usd;price_history;location;home_delivery;virtual_appointments;included_warranty;VIN;description;transmission;transmission_type;engine;engine_vol;fuel;mpg;milage;milage_unit;body;drive;color;vehicle_history_map['1-owner vehicle'] as one_owner;vehicle_history_map['Accidents or damage'] as accidents_or_damage;vehicle_history_map['Clean title'] as clean_title;vehicle_history_map['Personal use only'] as personal_use_only;scrap_date;source_file_name;loaded_date" \
                    if DEBUG_TOKENIZED_ATTRS else \
                         "card_id,title,price_primary,location,labels,description,scrap_date",
            "partitionBy": "",
            "options": {"header": True, "truncate": False},
            "mode": "append",
            "process": DEBUG_MODE
        },
        {
            "ETL_desc": "card csv (direct)",
            "format": "csv",
            "attr_list": "card_id;title;price_primary;price_history;location;labels;description;vehicle_history;comment;scrap_date;loaded_date",
            # "partitionBy": "year;price_range",
            "partitionBy": "",
            "options": {"header": True, "path": "batch/CARS_COM/CSV/car_card"},
            "mode": "append",
            "process": True
        },
        {
            "ETL_desc": "card csv (tokenized)",
            "format": "csv",
            "attr_list": "card_id;vehicle;year;price_range;price_usd;price_history;location;home_delivery;virtual_appointments;included_warranty;VIN;transmission;transmission_type;engine;engine_vol;fuel;mpg;milage;milage_unit;body;drive;color;vehicle_history_map['1-owner vehicle'] as one_owner;vehicle_history_map['Accidents or damage'] as accidents_or_damage;vehicle_history_map['Clean title'] as clean_title;vehicle_history_map['Personal use only'] as personal_use_only;comment;scrap_date;source_file_name;loaded_date",
            # "partitionBy": "year;price_range",
            "partitionBy": "",
            "options": {"header": True, "path": "batch/CARS_COM/CSV/car_card_tokenized"},
            "mode": "append",
            "process": True
        },
        {
            "ETL_desc": "card_options csv",
            "format": "csv",
            "attr_list": "card_id;explode(options) as option_group;scrap_date;loaded_date|card_id;option_group.category;explode(option_group.items) as item;scrap_date;loaded_date",
            # "partitionBy": "category",
            "partitionBy": "",
            "options": {"header": True, "truncate": False, "path": "batch/CARS_COM/CSV/car_card_options"},
            "mode": "append",
            "process": True
        },
        {
            "ETL_desc": "card_info csv",
            "format": "csv",
            "attr_list": "card_id;url;source_file_name;scrap_date;loaded_date",
            "partitionBy": "",
            "options": {"header": True, "path": "batch/CARS_COM/CSV/car_card_info"},
            "mode": "append",
            "process": True
        },
        {
            "ETL_desc": "card_gallery csv",
            "format": "csv",
            "attr_list": "card_id;posexplode(gallery) as (num, url);scrap_date;loaded_date",
            "partitionBy": "",
            "options": {"header": True, "path": "batch/CARS_COM/CSV/car_card_gallery"},
            "mode": "append",
            "process": True
        }
    ]

    for etl_config in ETL_configs:
        if not etl_config["process"]:
            continue

        stage = df
        for attrs_to_select in etl_config["attr_list"].split("|"):
            stage = stage.selectExpr(attrs_to_select.split(";"))

        stage = stage.write \
            .format(etl_config["format"]) \
            .options(**etl_config["options"]) \
            .mode(etl_config["mode"])

        if etl_config["partitionBy"] != "":
            stage = stage.partitionBy(etl_config["partitionBy"].split(";"))

        stage.save()


    files_list = df.select("input_file_name", "source_file_name").collect()

    df.unpersist()


def main():
    #extract
    source_df = create_input_df()
    #transform
    tokenized_df = tokenize_data(source_df)
    cleaned_df = clean_data(tokenized_df)
    #load
    save_data(cleaned_df)



if __name__ == "__main__":
    main()
