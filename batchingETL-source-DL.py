import os
import time

from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StringType, TimestampType, StructType
from pyspark.sql.functions import input_file_name, current_timestamp, lit

DEBUG_MODE = True        # instead of a hardcoded value, in future it'll become a config parameter
DEBUG_TOKENIZED_ATTRS = True
ARCHIVE_SOURCE_FILES = True

spark = SparkSession.builder.master("local[*]").appName("car ads batch ETL (source to DL)") \
    .config("spark.driver.extraJavaOptions", "-Duser.timezone=GMT") \
    .config("spark.executor.extraJavaOptions", "-Duser.timezone=GMT") \
    .config("spark.sql.session.timeZone", "UTC") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .config("spark.ui.enabled", False) \
    .config("spark.sql.parser.escapedStringLiterals", True) \
    .config("spark.sql.files.openCostInBytes", "50000") \
    .config("spark.sql.sources.parallelPartitionDiscovery.threshold", 32) \
    .config("spark.executor.memory", "3g") \
    .config("spark.driver.memory", "3g") \
    .getOrCreate()

    # .config("spark.ui.port", "4041") \
    # .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.11:0.18.0") \
    # about partitioning in Spark: https://medium.com/swlh/building-partitions-for-processing-data-files-in-apache-spark-2ca40209c9b7


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
        .add("options", ArrayType(
            StructType()
                .add("category", StringType(), False)
                .add("items", ArrayType(StringType()))
        )) \
        .add("vehicle_history", StringType(), False) \
        .add("comment", StringType(), False) \
        .add("location", StringType(), False) \
        .add("labels", StringType(), False) \
        .add("description", StringType(), False) \
        .add("scrap_date", TimestampType(), False)

    # print(os.getcwd() + "/scrapped_data/cars_com/json/*/*/*/")
    # read source files - go through all the existing files
    source_df = spark \
        .read \
        .format("json") \
        .schema(user_schema) \
        .option("encoding", "UTF-8") \
        .option("multiLine", True) \
        .option("path", "scrapped_data/cars_com/json/*/*/*/") \
        .load() \
        .withColumn("input_file_name", input_file_name()) \
        .withColumn("source_id", lit("cars.com scrapper")) \
        .withColumn("dl_loaded_date", current_timestamp())
    return source_df


def tokenize_data(df):
    # title: 2023 Hyundai Elantra SEL
    # description: 2023, Automatic, 3.8L V6 24V GDI DOHC, Gasoline, 7 mi. | pickup truck, Rear-wheel Drive, Tactical Green
    # price_primary: $19,995
    # price_history: 2/21/23: $22,987 | 3/23/23: $21,987 | 4/07/23: $20,931 | 4/21/23: $19,971
    # labels: Great Deal | $788 under|CPO Warrantied|Home Delivery|Virtual Appointments|VIN: 3KPA25AD0PE512839|Included warranty

    df.createOrReplaceTempView("source_df")

    tokenized_df = spark.sql("""
        select card_id,
               title,
               substring(title, -length(title)+5) as vehicle,
               cast(substring(title, 1, 4) as int) as year,
               /*concat(
                  cast((cast(regexp_replace(price_primary, '[$,]', '') as int) div 10000)*10000 as string),
                  '-',
                  cast((cast(regexp_replace(price_primary, '[$,]', '') as int) div 10000)*10000 + 9999 as string)
               ) as price_range,*/
               case 
                    when try_to_number(price_primary, '$9,999,999,999') is not null
                    then concat(
                            cast((try_to_number(price_primary, '$9,999,999,999') div 10000)*10000 as string),
                            '-',
                            cast((try_to_number(price_primary, '$9,999,999,999') div 10000)*10000 + 9999 as string)
                        ) 
                    else 'Unknown'
               end as price_range,
               price_primary,
               try_to_number(price_primary, '$9,999,999,999') as price_usd,
               price_history,
               case when trim(price_history) <> '' then split(trim(price_history), ' [|] ') end as price_history_split,
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
                   when instr(split(description, ', ')[1], 'Manual') <> 0 or 
                        instr(split(description, ', ')[1], 'M/T') <> 0 then 'Manual'
                   when instr(split(description, ', ')[1], 'Automatic') <> 0 or
                        instr(split(description, ', ')[1], 'A/T') <> 0 then 'Automatic'
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
               comment,
               vehicle_history,
               map_from_arrays(
                   regexp_extract_all(vehicle_history, '([^:]+): ([^|]+)[ ]?[|]?[ ]?', 1),
                   transform(regexp_extract_all(vehicle_history, '([^:]+): ([^|]+)[ ]?[|]?[ ]?', 2), x -> trim(x))
               ) as vehicle_history_map,
               options,
               gallery,
               url,
               scrap_date,
               source_id,
               dl_loaded_date,
               input_file_name as input_file_name,
               replace(input_file_name, 'scrapped_data/', 'archived_data/') as source_file_name               
        from source_df
    """)

    return tokenized_df


def clean_data(df, additional=None):
    additional_actions = additional.split("|")

    bad_data_df = df.where("""
        trim(card_id) in ('', '–') or
        trim(transmission) in ('', '–') or
        trim(engine) in ('', '–') or
        trim(drive) in ('', '–') or
        milage in ('', '–') or
        price_usd is null      
    """)

    if "archive_baddata_source_files" in additional_actions:
        process_source_files(bad_data_df, dest_folder="bad_data", mode="archive_source_files")

    if "delete_baddata_source_files" in additional_actions:
        process_source_files(bad_data_df, dest_folder=None, mode="delete_source_files")

    return df.where("""
            trim(card_id) not in ('', '–') and
            trim(transmission) not in ('', '–') and
            trim(engine) not in ('', '–') and
            trim(drive) not in ('', '–') and
            milage not in ('', '–') and
            price_usd is not null   
        """)


def process_source_files(df, dest_folder, mode="archive_source_files"):
    files_list = df.select("input_file_name").collect()

    # archive already processed source files
    for rec in files_list:
        input_file = rec["input_file_name"]
        if input_file.startswith("file:/"):
            input_file = input_file.replace("file:/", "")
            while input_file[0] == "/":
                input_file = input_file[1:]

            # in case of unix-like os lieve exactly one leading "/" symbol (to have a correct absolute path)
            if os.name != "nt":
                input_file = "/" + input_file

        if mode == "delete_source_files":
            os.remove(input_file)
            continue

        if mode == "archive_source_files":
            archived_file = input_file.replace("scrapped_data/", f"{dest_folder}/")

            folders_chain = archived_file.split("/")[:-1]

            if os.name == "nt":
                starting_folder = folders_chain[0]
            else:
                starting_folder = "/" + folders_chain[0]

            try:
                make_folder(starting_folder, folders_chain[1:])
                if os.path.isfile(archived_file):
                    os.remove(archived_file)
                os.rename(input_file, archived_file)
            except:
                pass


def save_data(df, etl_desc=None, additional=None, dest_format="csv"):
    df = df.coalesce(1)
    # df.persist()

    ETL_configs = [
        {
            "ETL_desc": "debug info - card (direct)",
            "format": "console",
            "attr_list": "card_id;title;price_primary;price_history;location;labels;description;vehicle_history;comment;options;gallery;url;scrap_date;source_id;dl_loaded_date;input_file_name",
            "partitionBy": "",
            "options": {"header": True, "truncate": False},
            "mode": "overwrite",
            "process": DEBUG_MODE
        },
        {
            "ETL_desc": "card (direct)",
            "format": dest_format,
            "attr_list": "card_id;title;price_primary;price_history;location;labels;description;vehicle_history;comment;options;gallery;url;scrap_date;source_id;dl_loaded_date;input_file_name",
            # "partitionBy": "year;price_range",
            "partitionBy": "",
            "options": {"header": True, "path": f"batch_data/cars_com/{dest_format}/car_card_direct"},
            "mode": "overwrite",
            "process": True
        },
        {
            "ETL_desc": "debug info - card (tokenized)",
            "format": "console",
            "attr_list": "card_id;vehicle;year;price_range;price_usd;location;home_delivery;virtual_appointments;included_warranty;VIN;transmission;transmission_type;engine;engine_vol;fuel;mpg;milage;milage_unit;body;drive;color;vehicle_history_map['1-owner vehicle'] as one_owner;vehicle_history_map['Accidents or damage'] as accidents_or_damage;vehicle_history_map['Clean title'] as clean_title;vehicle_history_map['Personal use only'] as personal_use_only;comment;scrap_date;source_id;dl_loaded_date",
            "partitionBy": "",
            "options": {"header": True, "truncate": False},
            "mode": "overwrite",
            "process": DEBUG_MODE
        },
        {
            "ETL_desc": "card (tokenized)",
            "format": dest_format,
            "attr_list": "card_id;vehicle;year;price_usd;location;home_delivery;virtual_appointments;included_warranty;VIN;transmission;transmission_type;engine;engine_vol;fuel;mpg;milage;milage_unit;body;drive;color;vehicle_history_map['1-owner vehicle'] as one_owner;vehicle_history_map['Accidents or damage'] as accidents_or_damage;vehicle_history_map['Clean title'] as clean_title;vehicle_history_map['Personal use only'] as personal_use_only;comment;scrap_date;source_id;dl_loaded_date",
            # "partitionBy": "year;price_range",
            "partitionBy": "",
            "options": {"header": True, "path": f"batch_data/cars_com/{dest_format}/car_card_tokenized"},
            "mode": "overwrite",
            "process": True
        },
        {
            "ETL_desc": "debug info - card_options",
            "format": "console",
            "attr_list": "card_id;explode(options) as option_group;scrap_date;source_id;dl_loaded_date|card_id;option_group.category;explode(option_group.items) as item;scrap_date;source_id;dl_loaded_date",
            # "partitionBy": "category",
            "partitionBy": "",
            "options": {"header": True, "truncate": False},
            "mode": "overwrite",
            "process": True
        },
        {
            "ETL_desc": "card_options",
            "format": dest_format,
            "attr_list": "card_id;explode(options) as option_group;scrap_date;source_id;dl_loaded_date|card_id;option_group.category;explode(option_group.items) as item;scrap_date;source_id;dl_loaded_date",
            # "partitionBy": "category",
            "partitionBy": "",
            "options": {"header": True, "truncate": False, "path": f"batch_data/cars_com/{dest_format}/car_card_options"},
            "mode": "overwrite",
            "process": True
        },
        {
            "ETL_desc": "debug info - card_info",
            "format": "console",
            "attr_list": "card_id;url;source_file_name;scrap_date;source_id;dl_loaded_date",
            "partitionBy": "",
            "options": {"header": True, "truncate": False},
            "mode": "overwrite",
            "process": True
        },
        {
            "ETL_desc": "card_info",
            "format": dest_format,
            "attr_list": "card_id;url;source_file_name;scrap_date;source_id;dl_loaded_date",
            "partitionBy": "",
            "options": {"header": True, "truncate": False, "path": f"batch_data/cars_com/{dest_format}/car_card_info"},
            "mode": "overwrite",
            "process": True
        },
        {
            "ETL_desc": "debug info - card_gallery",
            "format": "console",
            "attr_list": "card_id;posexplode(gallery) as (num, url);scrap_date;source_id;dl_loaded_date",
            "partitionBy": "",
            "options": {"header": True, "truncate": False},
            "mode": "overwrite",
            "process": True
        },
        {
            "ETL_desc": "card_gallery",
            "format": dest_format,
            "attr_list": "card_id;posexplode(gallery) as (num, url);scrap_date;source_id;dl_loaded_date",
            "partitionBy": "",
            "options": {"header": True, "path": f"batch_data/cars_com/{dest_format}/car_card_gallery"},
            "mode": "overwrite",
            "process": True
        },
        {
            "ETL_desc": "debug info - card_price_history",
            "format": "console",
            "attr_list": "card_id;explode(price_history_split) as price_change;scrap_date;source_id;dl_loaded_date|card_id;to_date(split(price_change, ': ')[0], 'MM/dd/yy') as date;to_number(split(price_change, ': ')[1], '$9,999,999,999') as price;scrap_date;source_id;dl_loaded_date",
            "partitionBy": "",
            "options": {"header": True, "truncate": False},
            "mode": "overwrite",
            "process": True
        },
        {
            "ETL_desc": "card_price_history",
            "format": dest_format,
            "attr_list": "card_id;explode(price_history_split) as price_change;scrap_date;source_id;dl_loaded_date|card_id;to_date(split(price_change, ': ')[0], 'MM/dd/yy') as date;to_number(split(price_change, ': ')[1], '$9,999,999,999') as price;scrap_date;source_id;dl_loaded_date",
            "partitionBy": "",
            "options": {"header": True, "path": f"batch_data/cars_com/{dest_format}/card_price_history"},
            "mode": "overwrite",
            "process": True
        }
    ]

    additional_actions = additional.split("|")

    for etl_config in ETL_configs:
        if not etl_config["process"] or (etl_desc not in [None, "", '*'] and etl_config["ETL_desc"] not in etl_desc.split(";")):
            continue

        stage = df
        for attrs_to_select in etl_config["attr_list"].split("|"):
            stage = stage.selectExpr(attrs_to_select.split(";"))

        if "debug info" in additional_actions:
            stage.show(truncate=False)

        stage = stage.write \
            .format(etl_config["format"]) \
            .options(**etl_config["options"]) \
            .mode(etl_config["mode"])

        if etl_config["partitionBy"] != "":
            stage = stage.partitionBy(etl_config["partitionBy"].split(";"))

        if etl_config["format"] == "console":
            print(f"{etl_config['ETL_desc']}:")

        stage.save()


    if "archive_source_files" in additional_actions:
        process_source_files(df, dest_folder="archived_data", mode="archive_source_files")
    if "delete_source_files" in additional_actions:
        process_source_files(df, dest_folder="archived_data", mode="delete_source_files")
    if "reopen_df" in additional_actions:
        # reopen source data - that time read from just saved file
        df = spark \
            .read \
            .format(dest_format) \
            .option("encoding", "UTF-8") \
            .option("multiLine", True) \
            .option("path", f"batch_data/cars_com/{dest_format}/car_card") \
            .load()

    # df.unpersist()

    return df


def main():
    # start_time = time.time()

    # extract data from source files
    source_df = create_input_df()

    # print(f"\ntime: {time.strftime('%X', time.gmtime(time.time() - start_time))}")
    # load data without any processing but with new audit attributes added
    source_df = save_data(source_df,
                          etl_desc="card (direct)",
                          additional="reopen_df",
                          dest_format="parquet")

    # print(f"\ntime: {time.strftime('%X', time.gmtime(time.time() - start_time))}")
    # transform data
    tokenized_df = tokenize_data(source_df)
    # cleaned_df = clean_data(tokenized_df, additional="archive_baddata_source_files")
    cleaned_df = clean_data(tokenized_df, additional="")

    # print(f"\ntime: {time.strftime('%X', time.gmtime(time.time() - start_time))}")
    # load tokenized & cleaned data
    # save_data(cleaned_df,
    #           etl_desc="card (tokenized);card_options;card_info;card_gallery;card_price_history",
    #           additional="archive_source_files",
    #           dest_format="csv")
    save_data(cleaned_df,
              etl_desc="card (tokenized);card_options;card_info;card_gallery;card_price_history",
              additional="debug info",
              dest_format="parquet")

    # print(f"\ntime: {time.strftime('%X', time.gmtime(time.time() - start_time))}")


if __name__ == "__main__":
    main()
