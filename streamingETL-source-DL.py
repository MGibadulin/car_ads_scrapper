import os

from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StringType, StructType
from pyspark.sql.functions import input_file_name

DEBUG_MODE = True        #instead of a hardcoded value, in future it'll become a console argument
DEBUG_TOKENIZED_ATTRS = True

spark = SparkSession.builder.master("local[*]").appName('file streaming example') \
    .config('spark.driver.extraJavaOptions', '-Duser.timezone=GMT') \
    .config('spark.executor.extraJavaOptions', '-Duser.timezone=GMT') \
    .config('spark.sql.session.timeZone', 'UTC') \
    .config("spark.ui.port", "4041") \
    .getOrCreate()
# sc.setLogLevel(newLevel)

def make_folder(start_folder, subfolders_chain):
    folder = start_folder
    for subfolder in subfolders_chain:
        folder += "/" + subfolder
        if not os.path.isdir(folder):
            os.mkdir(folder)

    return folder


def create_input_file_stream():
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
    #source file streaming - go through all the existing files, then wait for new files appeared
    source_df = spark \
        .readStream \
        .option("maxFilesPerTrigger", 10) \
        .format("json") \
        .schema(user_schema) \
        .option("encoding", "UTF-8") \
        .option("multiLine", True) \
        .option("path", os.path.abspath(os.getcwd()) + "/scrapped_cards/CARS_COM/JSON/*/*/*/") \
        .load() \
        .withColumn("input_file_name", input_file_name())

    return source_df

def transform_stream_data(incoming_stream_df):
    # title: 2023 Hyundai Elantra SEL
    # description: 2023, Automatic, 3.8L V6 24V GDI DOHC, Gasoline, 7 mi. | pickup truck, Rear-wheel Drive, Tactical Green
    # price_primary: $19,995
    # price_history: 2/21/23: $22,987 | 3/23/23: $21,987 | 4/07/23: $20,931 | 4/21/23: $19,971
    # labels: Great Deal | $788 under|CPO Warrantied|Home Delivery|Virtual Appointments|VIN: 3KPA25AD0PE512839|Included warranty

    incoming_stream_df.createOrReplaceTempView("source_micro_batch")

    transformed_df = spark.sql("""
        select card_id,
               title,
               substring(title, 6, length(title) - 5) as vehicle,
               cast(substring(title, 1, 4) as int) as year,
               concat(
                  cast((cast(replace(replace(price_primary, '$', ''), ',', '') as int) div 10000)*10000 as string),
                  '-',
                  cast((cast(replace(replace(price_primary, '$', ''), ',', '') as int) div 10000)*10000 + 9999 as string)
               ) as price,
               price_primary,
               cast(replace(replace(price_primary, '$', ''), ',', '') as int) as price_usd,
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
                   when instr(labels, 'VIN: ') <> 0 then split(right(labels, length(labels)-instr(labels, 'VIN: ')-4), '[|]')[0]
                   else ''
               end as VIN,
               description,
               split(description, ', ')[1] as transmission,
               split(description, ', ')[2] as engine,               
               split(split(description, ', ')[3], ' ')[0] as fuel,
               regexp_extract(split(description, ', ')[3], '[(](.*)[)]$', 1) as mpg,
               replace(replace(split(split(description, ', ')[4], '[|]')[0], ' '), 'mi.', '') as milage,
               'mi.' as milage_unit,
               split(split(description, ', ')[4], ' [|] ')[1] as body,
               split(description, ', ')[5] as drive,
               split(description, ', ')[6] as color,
               comment,
               vehicle_history,
               options,
               gallery,
               url,
               scrap_date,
               replace(replace(input_file_name, 'file:///', ''), 'file:/', '') as input_file_name,
               replace(replace(replace(input_file_name, 'file:///', ''), 'file:/', ''), 'scrapped_cards/', 'archived_cards/') as source_file_name,
               current_timestamp() as loaded_date
        from source_micro_batch
    """)

    return transformed_df


def save_batch_data(micro_batch_df, epoch_id):
    micro_batch_df = micro_batch_df.repartition(1)
    micro_batch_df.persist()

    ETL_configs = [
        {

            "ETL_desc": "debug info",
            "format": "console",
            "attr_list": "card_id;vehicle;year;price;location;labels;home_delivery;virtual_appointments;included_warranty;VIN;transmission;engine;fuel;mpg;milage;milage_unit;body;drive;color;scrap_date" \
                         if DEBUG_TOKENIZED_ATTRS else \
                         "card_id,title,price_primary,location,labels,description,scrap_date",
            "partitionBy": "",
            "options": {"header": True, "truncate": False},
            "mode": "append",
            "process": DEBUG_MODE
        },
        {
            "ETL_desc": "card csv",
            "format": "csv",
            "attr_list": "card_id;vehicle;year;price;price_usd;location;labels;transmission;engine;fuel;milage;body;drive;color;comment;scrap_date;loaded_date",
            "partitionBy": "year;price",
            "options": {"header": True, "path": "stream/CARS_COM/CSV/car_card"},
            "mode": "append",
            "process": True
        },
        {
            "ETL_desc": "card_options csv",
            "format": "csv",
            "attr_list": "card_id;explode(options) as option_group;scrap_date;loaded_date|card_id;option_group.category;explode(option_group.items) as item;scrap_date;loaded_date",
            "partitionBy": "category",
            "options": {"header": True, "truncate": False, "path": "stream/CARS_COM/CSV/car_card_options"},
            "mode": "append",
            "process": True
        },
        {
            "ETL_desc": "card_info csv",
            "format": "csv",
            "attr_list": "card_id;url;source_file_name;scrap_date;loaded_date",
            "partitionBy": "",
            "options": {"header": True, "path": "stream/CARS_COM/CSV/car_card_info"},
            "mode": "append",
            "process": True
        },
        {
            "ETL_desc": "card_gallery csv",
            "format": "csv",
            "attr_list": "card_id;posexplode(gallery) as (num, url);scrap_date;loaded_date",
            "partitionBy": "",
            "options": {"header": True, "path": "stream/CARS_COM/CSV/car_card_gallery"},
            "mode": "append",
            "process": True
        }
    ]

    for etl_config in ETL_configs:
        if not etl_config["process"]:
            continue

        stage = micro_batch_df
        for attrs_to_select in etl_config["attr_list"].split("|"):
            stage = stage.selectExpr(attrs_to_select.split(";"))

        stage = stage.write \
            .format(etl_config["format"]) \
            .options(**etl_config["options"]) \
            .mode(etl_config["mode"])

        if etl_config["partitionBy"] != "":
            stage = stage.partitionBy(etl_config["partitionBy"].split(";"))

        stage.save()


    files_list = micro_batch_df.select("input_file_name", "source_file_name").collect()

    micro_batch_df.unpersist()

    #archive the already processed source files
    for rec in files_list:
        input_file = rec["input_file_name"]     #.replace("file:/", "")
        archived_file = rec["source_file_name"] #input_file.replace("scrapped_cards/", "archived_cards/")

        folders_chain = archived_file.split("/")[:-1]
        try:
            make_folder(folders_chain[0], folders_chain[1:])
            os.rename(input_file, archived_file)
        except:
            pass


def main():
    #extract
    source_df = create_input_file_stream()
    #transform
    transformed_df = transform_stream_data(source_df)
    #load
    stream_df = transformed_df \
        .writeStream \
        .trigger(processingTime='3 seconds') \
        .option("checkpointLocation", "stream/checkpoints") \
        .option("encoding", "UTF-8") \
        .outputMode('append') \
        .foreachBatch(save_batch_data) \
        .start()

    stream_df.awaitTermination()


if __name__ == "__main__":
    main()
