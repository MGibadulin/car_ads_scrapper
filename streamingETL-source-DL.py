import os

from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StringType, StructType
from pyspark.sql.functions import input_file_name

DEBUG_MODE = True   #instead of a hardcoded value, in future it'll become a console argument

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


def foreach_batch_function(micro_batch_df, epoch_id):
    micro_batch_df = micro_batch_df.repartition(1)
    micro_batch_df.persist()

    if DEBUG_MODE:
        micro_batch_df.select("card_id",
                              "title",
                              "vehicle",
                              "year",
                              "price",
                              "price_primary",
                              "price_usd",
                              # "price_history",
                              "location",
                              "labels",
                              "description",
                              "transmission",
                              "engine",
                              "fuel",
                              "milage",
                              "body",
                              "drive",
                              "color",
                              # "comment",
                              # "vehicle_history",
                              # "options",
                              # "gallery",
                              "scrap_date",
                              "input_file_name"
                             ) \
            .write \
            .format("console") \
            .option("header", True) \
            .option("truncate", False) \
            .save()

    #form card csv
    micro_batch_df.selectExpr("card_id",
                          "vehicle",
                          "year",
                          "price",
                          "price_usd",
                          # "price_history",
                          "location",
                          "labels",
                          "transmission",
                          "engine",
                          "fuel",
                          "milage",
                          "body",
                          "drive",
                          "color",
                          "comment",
                          # "vehicle_history",
                          # "options",
                          # "gallery",
                          "scrap_date",
                          # "source_file_name",
                          "loaded_date"
                         ) \
        .write \
        .format("csv") \
        .partitionBy("year", "price") \
        .option("header", True) \
        .option("path", "stream/CARS_COM/CSV/car_card") \
        .mode("append") \
        .save()

    #form card_options csv
    micro_batch_df.selectExpr("card_id",
                          "explode(options) as option_group",
                          "scrap_date",
                          "loaded_date"
                         ) \
                  .selectExpr("card_id",
                              "option_group.category",
                              "explode(option_group.items) as item",
                              "scrap_date",
                              "loaded_date"
                             ) \
        .write \
        .format("csv") \
        .partitionBy("category") \
        .option("header", True) \
        .option("path", "stream/CARS_COM/CSV/car_card_option") \
        .mode("append") \
        .save()

    #form card_info csv - url & source_file_name
    micro_batch_df.selectExpr("card_id",
                          "url",
                          "source_file_name",
                          "scrap_date",
                          "loaded_date"
                         ) \
        .write \
        .format("csv") \
        .option("header", True) \
        .option("path", "stream/CARS_COM/CSV/car_card_info") \
        .mode("append") \
        .save()

    #form card_gallery csv - num & url
    micro_batch_df.selectExpr("card_id",
                          "posexplode(gallery) as (num, url)",
                          "scrap_date",
                          "loaded_date"
                         ) \
        .write \
        .format("csv") \
        .option("header", True) \
        .option("path", "stream/CARS_COM/CSV/car_card_gallery") \
        .mode("append") \
        .save()
    
    
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

    #source file streaming - go through all the existing files, then wait for new files appeared
    source_df = spark \
        .readStream \
        .option("maxFilesPerTrigger", 100) \
        .format("json") \
        .schema(user_schema) \
        .option("encoding", "UTF-8") \
        .option("multiLine", True) \
        .option("path", "C:/Users/User/PycharmProjects/car_ads_scrapper/scrapped_cards/CARS_COM/JSON/*/*/*/") \
        .load() \
        .withColumn("input_file_name", input_file_name())

    source_df.createOrReplaceTempView("source_micro_batch")

    # title: 2023 Hyundai Elantra SEL
    # description: 2023, Automatic, 3.8L V6 24V GDI DOHC, Gasoline, 7 mi. | pickup truck, Rear-wheel Drive, Tactical Green
    # price_primary: $19,995
    # price_history: 2/21/23: $22,987 | 3/23/23: $21,987 | 4/07/23: $20,931 | 4/21/23: $19,971
    # labels: Great Deal | $788 under|CPO Warrantied|Home Delivery|Virtual Appointments|VIN: 3KPA25AD0PE512839|Included warranty

    transformed_df = spark.sql("""
        select card_id,
               title,
               substring(title, 6, len(title) - 5) as vehicle,
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
               description,
               split_part(description, ',', 2) as transmission,
               split_part(description, ',', 3) as engine,
               split_part(description, ',', 4) as fuel,
               split_part(split_part(description, ',', 5), '|', 1) as milage,
               split_part(split_part(description, ',', 5), '|', 2) as body,
               split_part(description, ',', 6) as drive,
               split_part(description, ',', 7) as color,
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

    # foreach_batch_function is used for writing destinations
    stream_df = transformed_df \
        .writeStream \
        .trigger(processingTime='10 seconds') \
        .option("checkpointLocation", "stream/checkpoints") \
        .option("encoding", "UTF-8") \
        .outputMode('append') \
        .foreachBatch(foreach_batch_function) \
        .start()

    stream_df.awaitTermination()


if __name__ == "__main__":
    main()
