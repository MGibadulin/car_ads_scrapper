import os
import time
import base64

from pyspark.sql import SparkSession

DEBUG_MODE = True        # instead of a hardcoded value, in future it'll become a config parameter
DEBUG_TOKENIZED_ATTRS = True
ARCHIVE_SOURCE_FILES = True

GCP_PROJECT_ID = "paid-project-346208"
GCP_DATASET_NAME = "car_ads_ds_landing_test"
GCP_TABLE_NAME = "cars_com_card_direct"
# GCP_TEMPORARY_BUCKET_NAME = "temporary_spark_bucket"

spark = SparkSession.builder.master("local[*]").appName("car ads batch ETL (source to DL)") \
    .config("spark.driver.extraJavaOptions", "-Duser.timezone=GMT") \
    .config("spark.executor.extraJavaOptions", "-Duser.timezone=GMT") \
    .config("spark.sql.session.timeZone", "UTC") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .config("spark.ui.port", "4041") \
    .config("spark.sql.parser.escapedStringLiterals", True) \
    .config("spark.sql.files.openCostInBytes", "50000") \
    .config("spark.sql.sources.parallelPartitionDiscovery.threshold", 32) \
    .config("spark.executor.memory", "3g") \
    .config("spark.driver.memory", "3g") \
    .config("spark.jars", "jars/gcs-connector-hadoop3-latest.jar,jars/google-api-client-1.30.10.jar,jars/spark-bigquery-latest_2.12.jar") \
    .getOrCreate()

    # .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.11:0.18.0") \
    # about partitioning in Spark: https://medium.com/swlh/building-partitions-for-processing-data-files-in-apache-spark-2ca40209c9b7




def main():
    df = spark.read.format("bigquery") \
                    .option("credentialsFile", "bigquery-credentials.json") \
                    .option('parentProject', GCP_PROJECT_ID) \
                    .option("table", f"{GCP_PROJECT_ID}.{GCP_DATASET_NAME}.{GCP_TABLE_NAME}") \
                    .load()

    df.show(truncate=False)

    df.write.format("bigquery") \
                    .option("credentialsFile", "bigquery-credentials.json") \
                    .option('parentProject', GCP_PROJECT_ID) \
                    .option("table", f"{GCP_PROJECT_ID}.{GCP_DATASET_NAME}.{GCP_TABLE_NAME}_copy") \
                    .option("writeMethod", "direct") \
                    .mode("overwrite") \
                    .save()
                # .option("temporaryGcsBucket", GCP_TEMPORARY_BUCKET_NAME)

    print(f"Created a copy of the source table: {GCP_PROJECT_ID}.{GCP_DATASET_NAME}.{GCP_TABLE_NAME}_copy")


if __name__ == "__main__":
    main()
