# -*- coding: utf-8 -*-
"""
ETL Bronze Layer (Delta Lake + Glue 5.0 + boto3 registration)

1. Extract CSV from S3 Dropzone
2. Add metadata, partitioning columns
3. Write Delta Lake partitioned in S3
4. Register as EXTERNAL TABLE in AWS Glue
"""

import logging
import sys
import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, to_date, year, month, date_format, input_file_name
from pyspark.sql.utils import AnalysisException
from delta.tables import DeltaTable


# Config (lowercase)
source_path = "s3://leonardo-souza-dropzone-dev-us-east-1/manual_files/kaggle/nyc_taxi_trips/csv/"
destination_path = "s3://leonardo-souza-bronze-dev-us-east-1/manual_files/kaggle/nyc_taxi_trips/"
database_name = "bronze_layer_manual_files"
table_name = "kaggle_nyc_yellow_taxi_trip_data"
partition_keys = ["year", "month"]

def extract_data(spark: SparkSession, path: str, logger: logging.Logger) -> DataFrame:
    logger.info(f"[extract] reading from {path}")
    df = spark.read.format("csv") \
        .option("header","true").option("inferSchema","true") \
        .option("quote","\"").option("sep",",").load(path)
    count = df.count()
    logger.info(f"[extract] records read: {count}")
    if count == 0:
        logger.warning("[extract] no data found!")
    df.printSchema()
    return df

def transform_data(df: DataFrame, logger: logging.Logger) -> DataFrame:
    logger.info("[transform] adding metadata & partition columns")
    df2 = df.withColumn("ingestion_timestamp", date_format(col("_metadata.file_modification_time"), "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("source_file", input_file_name()) \
        .withColumn("pickup_date", to_date(col("tpep_pickup_datetime"))) \
        .withColumn("year", year(col("pickup_date"))) \
        .withColumn("month", month(col("pickup_date")))
    df2.printSchema()
    return df2

def register_table_glue(spark, df, path, db, table, parts, logger):
    logger.info(f"[register] registrando {db}.{table} no Glue...")
    columns = [
        {"Name": c.name, "Type": c.dataType.simpleString()}
        for c in df.schema if c.name not in parts
    ]

    table_input = {
        "Name": table,
        "TableType": "EXTERNAL_TABLE",
        "StorageDescriptor": {
            "Columns": columns,
            "Location": path,
            "InputFormat": "io.delta.hadoop.DeltaInputFormat",
            "OutputFormat": "io.delta.hadoop.DeltaOutputFormat",
            "SerdeInfo": {
                "SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
            }
        },
        # Aqui fora do StorageDescriptor:
        "PartitionKeys": [{"Name": p, "Type": "int"} for p in parts],
        "Parameters": {"classification": "delta"},
    }

    boto3.client("glue").create_table(DatabaseName=db, TableInput=table_input)
    logger.info(f"[register] tabela {db}.{table} criada com sucesso")

def load_and_register(spark: SparkSession, df: DataFrame, path: str, db: str, table: str, parts: list, logger: logging.Logger):

    count = df.count()
    logger.info(f"[load] writing {count} records to Delta at {path}")

    try:
        if DeltaTable.isDeltaTable(spark, path):
            write_mode = "append"
        else:
            write_mode = "overwrite"
    except Exception as e:
        logger.warning(f"[load] delta check failed, assuming first load: {e}")
        write_mode = "overwrite"

    logger.info(f"[load] detected write mode: {write_mode}")

    df.select([c for c in df.columns]).write.format("delta") \
        .mode(write_mode).partitionBy(*parts).save(path)

    logger.info("[load] delta write complete")

    if write_mode == "overwrite":
        register_table_glue(spark, df, path, db, table, parts, logger)


def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [%(funcName)s] %(message)s')
    logger = logging.getLogger(__name__)
    args = getResolvedOptions(sys.argv, ["JOB_NAME"])
    sc = SparkContext()
    sc.setLogLevel("INFO")
    # Delta extensions via Glue params
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session

    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)
    spark.conf.set("spark.sql.files.includeMetadata", "true")

    try:
        df_raw = extract_data(spark, source_path, logger)
        df_trans = transform_data(df_raw, logger)
        load_and_register(spark, df_trans, destination_path, database_name, table_name, partition_keys, logger)
        logger.info("[main] Job completed successfully")
    except Exception as e:
        logger.error("[main] job failed!", exc_info=True)
        raise
    finally:
        job.commit()

if __name__ == "__main__":
    main()
