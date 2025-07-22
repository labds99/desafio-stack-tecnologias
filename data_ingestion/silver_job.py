# -*- coding: utf-8 -*-

"""
Silver Layer – Bronze → Silver (Delta + Glue)
1. Leitura da tabela Delta Bronze.
2. Limpeza mínima e tipagem correta.
3. Elimina duplicatas.
4. Adiciona colunas técnicas.
5. Faz merge Delta com a Silver (upsert).
6. Registra a tabela no Glue Data Catalog se não existir.
"""

import logging, sys, boto3, os
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, to_timestamp, year, month, date_format, expr
)
from delta.tables import DeltaTable

# CONFIGURAÇÕES
bronze_path = "s3://leonardo-souza-bronze-dev-us-east-1/manual_files/kaggle/nyc_taxi_trips/"
silver_path = "s3://leonardo-souza-silver-dev-us-east-1/manual_files/kaggle/nyc_taxi_trips/"
db_name = "silver_layer_manual_files"
table_silver = "kaggle_nyc_taxi_trip_silver"
partition_keys = ["year", "month"]


def clean_and_cast(df: DataFrame, logger: logging.Logger) -> DataFrame:
    logger.info("[clean] Cast, filter & dedupe")
    df2 = df \
        .withColumn("pickup_ts", to_timestamp(col("tpep_pickup_datetime"))) \
        .withColumn("dropoff_ts", to_timestamp(col("tpep_dropoff_datetime"))) \
        .withColumn("passenger_count", col("passenger_count").cast("int")) \
        .withColumn("trip_distance", col("trip_distance").cast("double")) \
        .filter(col("pickup_ts").isNotNull() & col("dropoff_ts").isNotNull())

    df2 = df2.dropDuplicates(["vendorid", "pickup_ts", "dropoff_ts", "source_file"])
    df2 = df2 \
        .withColumn("ingestion_date", date_format(col("pickup_ts"), "yyyy-MM-dd")) \
        .withColumn("year", year(col("pickup_ts"))) \
        .withColumn("month", month(col("pickup_ts")))
    count = df2.count()
    logger.info(f"[clean] Silver record count: {count}")
    return df2


def merge_into_silver(spark, df: DataFrame, path: str, keys: list, logger: logging.Logger):
    logger.info("[merge] Realizando merge Delta (upsert) na camada Silver...")
    if DeltaTable.isDeltaTable(spark, path):
        target = DeltaTable.forPath(spark, path)
        condition = " AND ".join([f"target.{k} = source.{k}" for k in keys])
        target.alias("target").merge(
            df.alias("source"), condition
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()
        logger.info("[merge] Merge Delta concluído com sucesso.")
    else:
        df.write.format("delta").mode("overwrite").partitionBy(*partition_keys).save(path)
        logger.info("[merge] Primeira carga realizada com overwrite.")


def register_silver_if_not_exists(spark, df: DataFrame, path: str, db: str, table: str, parts: list, logger: logging.Logger):
    glue = boto3.client("glue")
    try:
        glue.get_table(DatabaseName=db, Name=table)
        logger.info("[catalog] Tabela já registrada no Glue Catalog.")
    except glue.exceptions.EntityNotFoundException:
        logger.info("[catalog] Tabela não existe, registrando...")
        cols = [{"Name": f.name, "Type": f.dataType.simpleString()} for f in df.schema if f.name not in parts]
        table_input = {
            "Name": table,
            "TableType": "EXTERNAL_TABLE",
            "StorageDescriptor": {
                "Columns": cols,
                "Location": path,
                "InputFormat": "io.delta.hadoop.DeltaInputFormat",
                "OutputFormat": "io.delta.hadoop.DeltaOutputFormat",
                "SerdeInfo": {"SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"}
            },
            "PartitionKeys": [{"Name": p, "Type": "int"} for p in parts],
            "Parameters": {"classification": "delta"}
        }
        glue.create_table(DatabaseName=db, TableInput=table_input)
        logger.info("[catalog] Registro realizado com sucesso.")


def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger("silver_job")
    args = getResolvedOptions(sys.argv, ["JOB_NAME"])
    sc = SparkContext()
    glue_ctx = GlueContext(sc)
    spark = glue_ctx.spark_session
    job = None

    try:
        job = glue_ctx.create_job(args["JOB_NAME"]) if hasattr(glue_ctx, "create_job") else None
        if job: job.init(args["JOB_NAME"], args)

        df_bronze = spark.read.format("delta").load(bronze_path)
        logger.info("[main] Bronze loaded")

        df_clean = clean_and_cast(df_bronze, logger)
        merge_into_silver(spark, df_clean, silver_path, ["vendorid", "pickup_ts", "dropoff_ts"], logger)
        register_silver_if_not_exists(spark, df_clean, silver_path, db_name, table_silver, partition_keys, logger)

        logger.info("[main] Silver pipeline completed successfully")
    except Exception:
        logger.exception("[main] Silver job failed")
        raise
    finally:
        if job: job.commit()


if __name__ == "__main__":
    main()
