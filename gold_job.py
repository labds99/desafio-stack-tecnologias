# -*- coding: utf-8 -*-

"""
Script ETL para a camada GOLD do Data Lake (Delta Lake + Glue 5.0).

1. Lê dados da camada Silver (Delta).
2. Aplica enriquecimento e agregações.
3. Escreve os dados agregados na camada Gold (Delta Lake).
4. Se a tabela Gold existir, realiza MERGE; senão, cria com particionamento e registra no Glue Catalog.
"""

import logging
import sys
import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import year, month, col, sum as _sum, count as _count
from delta.tables import DeltaTable

# Configurações
silver_path = "s3://leonardo-souza-silver-dev-us-east-1/manual_files/kaggle/nyc_taxi_trips/"
gold_path = "s3://leonardo-souza-gold-dev-us-east-1/manual_files/kaggle/nyc_taxi_trips/"
db_name = "gold_layer_manual_files"
table_name = "kaggle_nyc_taxi_trip_gold"
partition_keys = ["year", "month"]

def enrich_and_aggregate(df, logger):
    logger.info("[gold_job] Iniciando enriquecimento e agregação dos dados...")
    df_gold = df.withColumn("year", year(col("pickup_ts"))) \
                .withColumn("month", month(col("pickup_ts"))) \
                .groupBy("year", "month", "payment_type") \
                .agg(
                    _count("*").alias("total_trips"),
                    _sum("total_amount").alias("total_amount_collected")
                )
    df_gold.printSchema()
    df_gold.show(5)
    logger.info("[gold_job] Enriquecimento + agregação finalizada.")
    return df_gold

def register_gold_if_not_exists(spark, df, path, db, table, parts, logger):
    glue = boto3.client("glue")
    try:
        glue.get_table(DatabaseName=db, Name=table)
        logger.info("[catalog] Tabela já registrada no Glue Catalog.")
    except glue.exceptions.EntityNotFoundException:
        logger.info("[catalog] Tabela não existe. Registrando...")
        cols = [{"Name": f.name, "Type": f.dataType.simpleString()} for f in df.schema if f.name not in parts]
        table_input = {
            "Name": table,
            "TableType": "EXTERNAL_TABLE",
            "StorageDescriptor": {
                "Columns": cols,
                "Location": path,
                "InputFormat": "io.delta.hadoop.DeltaInputFormat",
                "OutputFormat": "io.delta.hadoop.DeltaOutputFormat",
                "SerdeInfo": {
                    "SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
                }
            },
            "PartitionKeys": [{"Name": p, "Type": "int"} for p in parts],
            "Parameters": {"classification": "delta"}
        }
        glue.create_table(DatabaseName=db, TableInput=table_input)
        logger.info("[catalog] Registro realizado com sucesso.")

def merge_or_insert(spark, df, path, db, table, partition_keys, logger):
    logger.info("[gold_job] Iniciando escrita da camada GOLD...")

    if DeltaTable.isDeltaTable(spark, path):
        logger.info("[gold_job] Realizando merge com dados existentes...")
        delta_table = DeltaTable.forPath(spark, path)
        condition = " AND ".join([f"target.{k} = source.{k}" for k in partition_keys + ["payment_type"]])
        delta_table.alias("target").merge(
            df.alias("source"), condition
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()
        logger.info("[gold_job] Merge concluído.")
    else:
        logger.info("[gold_job] Primeira carga. Realizando overwrite.")
        df.write.format("delta") \
           .mode("overwrite") \
           .partitionBy(*partition_keys) \
           .save(path)
        register_gold_if_not_exists(spark, df, path, db, table, partition_keys, logger)

def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger("gold_job")
    args = getResolvedOptions(sys.argv, ["JOB_NAME"])
    sc = SparkContext()
    glue_ctx = GlueContext(sc)
    spark = glue_ctx.spark_session
    job = None

    try:
        job = glue_ctx.create_job(args["JOB_NAME"]) if hasattr(glue_ctx, "create_job") else None
        if job: job.init(args["JOB_NAME"], args)

        df_silver = spark.read.format("delta").load(silver_path)
        logger.info("[main] Silver carregado com sucesso.")

        df_gold = enrich_and_aggregate(df_silver, logger)
        merge_or_insert(spark, df_gold, gold_path, db_name, table_name, partition_keys, logger)

        logger.info("[main] Gold pipeline completed successfully")
    except Exception:
        logger.exception("[main] Gold job failed")
        raise
    finally:
        if job: job.commit()

if __name__ == "__main__":
    main()
