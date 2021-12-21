# Databricks notebook source
from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    current_timestamp,
    from_json,
    from_unixtime,
    lag,
    lead,
    lit,
    mean,
    stddev,
    max,
)
from typing import List
from pyspark.sql.session import SparkSession
from pyspark.sql.window import Window

# COMMAND ----------

def ingest_movie_data(raw_directory: str) -> bool:

    raw_df = spark.read.option("multiLine", 'true').format('json').load(dbutils.fs.ls(raw_directory)[0].path).select(explode('movie'))

    movie_file_paths = [file.path for file in dbutils.fs.ls(raw_directory) if 'movie' in file.path]

    for file in range(1,len(movie_file_paths)):
        raw_df = raw_df.union(spark.read.option("multiLine", 'true').format('json').load(movie_file_paths[file]).select(explode('movie')))

    raw_df.write.format('json').save(rawPath)
    return True


# COMMAND ----------

def read_batch_raw() -> DataFrame:
    return spark.read.format('json').load(rawPath)

# COMMAND ----------

def read_batch_bronze() -> DataFrame:
    return spark.read.format('delta').load(bronzePath).filter(col('status') == lit('new'))

# COMMAND ----------

def transform_raw(raw: DataFrame) -> DataFrame:
    return raw_df.select(
      col('col').alias('movie'),
      current_timestamp().alias("ingesttime"),
      lit('new').alias('status'),
      current_timestamp().cast("date").alias("p_ingestdate"),
      lit(raw_directory).alias('source')
    )
