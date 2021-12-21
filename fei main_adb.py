# Databricks notebook source
# MAGIC %run /Users/fyj121322@gmail.com/Movie_final_project/Operations_ADB

# COMMAND ----------

from pyspark.sql.functions import *
from delta import DeltaTable
from datetime import datetime

# COMMAND ----------

username = 'yinjiao_fei'
GenresDimPath = f"/dbacademyfinal/{username}/movie_databricks_final/Genres/"
MoviePipelinePath = f"/dbacademyfinal/{username}/movie_databricks_final/classic/"

landingPath = MoviePipelinePath + "landing/"
rawPath = MoviePipelinePath + "raw/"
bronzePath = MoviePipelinePath + "bronze/"
silverPath = MoviePipelinePath + "silver/"
silverQuarantinePath = MoviePipelinePath + "silverQuarantine/"
goldPath = MoviePipelinePath + "gold/"

# COMMAND ----------

raw_directory = 'dbfs:/FileStore/Final project'

#ingest raw data


raw_directory = 'dbfs:/FileStore/Final project'

#ingest raw data
ingest_movie_data(raw_directory)


#raw to bronze

#read raw
raw_df = read_batch_raw()
transformed_raw_df = transform_raw(raw_df)
raw_to_Bronze_Writer = batch_writer(transformed_dataframe=transformed_raw_df, partition_column="ingesttime")
raw_to_Bronze_Writer.save(bronzePath)

dbutils.fs.rm(rawPath, recurse=True)

#bronze to silver 
dbutils.fs.rm(silverPath, recurse=True)
bronze_df = read_batch_bronze()

silver_clean_df = transform_bronze(bronze_df)

#write to silver path
bronzetoSilverWriter = batch_writer(transformed_dataframe=silver_clean_df, partition_column="CreatedDate", exclude_columns = ['movie'])
bronzetoSilverWriter.save(silverPath)

update_bronze_table_status(bronzePath, silver_clean_df, "loaded")

# COMMAND ----------


