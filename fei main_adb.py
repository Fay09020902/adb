# Databricks notebook source
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

from pyspark.sql.functions import *
from delta import DeltaTable
from datetime import datetime

# COMMAND ----------

raw_directory = 'dbfs:/FileStore/Final project'

#ingest raw data


raw_df = spark.read.option("multiLine", 'true').format('json').load(dbutils.fs.ls(raw_directory)[0].path).select(explode('movie'))

movie_file_paths = [file.path for file in dbutils.fs.ls(raw_directory) if 'movie' in file.path]

for file in range(1,len(movie_file_paths)):
    raw_df = raw_df.union(spark.read.option("multiLine", 'true').format('json').load(movie_file_paths[file]).select(explode('movie')))

file_name = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
raw_df.write.format('json').save(rawPath + file_name)

# COMMAND ----------

#raw to bronze
raw_df = spark.read.format('json').load(rawPath + file_name)
transformed_raw = raw_df.select(
  col('col').alias('movie'),
  current_timestamp().alias("ingesttime"),
  lit('new').alias('status'),
  current_timestamp().cast("date").alias("p_ingestdate"),
  lit(raw_directory).alias('source'))
raw_to_Bronze_Writer = transformed_raw.write.format('delta').mode('append').partitionBy('ingesttime')
raw_to_Bronze_Writer.save(bronzePath)

dbutils.fs.rm(rawPath, recurse=True)

# COMMAND ----------

#bronze to silver 
dbutils.fs.rm(silverPath, recurse=True)
bronze_df = spark.read.format('delta').load(bronzePath).filter(col('status') == lit('new'))
transformed_bronze_df = bronze_df.dropDuplicates(["movie"]).select("movie.*", 'movie')
silver_clean_df = transformed_bronze_df.withColumn('RunTime', when(col("RunTime") < 0, -col('RunTime')).otherwise(col('RunTime'))).withColumn('Budget', when(col('Budget') < 10*6, 10*6).otherwise(col('Budget'))).drop(col('genres'))

bronzetoSilverWriter = silver_clean_df.drop(col('movie')).write.format('delta').mode('append')
bronzetoSilverWriter.save(silverPath)


# COMMAND ----------

bronzeTable = DeltaTable.forPath(spark, bronzePath)
dataframeAugmented = silver_clean_df.withColumn("status", lit('loaded'))
( bronzeTable.alias('bronze')
.merge(dataframeAugmented.alias('dataframe'),'bronze.movie = dataframe.movie')
.whenMatchedUpdate(set={"status": "dataframe.status"})
.execute()
)

# COMMAND ----------


