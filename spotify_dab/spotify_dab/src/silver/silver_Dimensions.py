# Databricks notebook source
# MAGIC %md
# MAGIC ## DimUser

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
import os
import sys

# Append path to load your custom transformation package
project_pth = os.path.join(os.getcwd(), '..', '..')
sys.path.append(project_pth)

from utils.transformations import reusable

# COMMAND ----------

# MAGIC %md
# MAGIC ### AUTOLOADER

# COMMAND ----------

# 1. Define active paths
checkpoint_path = "abfss://silver@storagespotifyproject0.dfs.core.windows.net/checkpoints/spotify_stream"

# 2. Ingest streaming data via Auto Loader
df_user = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "parquet") \
    .option("cloudFiles.schemaLocation", "abfss://silver@storagespotifyproject0.dfs.core.windows.net/DimUser/checkpoint") \
    .option("checkpointLocation", "abfss://silver@storagespotifyproject0.dfs.core.windows.net/checkpoints/preview_user_cache")\
    .option("schemaEvolutionMode", "addNewColumns")\
    .load("abfss://bronze@storagespotifyproject0.dfs.core.windows.net/DimUser/")

display(df_user, checkpointLocation = "abfss://silver@storagespotifyproject0.dfs.core.windows.net/checkpoints/preview_user_cache")

# 3. Apply uppercase transformation
df_user = df_user.withColumn("user_name", upper(col("user_name")))

# 4. Instantiate custom class and drop metadata fields
df_user_obj = reusable()
df_user = df_user_obj.dropColumns(df_user, ['_rescued_data'])



# COMMAND ----------

df_user.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation", "abfss://silver@storagespotifyproject0.dfs.core.windows.net/DimUser/checkpoint")\
    .trigger(once=True)\
    .option("path", "abfss://silver@storagespotifyproject0.dfs.core.windows.net/DimUser/data")\
    .toTable("spotify_cata.silver.DimUser")

# COMMAND ----------

# MAGIC %md
# MAGIC ## DimArtist

# COMMAND ----------

df_art = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "parquet") \
    .option("cloudFiles.schemaLocation", "abfss://silver@storagespotifyproject0.dfs.core.windows.net/DimArtist/checkpoint") \
    .option("checkpointLocation", "abfss://silver@storagespotifyproject0.dfs.core.windows.net/checkpoints/preview_artist_cache")\
    .option("schemaEvolutionMode", "addNewColumns")\
    .load("abfss://bronze@storagespotifyproject0.dfs.core.windows.net/DimArtist/")


# COMMAND ----------

df_art_obj = reusable()
df_art = df_art_obj.dropColumns(df_art, ['_rescued_data'])
df_art = df_art.dropDuplicates(['artist_id'])

# COMMAND ----------

df_art.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation", "abfss://silver@storagespotifyproject0.dfs.core.windows.net/DimArt/checkpoint")\
    .trigger(once=True)\
    .option("path", "abfss://silver@storagespotifyproject0.dfs.core.windows.net/DimArt/data")\
    .toTable("spotify_cata.silver.DimArtist")


# COMMAND ----------

# MAGIC %md
# MAGIC ## DimTrack

# COMMAND ----------

df_track = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "parquet") \
    .option("cloudFiles.schemaLocation", "abfss://silver@storagespotifyproject0.dfs.core.windows.net/DimTrack/checkpoint") \
    .option("checkpointLocation", "abfss://silver@storagespotifyproject0.dfs.core.windows.net/checkpoints/preview_track_cache")\
    .option("schemaEvolutionMode", "addNewColumns")\
    .load("abfss://bronze@storagespotifyproject0.dfs.core.windows.net/DimTrack/")


# COMMAND ----------

df_track = df_track.withColumn("durationFlag", when(col('duration_sec') < 150, "low") \
                               .when(col('duration_sec') < 300, "medium") \
                               .otherwise("high"))

df_track = df_track.withColumn("track_name", regexp_replace(col('track_name'), '-', ''))

df_track_obj = reusable()
df_track = df_track_obj.dropColumns(df_track, ['_rescued_data'])



# COMMAND ----------

df_track.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation", "abfss://silver@storagespotifyproject0.dfs.core.windows.net/DimTrack/checkpoint")\
    .trigger(once=True)\
    .option("path", "abfss://silver@storagespotifyproject0.dfs.core.windows.net/DimTrack/data")\
    .toTable("spotify_cata.silver.DimTrack")


# COMMAND ----------

# MAGIC %md
# MAGIC ## DimDate

# COMMAND ----------

df_date = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "parquet") \
    .option("cloudFiles.schemaLocation", "abfss://silver@storagespotifyproject0.dfs.core.windows.net/DimDate/checkpoint") \
    .option("checkpointLocation", "abfss://silver@storagespotifyproject0.dfs.core.windows.net/checkpoints/preview_date_cache")\
    .option("schemaEvolutionMode", "addNewColumns")\
    .load("abfss://bronze@storagespotifyproject0.dfs.core.windows.net/DimDate/")


# COMMAND ----------

df_date_obj = reusable()
df_date = df_date_obj.dropColumns(df_date, ['_rescued_data'])

df_date.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation", "abfss://silver@storagespotifyproject0.dfs.core.windows.net/DimDate/checkpoint")\
    .trigger(once=True)\
    .option("path", "abfss://silver@storagespotifyproject0.dfs.core.windows.net/DimDate/data")\
    .toTable("spotify_cata.silver.DimDate")

# COMMAND ----------

# MAGIC %md
# MAGIC ## FactStream

# COMMAND ----------

df_factStream = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "parquet") \
    .option("cloudFiles.schemaLocation", "abfss://silver@storagespotifyproject0.dfs.core.windows.net/FactStream/checkpoint") \
    .option("checkpointLocation", "abfss://silver@storagespotifyproject0.dfs.core.windows.net/checkpoints/preview_factStream_cache")\
    .option("schemaEvolutionMode", "addNewColumns")\
    .load("abfss://bronze@storagespotifyproject0.dfs.core.windows.net/FactStream/")

# COMMAND ----------

df_factStream_obj = reusable()
df_factStream = df_factStream_obj.dropColumns(df_factStream, ['_rescued_data'])

df_factStream.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation", "abfss://silver@storagespotifyproject0.dfs.core.windows.net/FactStream/checkpoint")\
    .trigger(once=True)\
    .option("path", "abfss://silver@storagespotifyproject0.dfs.core.windows.net/FactStream/data")\
    .toTable("spotify_cata.silver.FactStream")