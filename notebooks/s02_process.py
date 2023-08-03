# Databricks notebook source
# MAGIC %md
# MAGIC # Step 02 - Preprocess data for country

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setup

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

from utilities.transforms import *
import matplotlib.pyplot as plt

# COMMAND ----------

dbutils.widgets.text("country_iso3", "DZA")
country_iso = dbutils.widgets.get("country_iso3")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Get raw data

# COMMAND ----------

df_raw_loc = f"/mnt/raw/temp/workflow_demo/demo_db/demo_raw_covid"
df_raw = spark.read.format("delta").load(df_raw_loc).toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Preprocess for country

# COMMAND ----------

df = filter_country(df_raw.copy(), country=country_iso)
df = pivot_and_clean(df, fillna=0)  
df = clean_spark_cols(df)
df = df.reset_index()
df['country_iso'] = country_iso

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Insights

# COMMAND ----------

df.head()

# COMMAND ----------

# Using python
df.plot(figsize=(13,6), grid=True).legend(loc='upper left')
plt.title(f"Covid data for {country_iso}")
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Save to Delta Lake

# COMMAND ----------

# Convert from Pandas to a pyspark sql DataFrame.
df_spark = spark.createDataFrame(df)

# COMMAND ----------

output_loc = 'dbfs:/mnt/raw/temp/workflow_demo/demo_db/demo_covid_stats'
df_spark.write.format("delta").mode("overwrite").option(
    "replaceWhere", f"country_iso = '{country_iso}'"
).save(output_loc)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS demo_db.demo_covid_stats
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/raw/temp/workflow_demo/demo_db/demo_covid_stats'

# COMMAND ----------

