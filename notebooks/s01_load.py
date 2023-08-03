# Databricks notebook source
# MAGIC %md
# MAGIC # Step 01 - Load raw data

# COMMAND ----------

import pandas as pd
import pyspark.pandas as ps

# COMMAND ----------

# MAGIC %md
# MAGIC # Load and save raw data

# COMMAND ----------

data_path = 'https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/hospitalizations/covid-hospitalizations.csv'
print(f'Data path: {data_path}')

# COMMAND ----------

df = pd.read_csv(data_path)

# COMMAND ----------

# MAGIC %md
# MAGIC Save to Delta Lake

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS demo_db LOCATION '/mnt/raw/temp/workflow_demo/demo_db'

# COMMAND ----------

# Create pandas on Spark dataframe
psdf = ps.from_pandas(df)
spark_psdf = psdf.to_spark()
spark_psdf.write.format("delta").mode("overwrite").save('dbfs:/mnt/raw/temp/workflow_demo/demo_db/demo_raw_covid')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS demo_db.demo_raw_covid
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/raw/temp/workflow_demo/demo_db/demo_raw_covid'

# COMMAND ----------

# MAGIC %md
# MAGIC # Insights

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM demo_db.demo_raw_covid
# MAGIC LIMIT 100

# COMMAND ----------

df.iso_code.value_counts()

# COMMAND ----------

