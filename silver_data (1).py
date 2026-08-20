# Databricks notebook source
storage_account_name = "myazurestorageanalytics"
storage_account_key = ---------

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key
)

# COMMAND ----------

# DBTITLE 1,Cell 1
bronze_path = "abfss://bronze@myazurestorageanalytics.dfs.core.windows.net/bronze"

df_machines_bronze = spark.read.format("delta").load(f"{bronze_path}/machines")
df_maintenance_bronze = spark.read.format("delta").load(f"{bronze_path}/maintenance")
df_production_bronze = spark.read.format("delta").load(f"{bronze_path}/production")
df_sensor_bronze = spark.read.format("delta").load(f"{bronze_path}/sensor")

df_machines_bronze.show(5)
df_maintenance_bronze.show(5)
df_production_bronze.show(5)
df_sensor_bronze.show(5)

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import col, to_timestamp, to_date

# Machines - cast install_date to timestamp
df_machines_silver = df_machines_bronze.withColumn(
    "install_date", 
    col("install_date").cast(TimestampType())
)

# Sensor Readings - cast numeric columns (timestamp is trickier due to mixed formats, handle next)
df_sensor_silver = df_sensor_bronze.withColumn(
    "temperature", col("temperature").cast(DoubleType())
).withColumn(
    "vibration", col("vibration").cast(DoubleType())
).withColumn(
    "pressure", col("pressure").cast(DoubleType())
).withColumn(
    "rpm", col("rpm").cast(DoubleType())
)

# Maintenance Logs - cast downtime_hours to double (maintenance_date is mixed formats, handle next)
df_maintenance_silver = df_maintenance_bronze.withColumn(
    "downtime_hours", col("downtime_hours").cast(DoubleType())
)

# Production Output - cast numeric and date columns
df_production_silver = df_production_bronze.withColumn(
    "date", col("date").cast(DateType())
).withColumn(
    "units_produced", col("units_produced").cast(LongType())
).withColumn(
    "defect_count", col("defect_count").cast(LongType())
)

# Quick check
df_machines_silver.printSchema()
df_sensor_silver.printSchema()
df_maintenance_silver.printSchema()
df_production_silver.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col

print("Sensor timestamp nulls:", df_sensor_silver.filter(col("timestamp").isNull()).count())
print("Maintenance date nulls:", df_maintenance_silver.filter(col("maintenance_date").isNull()).count())

print("Total sensor rows:", df_sensor_silver.count())
print("Total maintenance rows:", df_maintenance_silver.count())

# COMMAND ----------

from pyspark.sql.functions import col, when


df_machines_silver = df_machines_silver.fillna(
    value="Unknown",
    subset=["manufacturer"]
)

# COMMAND ----------


df_maintenance_silver = df_maintenance_silver.fillna(
    value=0.0,
    subset=["downtime_hours"]
)

# COMMAND ----------

df_production_silver = df_production_silver.fillna(
    value=0,
    subset=["units_produced", "defect_count"]
)


# COMMAND ----------

# ================================
# Quick verification - check remaining nulls (simplified)
# ================================
from pyspark.sql.functions import col

print("\n=== Machines ===")
print("Manufacturer nulls:", df_machines_silver.filter(col("manufacturer").isNull()).count())

print("\n=== Sensor (intentionally kept) ===")
print("Temperature nulls:", df_sensor_silver.filter(col("temperature").isNull()).count())
print("Vibration nulls:", df_sensor_silver.filter(col("vibration").isNull()).count())
print("Pressure nulls:", df_sensor_silver.filter(col("pressure").isNull()).count())

print("\n=== Maintenance ===")
print("Downtime_hours nulls:", df_maintenance_silver.filter(col("downtime_hours").isNull()).count())

print("\n=== Production ===")
print("Units_produced nulls:", df_production_silver.filter(col("units_produced").isNull()).count())
print("Defect_count nulls:", df_production_silver.filter(col("defect_count").isNull()).count())

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

# ================================
# SENSOR READINGS - Remove duplicates based on reading_id
# ================================
# Strategy: Keep the first occurrence of each reading_id, drop the rest
df_sensor_silver = df_sensor_silver.withColumn(
    "row_num", 
    row_number().over(Window.partitionBy("reading_id").orderBy("ingestion_timestamp"))
).filter(col("row_num") == 1).drop("row_num")

# COMMAND ----------

# ================================
# MAINTENANCE LOGS - Remove duplicates based on log_id
# ================================
df_maintenance_silver = df_maintenance_silver.withColumn(
    "row_num",
    row_number().over(Window.partitionBy("log_id").orderBy("ingestion_timestamp"))
).filter(col("row_num") == 1).drop("row_num")

# COMMAND ----------

# ================================
# Verify duplicates removed
# ================================
print("Sensor reading_id count:", df_sensor_silver.count())
print("Sensor distinct reading_id count:", df_sensor_silver.select("reading_id").distinct().count())
print("Match?", df_sensor_silver.count() == df_sensor_silver.select("reading_id").distinct().count())

print("\nMaintenance log_id count:", df_maintenance_silver.count())
print("Maintenance distinct log_id count:", df_maintenance_silver.select("log_id").distinct().count())
print("Match?", df_maintenance_silver.count() == df_maintenance_silver.select("log_id").distinct().count())

# COMMAND ----------

# Define silver path (same pattern as bronze)
container_name = "bronze"
storage_account_name = "myazurestorageanalytics"

silver_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/silver"

# ================================
# Write all 4 Silver tables to Delta
# ================================
df_machines_silver.write.format("delta").mode("overwrite").save(f"{silver_path}/machines")
df_sensor_silver.write.format("delta").mode("overwrite").save(f"{silver_path}/sensor")
df_maintenance_silver.write.format("delta").mode("overwrite").save(f"{silver_path}/maintenance")
df_production_silver.write.format("delta").mode("overwrite").save(f"{silver_path}/production")

print("All Silver tables written successfully!")

# COMMAND ----------

# ================================
# Verify Silver writes
# ================================
print("=== Silver Layer Verification ===\n")

print("Machines row count:", df_machines_silver.count())
print("Sensor row count:", df_sensor_silver.count())
print("Maintenance row count:", df_maintenance_silver.count())
print("Production row count:", df_production_silver.count())

# List files in silver folder to confirm Delta format
print("\n=== File structure check ===")
dbutils.fs.ls(f"{silver_path}/machines")
dbutils.fs.ls(f"{silver_path}/sensor")
dbutils.fs.ls(f"{silver_path}/maintenance")
dbutils.fs.ls(f"{silver_path}/production")
