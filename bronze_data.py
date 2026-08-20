# Databricks notebook source
storage_account_name = "myazurestorageanalytics"
storage_account_key = ------

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key
)

# COMMAND ----------

container_name = "bronze"

def abfss_path(container, path):
    return f"abfss://{container}@{storage_account_name}.dfs.core.windows.net/{path}"


machines_path = abfss_path(container_name, "source/machines.csv")

# COMMAND ----------

container_name = "bronze"

def abfss_path(container, path):
    return f"abfss://{container}@{storage_account_name}.dfs.core.windows.net/{path}"

machines_path = abfss_path(container_name, "machines.csv")
maintenance_logs_path = abfss_path(container_name, "maintenance_logs.csv")
production_output_path = abfss_path(container_name, "production_output.csv")
sensor_readings_path = abfss_path(container_name, "sensor_readings.csv")



# COMMAND ----------

df_machines = spark.read.csv(
    machines_path,
    header = True,
    inferSchema = True
)

df_machines.show()
df_machines.printSchema()

# COMMAND ----------

df_maintenance = spark.read.csv(
    maintenance_logs_path,
    header = True,
    inferSchema = True
)

df_maintenance.show()
df_maintenance.printSchema()


# COMMAND ----------

df_production = spark.read.csv(
    production_output_path,
    header = True,
    inferSchema = True
)

df_production.show()
df_production.printSchema()


# COMMAND ----------


df_sensor = spark.read.csv(
    sensor_readings_path,
    header = True,
    inferSchema = True
)
df_sensor.show()
df_sensor.printSchema()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit
df_machines = df_machines.withColumn("ingestion_timestamp", current_timestamp()) \
                          .withColumn("source_file", lit("machines.csv"))

# COMMAND ----------


# Sensor Readings
df_sensor = df_sensor.withColumn("ingestion_timestamp", current_timestamp()) \
                      .withColumn("source_file", lit("sensor_readings.csv"))

# Maintenance Logs
df_maintenance = df_maintenance.withColumn("ingestion_timestamp", current_timestamp()) \
                                .withColumn("source_file", lit("maintenance_logs.csv"))

# Production Output
df_production = df_production.withColumn("ingestion_timestamp", current_timestamp()) \
                              .withColumn("source_file", lit("production_output.csv"))

# COMMAND ----------

bronze_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/bronze"

df_machines.write.format("delta").mode("overwrite").save(f"{bronze_path}/machines")

# COMMAND ----------

bronze_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/bronze"

df_maintenance.write.format("delta").mode("overwrite").save(f"{bronze_path}/maintenance")

# COMMAND ----------

bronze_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/bronze"

df_production.write.format("delta").mode("overwrite").save(f"{bronze_path}/production")

# COMMAND ----------

bronze_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/bronze"

df_sensor.write.format("delta").mode("overwrite").save(f"{bronze_path}/sensor")

# COMMAND ----------

dbutils.fs.ls(f"{bronze_path}/machines")
dbutils.fs.ls(f"{bronze_path}/maintenance")
dbutils.fs.ls(f"{bronze_path}/production")
dbutils.fs.ls(f"{bronze_path}/sensor")
