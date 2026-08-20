# Databricks notebook source
# Define paths
container_name = "bronze"
storage_account_name = "myazurestorageanalytics"
silver_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/silver"
gold_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/gold"

from pyspark.sql.functions import col, max, avg, min, datediff, current_date, count, when, sum as spark_sum


# ================================
# READ SILVER TABLES
# ================================
df_machines = spark.read.format("delta").load(f"{silver_path}/machines")
df_sensor = spark.read.format("delta").load(f"{silver_path}/sensor")
df_maintenance = spark.read.format("delta").load(f"{silver_path}/maintenance")
df_production = spark.read.format("delta").load(f"{silver_path}/production")

print("All Silver tables loaded successfully!")
...

# COMMAND ----------

# Step 2: Calculate production metrics (grouped by machine)
production_metrics = df_production.groupBy("machine_id").agg(
    count("record_id").alias("total_production_records"),
    spark_sum("units_produced").alias("lifetime_units_produced"),
    count(when(col("units_produced").isNull(), 1)).alias("downtime_days")
)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

# Step 3: Get latest maintenance info per machine (using window function)
latest_maintenance = df_maintenance.withColumn(
    "row_num",
    row_number().over(Window.partitionBy("machine_id").orderBy(col("maintenance_date").desc()))
).filter(col("row_num") == 1).select(
    "machine_id",
    col("maintenance_date").alias("last_maintenance_date"),
    col("issue_type").alias("last_issue_type")
).drop("row_num")

# COMMAND ----------

# Step 1: Calculate sensor averages (grouped by machine)
sensor_metrics = df_sensor.groupBy("machine_id").agg(
    avg("temperature").alias("avg_temperature"),
    avg("vibration").alias("avg_vibration"),
    avg("pressure").alias("avg_pressure")
)

print("Sensor metrics calculated successfully!")

# COMMAND ----------

# Step 4: Join everything together
df_machine_health = df_machines.join(
    sensor_metrics, "machine_id", "left"
).join(
    production_metrics, "machine_id", "left"
).join(
    latest_maintenance, "machine_id", "left"
)

# COMMAND ----------

# Step 5: Skip the problematic datediff, just calculate downtime percentage
df_machine_health = df_machine_health.withColumn(
    "downtime_percentage",
    (col("downtime_days") / col("total_production_records") * 100).cast("decimal(5,2)")
)

# COMMAND ----------

# Re-authenticate for this session
container_name = "bronze"
storage_account_name = "myazurestorageanalytics"
storage_account_key = "cMXFyIh5jvxhUSxYsFclZAVRX6hETV0MOmR03d2pMw3Phb6vPsWClEqRNGx9b6Unr2HZBjztEbm3+AStzBNJ/Q=="  # your actual key

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key
)

print("Azure auth configured!")

# COMMAND ----------

production_metrics.printSchema()
print("Production metrics count:", production_metrics.count())

# COMMAND ----------

# Simpler Gold approach: build Machine_Health directly from Silver
df_machine_health_gold = (
    df_machines
    .join(sensor_metrics, "machine_id", "left")
    .join(production_metrics, "machine_id", "left")
    .join(latest_maintenance, "machine_id", "left")
    .select(
        "machine_id",
        "machine_type",
        "plant_location",
        "manufacturer",
        "last_maintenance_date",
        "last_issue_type",
        col("lifetime_units_produced").alias("lifetime_units_produced"),
        col("downtime_days").alias("downtime_days"),
        (col("downtime_days") / col("total_production_records") * 100).cast("decimal(5,2)").alias("downtime_percentage"),
        col("avg_temperature").alias("avg_temperature"),
        col("avg_vibration").alias("avg_vibration"),
        col("avg_pressure").alias("avg_pressure")
    )
)

# Write to Gold
df_machine_health_gold.write.format("delta").mode("overwrite").save(f"{gold_path}/machine_health")

print("Machine_Health Gold table written!")
print("Total rows:", df_machine_health_gold.count())

# COMMAND ----------

# Maintenance_Analytics - aggregated maintenance stats per machine
df_maintenance_analytics = (
    df_machines.select("machine_id", "machine_type", "plant_location")
    .join(
        df_maintenance.groupBy("machine_id").agg(
            count("log_id").alias("total_maintenance_events"),
            avg("downtime_hours").alias("avg_downtime_per_event"),
            max("maintenance_date").alias("last_maintenance_date")
        ),
        "machine_id",
        "left"
    )
)

df_maintenance_analytics.write.format("delta").mode("overwrite").save(f"{gold_path}/maintenance_analytics")
print("Maintenance_Analytics written! Rows:", df_maintenance_analytics.count())

# COMMAND ----------

from pyspark.sql.functions import col, count, max, avg, lit, first, try_divide

# Production_Analytics - daily production aggregates (simplified)
df_production_analytics = (
    df_production
    .groupBy("machine_id", "date").agg(
        first("units_produced").alias("units_produced"),
        first("defect_count").alias("defect_count")
    )
    .withColumn(
        "defect_rate",
        (when(col("units_produced") != 0, col("defect_count") / col("units_produced")).otherwise(0) * 100).cast("decimal(5,2)")
    )
    .join(df_machines.select("machine_id", "machine_type", "plant_location"), "machine_id", "left")
)

df_production_analytics.write.format("delta").mode("overwrite").save(f"{gold_path}/production_analytics")
print("Production_Analytics written! Rows:", df_production_analytics.count())

# COMMAND ----------

# Sensor_Anomalies - flag readings with extreme values
df_sensor_anomalies = (
    df_sensor
    .filter(
        (col("temperature") > 90) | (col("vibration") > 4.0) | (col("pressure") > 150)
    )
    .select(
        "reading_id",
        "machine_id",
        "timestamp",
        "temperature",
        "vibration",
        "pressure",
        lit("potential_fault_detected").alias("anomaly_flag")
    )
    .join(df_machines.select("machine_id", "machine_type"), "machine_id", "left")
)

df_sensor_anomalies.write.format("delta").mode("overwrite").save(f"{gold_path}/sensor_anomalies")
print("Sensor_Anomalies written! Rows:", df_sensor_anomalies.count())

# COMMAND ----------

# Quick verification
print("\n=== Gold Layer Summary ===")
print("Machine_Health:", spark.read.format("delta").load(f"{gold_path}/machine_health").count(), "rows")
print("Maintenance_Analytics:", spark.read.format("delta").load(f"{gold_path}/maintenance_analytics").count(), "rows")
print("Production_Analytics:", spark.read.format("delta").load(f"{gold_path}/production_analytics").count(), "rows")
print("Sensor_Anomalies:", spark.read.format("delta").load(f"{gold_path}/sensor_anomalies").count(), "rows")