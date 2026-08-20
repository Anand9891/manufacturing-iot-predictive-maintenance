# Manufacturing IoT Predictive Maintenance Pipeline

A complete, production-grade **Medallion Architecture** data pipeline built on **Azure Databricks** and **Delta Lake**.

## Overview

This project demonstrates an end-to-end data engineering solution for IoT sensor data, maintenance logs, and production metrics from manufacturing equipment.

### Architecture
Raw Data (CSVs)
↓
[BRONZE LAYER] - Raw ingestion with lineage metadata
↓
[SILVER LAYER] - Cleaned, deduplicated, standardized schemas
↓
[GOLD LAYER] - Business-ready reporting tables


## Data Pipeline

### Bronze Layer (Raw)
- **machines**: 40 equipment records (master data)
- **sensor_readings**: 120,600 IoT readings (temperature, vibration, pressure, RPM)
- **maintenance_logs**: 606 maintenance events with downtime tracking
- **production_output**: 7,200 daily production records

**Key features:**
- Metadata columns: `ingestion_timestamp`, `source_file` (for lineage)
- Raw data preservation (no transformations)
- Delta format for ACID compliance

### Silver Layer (Cleaned)
- Null handling: Filled safe columns (manufacturer, downtime), kept sensor nulls (signal)
- Type casting: Proper timestamps, doubles, longs, dates
- Deduplication: Removed ~600 sensor duplicates, ~6 maintenance duplicates
- Format standardization: Parsed 4 different timestamp formats into consistent schema

**Key metrics:**
- 120,000 sensor rows (deduplicated)
- 600 maintenance logs
- 7,200 production records
- Zero null handling errors

### Gold Layer (Reporting)
Four business-ready tables for analytics and dashboards:

1. **machine_health** (40 rows)
   - Per-machine health snapshot
   - Avg sensor readings (temperature, vibration, pressure)
   - Lifetime production volume
   - Downtime percentage
   - Last maintenance info

2. **maintenance_analytics** (40 rows)
   - Total maintenance events per machine
   - Average downtime per event
   - Last maintenance date

3. **production_analytics** (7,200 rows)
   - Daily production aggregates
   - Units produced, defect counts
   - Defect rate (%)

4. **sensor_anomalies** (4,956 rows)
   - Flagged anomalies (temp > 90°C, vibration > 4.0, pressure > 150 PSI)
   - Potential fault detection for predictive maintenance

## Technical Stack

- **Platform**: Azure Databricks (Spark 13.3)
- **Storage**: Azure Data Lake Storage Gen2 (ADLS)
- **Format**: Delta Lake (ACID transactions, schema enforcement)
- **Orchestration**: Azure Data Factory (ADF) pipelines
- **Languages**: PySpark, SQL

## Key Accomplishments

✅ **Medallion Architecture** implemented end-to-end  
✅ **128k+ rows** flowing through production pipeline  
✅ **Real-world data quality** challenges solved:
  - Mixed timestamp formats (4 different patterns parsed consistently)
  - Null value strategy (3-4% nulls handled per column type)
  - Duplicate rows (0.5% sensor, 1% maintenance removed)
  - Type inference issues (explicit schema casting)

✅ **Sensor anomaly detection** (4.1% of readings flagged as faults)  
✅ **Delta Lake** reliability (ACID compliance, time travel capable)  
✅ **Scalable design** ready for production volumes
