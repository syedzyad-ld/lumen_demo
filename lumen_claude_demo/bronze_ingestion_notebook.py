# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer Ingestion - Source Dataset
# MAGIC **Source:** `/Volumes/catalog_demo/dev/source_dataset/sample_-_superstore.xls`  
# MAGIC **Target Catalog:** `dev_bronze_test`  
# MAGIC **Target Schema:** `raw`  
# MAGIC This notebook reads all sheets from the Excel source file and stores each sheet as a Delta table in the Bronze layer.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create Catalog and Schema

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS dev_bronze_test MANAGED LOCATION 's3://databricks-workspace-stack-9a61f-bucket/unity-catalog/6483314808213883/zyad_demo';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS dev_bronze_test.raw;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Install and Import Required Libraries

# COMMAND ----------

# Install openpyxl for reading Excel files
%pip install openpyxl xlrd

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, input_file_name

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Read Excel File and Discover All Sheets

# COMMAND ----------

# Source file path
source_path = "/Volumes/catalog_demo/dev/source_dataset/sample_-_superstore.xls"

# Read all sheet names from the Excel file
xls = pd.ExcelFile(source_path)
sheet_names = xls.sheet_names

print(f"Source file: {source_path}")
print(f"Number of sheets found: {len(sheet_names)}")
print(f"Sheet names: {sheet_names}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Read Each Sheet and Write as Delta Table

# COMMAND ----------

# Process each sheet
for sheet_name in sheet_names:
    print(f"\n{'='*60}")
    print(f"Processing sheet: '{sheet_name}'")
    print(f"{'='*60}")
    
    # Read sheet into pandas DataFrame
    pdf = pd.read_excel(source_path, sheet_name=sheet_name)
    
    print(f"  Rows: {len(pdf)}")
    print(f"  Columns: {list(pdf.columns)}")
    
    # Clean column names: replace spaces and special chars with underscore, lowercase
    pdf.columns = [
        col.lower()
           .replace(' ', '_')
           .replace('/', '_')
           .replace('-', '_')
           .replace('(', '')
           .replace(')', '')
           .replace('.', '_')
        for col in pdf.columns
    ]
    
    print(f"  Cleaned columns: {list(pdf.columns)}")
    
    # Convert pandas to Spark DataFrame
    spark_df = spark.createDataFrame(pdf)
    
    # Add ingestion metadata columns
    spark_df = spark_df \
        .withColumn("_ingestion_timestamp", current_timestamp()) \
        .withColumn("_source_file", lit(source_path)) \
        .withColumn("_source_sheet", lit(sheet_name))
    
    # Create table name from sheet name (clean it)
    table_name = sheet_name.lower().replace(' ', '_').replace('-', '_')
    full_table_name = f"dev_bronze_test.raw.{table_name}"
    
    # Write as Delta table (overwrite mode for idempotency)
    spark_df.write \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(full_table_name)
    
    print(f"  Written to: {full_table_name}")
    print(f"  Total rows written: {spark_df.count()}")
    print(f"  Schema:")
    spark_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Verify Tables Created

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN dev_bronze_test.raw;

# COMMAND ----------

# Display row counts for each table
tables = spark.sql("SHOW TABLES IN dev_bronze_test.raw").collect()

print("\n" + "="*60)
print("BRONZE LAYER INGESTION SUMMARY")
print("="*60)
print(f"{'Table':<40} {'Rows':<10} {'Columns':<10}")
print("-"*60)

for table in tables:
    tbl_name = f"dev_bronze_test.raw.{table.tableName}"
    count = spark.sql(f"SELECT COUNT(*) as cnt FROM {tbl_name}").collect()[0].cnt
    cols = len(spark.sql(f"DESCRIBE {tbl_name}").collect())
    print(f"{tbl_name:<40} {count:<10} {cols:<10}")

print("-"*60)
print("Bronze ingestion complete!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Preview Each Table

# COMMAND ----------

# Preview all tables
tables = spark.sql("SHOW TABLES IN dev_bronze_test.raw").collect()
for table in tables:
    tbl_name = f"dev_bronze_test.raw.{table.tableName}"
    print(f"\n--- {tbl_name} ---")
    display(spark.sql(f"SELECT * FROM {tbl_name} LIMIT 5"))