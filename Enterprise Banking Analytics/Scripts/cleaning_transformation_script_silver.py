# Databricks notebook source
# ==============================================================================
# PIPELINE: Bronze to Silver Transformation
# SCRIPT NAME: cleaning_transformation_script
# DESCRIPTION: Applies standard 5-step data cleaning, data typing, and PII masking
#              across separated source schemas.
# ==============================================================================

from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# 1. Create separate Silver schemas
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.core_banking_silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.credit_loans_silver")

# Define sources and their target silver schemas
pipeline_sources = [
    {
        "bronze_schema": "workspace.core_banking_bronze",
        "silver_schema": "workspace.core_banking_silver",
        "tables": ["accounts", "customers", "fraud_alerts", "transactions"]
    },
    {
        "bronze_schema": "workspace.credit_loans_bronze",
        "silver_schema": "workspace.credit_loans_silver",
        "tables": ["credit_cards", "credit_scores", "loan_applications", "loan_repayments"]
    }
]

# Sensitive PII columns to mask using SHA-256 (keeps IDs joinable while masking readable text)
PII_COLUMNS = ["email", "first_name", "last_name", "customer_name", "full_name", "phone_number", "ssn"]

# Loop through each domain and clean tables
for config in pipeline_sources:
    bronze_schema = config["bronze_schema"]
    silver_schema = config["silver_schema"]
    
    for table_name in config["tables"]:
        source_table = f"{bronze_schema}.{table_name}"
        target_table = f"{silver_schema}.silver_{table_name}"
        
        print(f"--- Processing {source_table} -> {target_table} ---")
        
        # Read Bronze table
        df = spark.read.table(source_table)
        
        # ----------------------------------------------------------------------
        # STEP 1: Standardize Column Names & Trim Whitespace
        # ----------------------------------------------------------------------
        for col_name in df.columns:
            clean_col_name = col_name.strip().lower()
            if clean_col_name != col_name:
                df = df.withColumnRenamed(col_name, clean_col_name)
        
        # Trim whitespace in string cells
        string_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]
        for col_name in string_cols:
            df = df.withColumn(col_name, F.trim(F.col(col_name)))
            
        # ----------------------------------------------------------------------
        # STEP 2: PII Data Masking (Security Layer)
        # ----------------------------------------------------------------------
        for col_name in df.columns:
            if any(pii_term in col_name for pii_term in PII_COLUMNS):
                print(f"  🔒 Masking PII Column: {col_name}")
                df = df.withColumn(col_name, F.sha2(F.col(col_name), 256))
                
        # ----------------------------------------------------------------------
        # STEP 3: Handle Nulls / Blank Strings
        # ----------------------------------------------------------------------
        for col_name in string_cols:
            df = df.withColumn(col_name, F.when(F.col(col_name) == "", None).otherwise(F.col(col_name)))
            
        # ----------------------------------------------------------------------
        # STEP 4: Deduplication
        # ----------------------------------------------------------------------
        df = df.dropDuplicates()
        
        # ----------------------------------------------------------------------
        # STEP 5: Save as Delta Table in Silver
        # ----------------------------------------------------------------------
        df.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(target_table)
        
        print(f"✓ Successfully created: {target_table}\n")

print("==================================================")
print("  All Core Banking & Credit Loans Silver Tables Created! ")
print("==================================================")