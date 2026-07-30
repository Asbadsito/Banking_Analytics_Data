# Databricks notebook source
# ==============================================================================
# PIPELINE: Silver to Gold Transformation
# SCRIPT NAME: gold_aggregation_script
# DESCRIPTION: Aggregates PII-masked Silver tables into 4 Star Schema Gold Data Marts
#              to power Power BI Dashboards and Executive Analytics.
# ==============================================================================

from pyspark.sql import functions as F

# 1. Ensure Gold Schema exists
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.gold")

print("--- Starting Gold Layer Transformations ---")

# ------------------------------------------------------------------------------
# MART 1: Executive Overview & Financial Health
# (Powers Dashboard Page 1: Deposit balances, DTI, Credit Ratings, Account Counts)
# ------------------------------------------------------------------------------
print("Building workspace.gold.gold_fact_financial_health...")

customers = spark.read.table("workspace.core_banking_silver.silver_customers")
accounts = spark.read.table("workspace.core_banking_silver.silver_accounts")
credit_scores = spark.read.table("workspace.credit_loans_silver.silver_credit_scores")

gold_financial_health = customers.alias("c") \
    .join(accounts.alias("a"), "customer_uuid", "left") \
    .join(credit_scores.alias("cs"), "customer_uuid", "left") \
    .groupBy(
        F.col("c.customer_uuid"), 
        F.col("c.city"), 
        F.col("c.country"), 
        F.col("c.employment_status"), 
        F.col("c.customer_segment"),
        F.col("c.annual_income")
    ) \
    .agg(
        F.countDistinct("a.account_id").alias("total_accounts_owned"),
        F.round(F.coalesce(F.sum("a.current_balance"), F.lit(0)), 2).alias("total_deposit_balance"),
        F.max("cs.bureau_score").alias("bureau_credit_score"),
        F.max("cs.risk_rating").alias("credit_risk_rating"),
        F.max("cs.debt_to_income_ratio").alias("dti_ratio"),
        F.max("cs.delinquency_history_count").alias("missed_payments_count")
    )

gold_financial_health.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.gold.gold_fact_financial_health")

print("✓ Successfully built workspace.gold.gold_fact_financial_health\n")


# ------------------------------------------------------------------------------
# MART 2: Transactions Analytics
# (Powers Dashboard Page 2: Spending Sectors, Channels, Monthly Volume & Pass/Fail Rates)
# ------------------------------------------------------------------------------
print("Building workspace.gold.gold_fact_transactions_analytics...")

transactions = spark.read.table("workspace.core_banking_silver.silver_transactions")

gold_transactions = transactions \
    .withColumn("transaction_month", F.date_trunc("month", F.col("transaction_timestamp"))) \
    .groupBy(
        "transaction_month", 
        "transaction_type", 
        "spending_sector", 
        "channel", 
        "status", 
        "transaction_city"
    ) \
    .agg(
        F.count("transaction_id").alias("total_transaction_count"),
        F.round(F.sum("amount"), 2).alias("total_transaction_amount"),
        F.round(F.avg("amount"), 2).alias("avg_transaction_amount")
    )

gold_transactions.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.gold.gold_fact_transactions_analytics")

print("✓ Successfully built workspace.gold.gold_fact_transactions_analytics\n")


# ------------------------------------------------------------------------------
# MART 3: Credit Cards & Loan Portfolio Performance
# (Powers Dashboard Page 3: Loan Status, Principal vs Interest Repaid, Late Fees)
# ------------------------------------------------------------------------------
print("Building workspace.gold.gold_fact_loans_and_credit...")

loans = spark.read.table("workspace.credit_loans_silver.silver_loan_applications")
repayments = spark.read.table("workspace.credit_loans_silver.silver_loan_repayments")

gold_loans = loans.alias("l") \
    .join(repayments.alias("r"), "loan_id", "left") \
    .groupBy(
        F.col("l.loan_type"), 
        F.col("l.loan_status"), 
        F.col("l.term_months")
    ) \
    .agg(
        F.countDistinct("l.loan_id").alias("total_loans_count"),
        F.round(F.sum("l.principal_amount"), 2).alias("total_principal_borrowed"),
        F.round(F.coalesce(F.sum("r.amount_paid"), F.lit(0)), 2).alias("total_amount_repaid"),
        F.round(F.coalesce(F.sum("r.principal_component"), F.lit(0)), 2).alias("principal_repaid_component"),
        F.round(F.coalesce(F.sum("r.interest_component"), F.lit(0)), 2).alias("interest_profit_earned"),
        F.round(F.coalesce(F.sum("r.late_fee_applied"), F.lit(0)), 2).alias("total_late_fees_collected")
    )

gold_loans.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.gold.gold_fact_loans_and_credit")

print("✓ Successfully built workspace.gold.gold_fact_loans_and_credit\n")


# ------------------------------------------------------------------------------
# MART 4: Security, Risk & Fraud Tracking
# (Powers Dashboard Page 4: Risk Scores, Alert Types & Resolution Statuses)
# ------------------------------------------------------------------------------
print("Building workspace.gold.gold_fact_fraud_risk...")

fraud = spark.read.table("workspace.core_banking_silver.silver_fraud_alerts")

gold_fraud = fraud \
    .groupBy("alert_type", "status") \
    .agg(
        F.count("alert_id").alias("total_alerts_raised"),
        F.round(F.avg("risk_score"), 2).alias("avg_risk_score"),
        F.max("risk_score").alias("max_risk_score")
    )

gold_fraud.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.gold.gold_fact_fraud_risk")

print("✓ Successfully built workspace.gold.gold_fact_fraud_risk\n")

print("==================================================")
print("  All 4 Gold summaries Successfully Generated!        ")
print("==================================================")