"""
Centralized configuration for the ETL Lambda function.

All environment-dependent values are loaded from environment variables
with sensible defaults for local development.
"""

import os


# --- AWS DynamoDB Tables ---
JOBS_TABLE = os.environ.get("JOBS_TABLE", "JobsTable")
SCHEMA_TABLE = os.environ.get("SCHEMA_TABLE", "SchemaTable")

# --- AWS S3 Buckets ---
DATALAKE_BUCKET = os.environ.get("DATALAKE_BUCKET", "data-lake-bucket-processed")

# --- Processing Defaults ---
OUTLIER_IQR_MULTIPLIER = float(os.environ.get("OUTLIER_IQR_MULTIPLIER", "1.5"))
NUMERIC_FILL_STRATEGY = os.environ.get("NUMERIC_FILL_STRATEGY", "median")  # median | mean
CATEGORICAL_FILL_VALUE = os.environ.get("CATEGORICAL_FILL_VALUE", "Unknown")
