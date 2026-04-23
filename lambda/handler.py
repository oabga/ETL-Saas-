"""
AWS Lambda entry point for the ETL pipeline.

Triggered by an S3 PUT event when a user uploads a CSV file.
Orchestrates the full Extract → Transform → Load flow:

  1. Parse the S3 event to identify the uploaded object
  2. Read the CSV into a DataFrame
  3. Apply data-cleaning transformations
  4. Register the detected schema
  5. Write the result as Parquet to the data-lake bucket
  6. Track job status in DynamoDB throughout the process
"""

import json
import logging
import uuid
from datetime import datetime

from etl_processor import transform_data
from job_tracker import get_existing_job, update_job_status
from s3_io import read_csv_from_s3, save_to_datalake
from schema_registry import detect_and_register_schema

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    """AWS Lambda handler — entry point for S3-triggered ETL jobs."""
    try:
        bucket, key, user_id, job_id, original_filename = _parse_s3_event(event)

        logger.info(
            "Processing job=%s  user=%s  file=%s", job_id, user_id, original_filename
        )

        # Preserve fields originally written by the backend API
        existing_job = get_existing_job(job_id)

        update_job_status(
            job_id,
            user_id,
            "PROCESSING",
            {
                "source_bucket": bucket,
                "source_key": key,
                "started_at": datetime.utcnow().isoformat(),
            },
            existing_job,
        )

        # --- Extract ---
        df = read_csv_from_s3(bucket, key)
        logger.info("Loaded %d rows × %d columns", len(df), len(df.columns))

        # --- Transform ---
        df = transform_data(df)
        logger.info("After cleaning: %d rows × %d columns", len(df), len(df.columns))

        # --- Schema detection ---
        schema_id = detect_and_register_schema(df, user_id, original_filename)
        logger.info("Schema ID: %s", schema_id)

        # --- Load ---
        output_path = save_to_datalake(df, user_id, schema_id, job_id)
        logger.info("Output: %s", output_path)

        update_job_status(
            job_id,
            user_id,
            "COMPLETED",
            {
                "source_bucket": bucket,
                "source_key": key,
                "output_path": output_path,
                "ended_at": datetime.utcnow().isoformat(),
            },
            existing_job,
        )

        return {
            "statusCode": 200,
            "body": json.dumps(
                {
                    "message": "Success",
                    "job_id": job_id,
                    "output_path": output_path,
                }
            ),
        }

    except Exception as exc:
        logger.exception("ETL pipeline failed")

        if "job_id" in locals() and "user_id" in locals():
            ej = existing_job if "existing_job" in locals() else get_existing_job(job_id)
            update_job_status(
                job_id,
                user_id,
                "FAILED",
                {
                    "error": str(exc),
                    "failed_at": datetime.utcnow().isoformat(),
                },
                ej,
            )

        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(exc)}),
        }


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _parse_s3_event(event: dict) -> tuple:
    """
    Extract bucket, key, user_id, job_id, and original filename
    from the S3 event payload.

    Expected key format::

        uploads/<user_id>/<job_id>_<original_filename>.csv
    """
    record = event["Records"][0]["s3"]
    bucket = record["bucket"]["name"]
    key = record["object"]["key"]

    parts = key.split("/")
    user_id = parts[1] if len(parts) > 1 else "anonymous"

    filename_part = parts[-1] if parts else ""

    if "_" in filename_part:
        job_id = filename_part.split("_")[0]
        original_filename = "_".join(filename_part.split("_")[1:])
    else:
        job_id = str(uuid.uuid4())
        original_filename = filename_part

    return bucket, key, user_id, job_id, original_filename