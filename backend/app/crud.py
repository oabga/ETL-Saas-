"""
DynamoDB CRUD operations for ETL jobs.

Each job record tracks the lifecycle of a single CSV upload:
QUEUED → PROCESSING → COMPLETED | FAILED.
"""

import logging
from datetime import datetime
from typing import Any, Dict, List

import boto3

from config import AWS_REGION, JOBS_TABLE

logger = logging.getLogger(__name__)
dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)


def create_job(job_id: str, user_id: str, filename: str, s3_key: str) -> None:
    """
    Insert a new job record with status ``QUEUED``.

    Called by the upload endpoint immediately after the CSV is
    written to the source S3 bucket.
    """
    table = dynamodb.Table(JOBS_TABLE)
    table.put_item(
        Item={
            "jobId": job_id,
            "userId": user_id,
            "status": "QUEUED",
            "filename": filename,
            "s3_key": s3_key,
            "created_at": datetime.utcnow().isoformat(),
        }
    )
    logger.info("Created job %s for user %s", job_id, user_id)


def get_jobs_by_user(user_id: str) -> List[Dict[str, Any]]:
    """
    Return all jobs belonging to *user_id*.

    .. note::
       This currently uses ``scan`` with a filter expression.
       For production at scale, add a GSI on ``userId`` and switch
       to ``query`` to avoid full-table scans.
    """
    table = dynamodb.Table(JOBS_TABLE)
    response = table.scan(
        FilterExpression="userId = :uid",
        ExpressionAttributeValues={":uid": user_id},
    )
    items = response.get("Items", [])

    return [_normalize_job(item) for item in items]


def get_job(job_id: str) -> Dict[str, Any]:
    """
    Fetch a single job by its primary key.

    Returns an empty dict if the job does not exist.
    """
    table = dynamodb.Table(JOBS_TABLE)
    response = table.get_item(Key={"jobId": job_id})
    item = response.get("Item")

    if item is None:
        return {}

    return _normalize_job(item)


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _normalize_job(item: Dict[str, Any]) -> Dict[str, Any]:
    """
    Map DynamoDB snake_case field names to camelCase for the API
    response, keeping a consistent contract with the frontend.
    """
    return {
        "jobId": item.get("jobId"),
        "userId": item.get("userId"),
        "filename": item.get("filename"),
        "status": item.get("status"),
        "s3Key": item.get("s3_key"),
        "createdAt": item.get("created_at"),
        "metadata": item.get("metadata", {}),
    }