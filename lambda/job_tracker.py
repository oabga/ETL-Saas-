"""
Job lifecycle tracker backed by DynamoDB.

Every CSV upload creates a *job* that transitions through:

    QUEUED → PROCESSING → COMPLETED | FAILED

This module handles reading and updating those records while
preserving fields set by the backend (filename, created_at, …).
"""

import logging
from datetime import datetime
from typing import Any, Dict, Optional

import boto3

from config import JOBS_TABLE

logger = logging.getLogger(__name__)
dynamodb = boto3.resource("dynamodb")


def get_existing_job(job_id: str) -> Dict[str, Any]:
    """
    Fetch the current job record from DynamoDB.

    Returns an empty dict if the job does not exist or on error.
    """
    try:
        table = dynamodb.Table(JOBS_TABLE)
        response = table.get_item(Key={"jobId": job_id})
        return response.get("Item", {})
    except Exception:
        logger.exception("Failed to fetch job %s", job_id)
        return {}


def update_job_status(
    job_id: str,
    user_id: str,
    status: str,
    metadata: Dict[str, Any],
    existing_job: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Write (or overwrite) the job record with a new *status*.

    Fields originally written by the backend API (``filename``,
    ``s3_key``, ``created_at``) are preserved from *existing_job*
    so they are not lost when Lambda updates the status.
    """
    if existing_job is None:
        existing_job = {}

    table = dynamodb.Table(JOBS_TABLE)

    item = {
        "jobId": job_id,
        "userId": user_id,
        "status": status,
        "timestamp": int(datetime.utcnow().timestamp()),
        "metadata": metadata,
        "filename": existing_job.get("filename"),
        "s3_key": existing_job.get("s3_key"),
        "created_at": existing_job.get("created_at"),
    }

    table.put_item(Item=item)
    logger.info("Job %s → %s", job_id, status)
