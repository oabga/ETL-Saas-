"""
FastAPI application — ETL SaaS API.

Provides REST endpoints for:
  - Uploading CSV files to S3
  - Listing and inspecting processing jobs
  - Downloading processed Parquet files
  - Previewing processed data
"""

import logging
import uuid
from io import BytesIO
from typing import List

import boto3
import pandas as pd
from fastapi import FastAPI, UploadFile, File, Depends, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware

from auth import get_current_user
from config import UPLOAD_BUCKET, DATALAKE_BUCKET, CORS_ORIGINS
from crud import create_job, get_jobs_by_user, get_job
from schemas import (
    DownloadResponse,
    JobListItem,
    JobResponse,
    PreviewResponse,
    UploadResponse,
)

logger = logging.getLogger(__name__)

# ------------------------------------------------------------------
# App initialisation
# ------------------------------------------------------------------

app = FastAPI(
    title="ETL SaaS API",
    description="Automated CSV-to-Parquet data processing pipeline",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

s3 = boto3.client("s3")


# ------------------------------------------------------------------
# Routes
# ------------------------------------------------------------------

@app.get("/health")
async def health_check():
    """Liveness probe for Kubernetes / ALB health checks."""
    return {"status": "ok"}


@app.post("/api/upload", response_model=UploadResponse)
async def upload_csv(
    file: UploadFile = File(...),
    user=Depends(get_current_user),
):
    """
    Upload a CSV file for ETL processing.

    The file is stored in the source S3 bucket, which triggers
    the Lambda ETL pipeline via an S3 event notification.
    """
    user_id = user.get("sub", "anonymous")

    if not file.filename.endswith(".csv"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Only .csv files are accepted",
        )

    job_id = str(uuid.uuid4())
    key = f"uploads/{user_id}/{job_id}_{file.filename}"

    s3.put_object(Bucket=UPLOAD_BUCKET, Key=key, Body=await file.read())
    create_job(job_id, user_id, file.filename, key)

    return UploadResponse(job_id=job_id, status="QUEUED")


@app.get("/api/jobs", response_model=List[JobListItem])
def list_jobs(user=Depends(get_current_user)):
    """Return all jobs belonging to the authenticated user."""
    return get_jobs_by_user(user.get("sub"))


@app.get("/api/jobs/{job_id}", response_model=JobResponse)
def get_job_detail(job_id: str, user=Depends(get_current_user)):
    """Return full details for a single job."""
    job = get_job(job_id)

    if not job or job.get("userId") != user.get("sub"):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN)

    return job


@app.get("/api/jobs/{job_id}/download", response_model=DownloadResponse)
def download_parquet(job_id: str, user=Depends(get_current_user)):
    """
    Generate a time-limited pre-signed URL for downloading the
    processed Parquet file.
    """
    job = get_job(job_id)

    if not job or job.get("userId") != user.get("sub"):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN)

    output_path = job.get("metadata", {}).get("output_path")
    if not output_path:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Processed Parquet file not found",
        )

    bucket, key = _parse_s3_uri(output_path)
    url = s3.generate_presigned_url(
        "get_object",
        Params={"Bucket": bucket, "Key": key},
        ExpiresIn=3600,
    )

    return DownloadResponse(download_url=url)


@app.get("/api/jobs/{job_id}/preview", response_model=PreviewResponse)
async def preview_parquet(job_id: str, user=Depends(get_current_user)):
    """Return the first 100 rows of the processed Parquet file."""
    job = get_job(job_id)

    if not job or job.get("userId") != user.get("sub"):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN)

    if job.get("status") != "COMPLETED":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Job has not completed yet",
        )

    output_path = job.get("metadata", {}).get("output_path")
    if not output_path:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Processed Parquet file not found",
        )

    bucket, key = _parse_s3_uri(output_path)

    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        df = pd.read_parquet(BytesIO(response["Body"].read()))

        preview_df = df.head(100)
        data = [
            [str(val) if pd.notna(val) else None for val in row]
            for _, row in preview_df.iterrows()
        ]

        return PreviewResponse(
            columns=preview_df.columns.tolist(),
            data=data,
            preview_rows=len(data),
            total_rows=len(df),
            job_id=job_id,
            filename=job.get("filename"),
        )

    except Exception as exc:
        logger.exception("Failed to preview job %s", job_id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to preview: {exc}",
        )


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _parse_s3_uri(uri: str) -> tuple:
    """Split ``s3://bucket/key`` into (bucket, key)."""
    path = uri.replace("s3://", "")
    parts = path.split("/", 1)
    return parts[0], parts[1]