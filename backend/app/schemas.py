"""
Pydantic response models for the ETL SaaS API.

These models serve as the single source of truth for API contracts
and are referenced by FastAPI's ``response_model`` parameter to
provide automatic validation and OpenAPI documentation.
"""

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


# ------------------------------------------------------------------
# Job models
# ------------------------------------------------------------------

class JobBase(BaseModel):
    """Fields common to every job representation."""
    jobId: str = Field(..., description="Unique job identifier (UUID)")
    status: str = Field(..., description="Current status: QUEUED | PROCESSING | COMPLETED | FAILED")
    filename: Optional[str] = Field(None, description="Original CSV filename")


class JobResponse(JobBase):
    """Full job detail returned by ``GET /api/jobs/{job_id}``."""
    userId: Optional[str] = None
    s3Key: Optional[str] = None
    createdAt: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


class JobListItem(JobBase):
    """Compact representation used in the jobs list."""
    s3Key: Optional[str] = None
    createdAt: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


# ------------------------------------------------------------------
# Upload models
# ------------------------------------------------------------------

class UploadResponse(BaseModel):
    """Response returned after a successful CSV upload."""
    job_id: str = Field(..., description="Newly created job ID")
    status: str = Field(default="QUEUED", description="Initial job status")


# ------------------------------------------------------------------
# Download models
# ------------------------------------------------------------------

class DownloadResponse(BaseModel):
    """Pre-signed S3 URL for downloading the processed Parquet file."""
    download_url: str


# ------------------------------------------------------------------
# Preview models
# ------------------------------------------------------------------

class PreviewResponse(BaseModel):
    """Paginated preview of the processed Parquet data."""
    columns: List[str] = Field(..., description="Column names")
    data: List[List[Optional[str]]] = Field(..., description="Row data (stringified)")
    preview_rows: int = Field(..., description="Number of rows in this preview")
    total_rows: int = Field(..., description="Total rows in the Parquet file")
    job_id: str
    filename: Optional[str] = None