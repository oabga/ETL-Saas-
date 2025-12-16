from fastapi import FastAPI, UploadFile, File, Depends, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from auth import get_current_user
from crud import create_job, get_jobs_by_user, get_job
from schemas import Job
import uuid
import boto3
from datetime import timedelta
import pandas as pd
from io import StringIO, BytesIO

app = FastAPI(title="ETL SaaS API")

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

s3 = boto3.client('s3')
UPLOAD_BUCKET = 'source-bucket-oabga' 
DATALAKE_BUCKET = 'data-lake-bucket-processed'

@app.get("/health")
async def health_check():
    return {"status": "ok"}

@app.post("/api/upload")
async def upload_csv(file: UploadFile = File(...), user=Depends(get_current_user)):
    user_id = user.get('sub', 'anonymous')
    if not file.filename.endswith('.csv'):
        raise HTTPException(400, "Only CSV allowed")
    
    job_id = str(uuid.uuid4())
    key = f"uploads/{user_id}/{job_id}_{file.filename}"
    
    s3.put_object(Bucket=UPLOAD_BUCKET, Key=key, Body=await file.read())
    
    create_job(job_id, user_id, file.filename, key)
    
    return {"job_id": job_id, "status": "QUEUED"}

@app.get("/api/jobs")
def list_jobs(user=Depends(get_current_user)):
    return get_jobs_by_user(user.get('sub'))

@app.get("/api/jobs/{job_id}")
def get_job_detail(job_id: str, user=Depends(get_current_user)):
    job = get_job(job_id)
    if job.get('userId') != user.get('sub'):
        raise HTTPException(403)
    return job

@app.get("/api/jobs/{job_id}/download")
def download_parquet(job_id: str, user=Depends(get_current_user)):
    job = get_job(job_id)
    if job.get('userId') != user.get('sub'):
        raise HTTPException(403)
    metadata = job.get('metadata', {})
    output_path = metadata.get('output_path')
    if not output_path:
        raise HTTPException(404, "No Parquet")
    
    parts = output_path.replace('s3://', '').split('/', 1)
    bucket = parts[0]
    key = parts[1]
    
    url = s3.generate_presigned_url('get_object', Params={'Bucket': bucket, 'Key': key}, ExpiresIn=3600)
    return {"download_url": url}

@app.get("/api/jobs/{job_id}/preview")
async def preview_parquet(job_id: str, user=Depends(get_current_user)):
    """Preview first 100 rows of Parquet file"""

    job = get_job(job_id)

    if job.get('userId') != user.get('sub'):
        raise HTTPException(403, "Not authorized")

    if job.get('status') != 'COMPLETED':
        raise HTTPException(400, "Job not completed yet")

    metadata = job.get('metadata', {})
    output_path = metadata.get('output_path')

    if not output_path:
        raise HTTPException(404, "Parquet file not found")

    # Parse S3 path: s3://bucket/key
    s3_path = output_path.replace('s3://', '')
    parts = s3_path.split('/', 1)
    bucket = parts[0]
    key = parts[1]

    print(f"Reading Parquet from s3://{bucket}/{key}")

    try:
        # Read from S3
        response = s3.get_object(Bucket=bucket, Key=key)
        parquet_bytes = response['Body'].read()

        # Parse Parquet
        df = pd.read_parquet(BytesIO(parquet_bytes))

        # Preview first 100 rows
        preview_df = df.head(100)

        # Convert to JSON-serializable format
        data = []
        for _, row in preview_df.iterrows():
            data.append([str(val) if pd.notna(val) else None for val in row])

        return {
            "columns": preview_df.columns.tolist(),
            "data": data,
            "preview_rows": len(data),
            "total_rows": len(df),
            "job_id": job_id,
            "filename": job.get('filename')
        }

    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(500, f"Failed to preview: {str(e)}")