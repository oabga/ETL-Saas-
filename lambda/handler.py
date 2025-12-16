import json
import boto3
import pandas as pd
import numpy as np
from io import StringIO, BytesIO
from datetime import datetime
import hashlib
import uuid

s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

JOBS_TABLE = 'JobsTable'
SCHEMA_TABLE = 'SchemaTable'
DATALAKE_BUCKET = 'data-lake-bucket-processed'

def lambda_handler(event, context):
    try:
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = event['Records'][0]['s3']['object']['key']
        
        # Extract job_id từ S3 key: uploads/user/jobid_filename.csv
        parts = key.split('/')
        user_id = parts[1] if len(parts) > 1 else 'anonymous'
        
        # Extract job_id từ filename (không tạo mới)
        filename_part = parts[-1] if len(parts) > 0 else ''
        job_id = filename_part.split('_')[0] if '_' in filename_part else str(uuid.uuid4())
        
        # Extract original filename
        original_filename = '_'.join(filename_part.split('_')[1:]) if '_' in filename_part else filename_part
        
        print(f"Processing job: {job_id}, user: {user_id}, file: {original_filename}")

        # Get existing job data to preserve filename and created_at
        existing_job = get_existing_job(job_id)
        
        update_job_status(job_id, user_id, 'PROCESSING', {
            'source_bucket': bucket,
            'source_key': key,
            'started_at': datetime.utcnow().isoformat()
        }, existing_job)

        df = read_csv_from_s3(bucket, key)
        original_rows = len(df)
        print(f"Original rows: {original_rows}")

        # df = transform_data(df)

        print(df)

        schema_id = detect_and_register_schema(df, user_id, original_filename)
        print(f"Schema ID: {schema_id}")

        output_path = save_to_datalake(df, user_id, schema_id, job_id)
        print(f"Output path: {output_path}")

        # Update COMPLETED with preserved data
        update_job_status(job_id, user_id, 'COMPLETED', {
            'source_bucket': bucket,
            'source_key': key,
            'output_path': output_path,
            'ended_at': datetime.utcnow().isoformat()
        }, existing_job)

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Success',
                'job_id': job_id,
                'output_path': output_path
            })
        }

    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()
        
        if 'job_id' in locals() and 'user_id' in locals():
            existing_job = get_existing_job(job_id) if 'existing_job' not in locals() else existing_job
            update_job_status(job_id, user_id, 'FAILED', {
                'error': str(e),
                'failed_at': datetime.utcnow().isoformat()
            }, existing_job)
        
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }


def get_existing_job(job_id):
    """Get existing job data from DynamoDB"""
    try:
        jobs_table = dynamodb.Table(JOBS_TABLE)
        response = jobs_table.get_item(Key={'jobId': job_id})
        return response.get('Item', {})
    except Exception as e:
        print(f"Error getting existing job: {e}")
        return {}


def update_job_status(job_id, user_id, status, metadata, existing_job=None):
    """Update job status in DynamoDB while preserving original data"""
    jobs_table = dynamodb.Table(JOBS_TABLE)
    
    if existing_job is None:
        existing_job = {}

    
    item = {
        'jobId': job_id,
        'userId': user_id,
        'status': status,
        'timestamp': int(datetime.utcnow().timestamp()),
        'metadata': metadata,
        'filename': existing_job.get('filename'),
        's3_key': existing_job.get('s3_key'),
        'created_at': existing_job.get('created_at')
    }

    jobs_table.put_item(Item=item)
    print(f"Job {job_id} status updated: {status}")


def read_csv_from_s3(bucket, key):
    """Read CSV from S3 and return DataFrame"""
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        csv_content = response['Body'].read().decode('utf-8')

        for delimiter in [',', ', ', ':', '\t', '|']:
            try:
                df = pd.read_csv(StringIO(csv_content), delimiter=delimiter)
                if len(df.columns) > 1:
                    return df
            except:
                continue
        
        raise ValueError(f"Could not parse CSV with any common delimiter")
    except Exception as e:
        raise Exception(f"Error reading CSV: {str(e)}")
    
def detect_and_register_schema(df, user_id, filename):
    """ 
    Detect schema and register in DynamoDB
    Returns schema_id for tracking
    """
    # Extract schema information
    schema_info = {
        'columns': list(df.columns),
        'dtypes': {col: str(dtype) for col, dtype in df.dtypes.items()},
    }

    schema_fingerprint = hashlib.md5(
        json.dumps(sorted(df.columns.tolist())).encode()
    ).hexdigest()

    schema_id = f"schema_{schema_fingerprint}"

    # Save to DynamoDB
    schema_table = dynamodb.Table(SCHEMA_TABLE)

    try:
        response = schema_table.get_item(Key={'schemaId': schema_id})
        if 'Item' in response:
            print(f"Schema {schema_id} already exists")
        else:
            # Register new schema
            schema_table.put_item(Item={
                'schemaId': schema_id,
                'userId': user_id,
                'schemaInfo': schema_info,
                'filename': filename,
                'created_at': datetime.utcnow().isoformat()
            })
            print(f"New schema registered: {schema_id}")
    except Exception as e:
        print(f"Error registering schema: {e}")
    
    return schema_id
    
def transform_data(df):
    """
    Apply transformations to clean the data
    """
    df_clean = df.copy()
    
    
    df_clean = df_clean.dropna(how='all')
    
    
    df_clean = df_clean.drop_duplicates()
    
    
    for column in df_clean.columns:
        dtype = df_clean[column].dtype
        
        if pd.api.types.is_numeric_dtype(dtype):
           
            if df_clean[column].isnull().any():
                median_val = df_clean[column].median()
                df_clean[column].fillna(median_val, inplace=True)
        
        elif pd.api.types.is_object_dtype(dtype):
            
            df_clean[column].fillna('Unknown', inplace=True)
        
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            
            if df_clean[column].isnull().any():
                df_clean[column].fillna(method='ffill', inplace=True)
    
    # 4. Remove outliers for numeric columns
    for column in df_clean.select_dtypes(include=[np.number]).columns:
        Q1 = df_clean[column].quantile(0.25)
        Q3 = df_clean[column].quantile(0.75)
        IQR = Q3 - Q1
        
       
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR
        
        df_clean = df_clean[
            (df_clean[column] >= lower_bound) & 
            (df_clean[column] <= upper_bound)
        ]
    
   
    for column in df_clean.select_dtypes(include=['object']).columns:
        df_clean[column] = df_clean[column].str.strip()
    
    
    df_clean['_processed_at'] = datetime.utcnow().isoformat()
    df_clean['_source_file'] = 'uploaded'
    
    return df_clean

def save_to_datalake(df, user_id, schema_id, job_id):
    """
    Save DataFrame to Data Lake as Parquet
    Organized by user and schema for easy querying
    """
    
    date_partition = datetime.utcnow().strftime('%Y-%m-%d')
    
    output_key = f"processed/user={user_id}/schema={schema_id}/date={date_partition}/{job_id}.parquet"
    
   
    parquet_buffer = BytesIO()
    df.to_parquet(parquet_buffer, engine='pyarrow', index=False)
   
    # Upload to S3
    s3.put_object(
        Bucket=DATALAKE_BUCKET,
        Key=output_key,
        Body=parquet_buffer.getvalue(),
        ContentType='application/octet-stream'
    )
    
    return f"s3://{DATALAKE_BUCKET}/{output_key}"