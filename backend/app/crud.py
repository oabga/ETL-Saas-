import boto3
from datetime import datetime

dynamodb = boto3.resource('dynamodb', region_name='ap-southeast-1')
JOBS_TABLE = 'JobsTable'

def create_job(job_id: str, user_id: str, filename: str, key: str):
    table = dynamodb.Table(JOBS_TABLE)
    table.put_item(Item={
        'jobId': job_id,
        'userId': user_id,
        'status': 'QUEUED',
        'filename': filename,
        's3_key': key,
        'created_at': datetime.utcnow().isoformat()
    })

def get_jobs_by_user(user_id: str):
    table = dynamodb.Table(JOBS_TABLE)
    response = table.scan(
        FilterExpression='userId = :uid', 
        ExpressionAttributeValues={':uid': user_id}
    )
    items = response.get('Items', [])
    
    
    normalized_items = []
    for item in items:
        normalized_items.append({
            'jobId': item.get('jobId'),
            'userId': item.get('userId'),
            'filename': item.get('filename'),
            'status': item.get('status'),
            's3Key': item.get('s3_key'),  
            'createdAt': item.get('created_at'), 
            'metadata': item.get('metadata', {})
        })
    
    return normalized_items

def get_job(job_id: str):
    table = dynamodb.Table(JOBS_TABLE)  
    response = table.get_item(Key={'jobId': job_id})
    item = response.get('Item', {})
    
    
    if item:
        return {
            'jobId': item.get('jobId'),
            'userId': item.get('userId'),
            'filename': item.get('filename'),
            'status': item.get('status'),
            's3Key': item.get('s3_key'),
            'createdAt': item.get('created_at'),
            'metadata': item.get('metadata', {})
        }
    return {}