"""
S3 I/O utilities for the ETL pipeline.

Handles reading raw CSV uploads from the source bucket and writing
processed Parquet files to the data-lake bucket.
"""

import boto3
import pandas as pd
from io import BytesIO, StringIO
from datetime import datetime

from config import DATALAKE_BUCKET

s3_client = boto3.client("s3")

# Delimiters tried (in order) when auto-detecting the CSV format.
_CANDIDATE_DELIMITERS = [",", ";", ":", "\t", "|"]


def read_csv_from_s3(bucket: str, key: str) -> pd.DataFrame:
    """
    Download a CSV object from S3 and return it as a DataFrame.

    The function attempts multiple common delimiters and returns the
    first parse that yields more than one column.

    Raises
    ------
    ValueError
        If the file cannot be parsed with any candidate delimiter.
    """
    response = s3_client.get_object(Bucket=bucket, Key=key)
    csv_content = response["Body"].read().decode("utf-8")

    for delimiter in _CANDIDATE_DELIMITERS:
        try:
            df = pd.read_csv(StringIO(csv_content), delimiter=delimiter)
            if len(df.columns) > 1:
                return df
        except Exception:
            continue

    raise ValueError(
        f"Could not parse CSV from s3://{bucket}/{key} "
        f"with any of the candidate delimiters: {_CANDIDATE_DELIMITERS}"
    )


def save_to_datalake(
    df: pd.DataFrame,
    user_id: str,
    schema_id: str,
    job_id: str,
) -> str:
    """
    Write *df* to the data-lake bucket as a Parquet file.

    The object key follows a Hive-style partition layout::

        processed/user=<uid>/schema=<sid>/date=<YYYY-MM-DD>/<job_id>.parquet

    Returns
    -------
    str
        Full ``s3://`` URI of the written object.
    """
    date_partition = datetime.utcnow().strftime("%Y-%m-%d")
    output_key = (
        f"processed/user={user_id}/schema={schema_id}"
        f"/date={date_partition}/{job_id}.parquet"
    )

    parquet_buffer = BytesIO()
    df.to_parquet(parquet_buffer, engine="pyarrow", index=False)

    s3_client.put_object(
        Bucket=DATALAKE_BUCKET,
        Key=output_key,
        Body=parquet_buffer.getvalue(),
        ContentType="application/octet-stream",
    )

    return f"s3://{DATALAKE_BUCKET}/{output_key}"
