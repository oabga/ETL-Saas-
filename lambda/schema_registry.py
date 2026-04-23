"""
Schema registry backed by DynamoDB.

Each unique combination of column names produces a deterministic
``schema_<md5>`` identifier.  The registry stores column metadata
so downstream consumers can understand the shape of ingested data.
"""

import hashlib
import json
import logging

import boto3
import pandas as pd
from datetime import datetime

from config import SCHEMA_TABLE

logger = logging.getLogger(__name__)
dynamodb = boto3.resource("dynamodb")


def detect_and_register_schema(
    df: pd.DataFrame,
    user_id: str,
    filename: str,
) -> str:
    """
    Compute a fingerprint for *df*'s column set, register it in
    DynamoDB if it does not already exist, and return the schema ID.

    Parameters
    ----------
    df : pd.DataFrame
        The DataFrame whose schema should be registered.
    user_id : str
        Owner of the upload.
    filename : str
        Original filename of the uploaded CSV.

    Returns
    -------
    str
        A deterministic schema ID of the form ``schema_<hex>``.
    """
    schema_info = {
        "columns": list(df.columns),
        "dtypes": {col: str(dtype) for col, dtype in df.dtypes.items()},
    }

    fingerprint = hashlib.md5(
        json.dumps(sorted(df.columns.tolist())).encode()
    ).hexdigest()

    schema_id = f"schema_{fingerprint}"

    table = dynamodb.Table(SCHEMA_TABLE)

    try:
        response = table.get_item(Key={"schemaId": schema_id})
        if "Item" in response:
            logger.info("Schema %s already exists — skipping registration", schema_id)
        else:
            table.put_item(
                Item={
                    "schemaId": schema_id,
                    "userId": user_id,
                    "schemaInfo": schema_info,
                    "filename": filename,
                    "created_at": datetime.utcnow().isoformat(),
                }
            )
            logger.info("Registered new schema: %s", schema_id)
    except Exception:
        logger.exception("Failed to register schema %s", schema_id)

    return schema_id
