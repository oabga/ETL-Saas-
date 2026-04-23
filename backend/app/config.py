"""
Application settings loaded from environment variables.

Uses Pydantic BaseSettings for validation and type-coercion.
In production the values come from the container environment;
for local development, create a ``.env`` file next to this module.
"""

import os


# --- AWS Region ---
AWS_REGION: str = os.environ.get("AWS_REGION", "ap-southeast-1")

# --- S3 Buckets ---
UPLOAD_BUCKET: str = os.environ.get("UPLOAD_BUCKET", "source-bucket-oabga")
DATALAKE_BUCKET: str = os.environ.get("DATALAKE_BUCKET", "data-lake-bucket-processed")

# --- DynamoDB ---
JOBS_TABLE: str = os.environ.get("JOBS_TABLE", "JobsTable")

# --- Cognito ---
COGNITO_REGION: str = os.environ.get("COGNITO_REGION", "ap-southeast-1")
COGNITO_USER_POOL_ID: str = os.environ.get("COGNITO_USER_POOL_ID", "ap-southeast-1_LZU2SXqyz")
COGNITO_CLIENT_ID: str = os.environ.get("COGNITO_CLIENT_ID", "6v29eis60gqjicijhirvp22of")
COGNITO_ISSUER: str = os.environ.get(
    "COGNITO_ISSUER",
    f"https://cognito-idp.{COGNITO_REGION}.amazonaws.com/{COGNITO_USER_POOL_ID}",
)
COGNITO_JWKS_URL: str = os.environ.get(
    "COGNITO_JWKS_URL",
    f"{COGNITO_ISSUER}/.well-known/jwks.json",
)

# --- CORS ---
CORS_ORIGINS: list = os.environ.get("CORS_ORIGINS", "*").split(",")
