"""
JWT authentication middleware for AWS Cognito.

Validates Bearer tokens from the ``Authorization`` header against
the Cognito User Pool's JWKS endpoint and returns the decoded
token payload (claims) to downstream route handlers.
"""

import logging
from typing import Dict, Any

import requests
from jose import jwt, JWTError
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

from config import COGNITO_JWKS_URL, COGNITO_CLIENT_ID, COGNITO_ISSUER

logger = logging.getLogger(__name__)
security = HTTPBearer()

# Cache JWKS keys at module level so they are fetched once per
# cold-start rather than on every request.
_jwks_cache: Dict[str, Any] = {}


def _get_jwks() -> Dict[str, Any]:
    """Return cached JWKS; fetch from Cognito on first call."""
    global _jwks_cache
    if not _jwks_cache:
        logger.info("Fetching JWKS from %s", COGNITO_JWKS_URL)
        _jwks_cache = requests.get(COGNITO_JWKS_URL, timeout=5).json()
    return _jwks_cache


def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security),
) -> Dict[str, Any]:
    """
    FastAPI dependency that extracts and validates a Cognito JWT.

    Returns
    -------
    dict
        The decoded JWT payload (``sub``, ``email``, etc.).

    Raises
    ------
    HTTPException 401
        If the token is missing, expired, or otherwise invalid.
    """
    token = credentials.credentials

    try:
        headers = jwt.get_unverified_header(token)
        kid = headers["kid"]

        jwks = _get_jwks()
        key = next((k for k in jwks["keys"] if k["kid"] == kid), None)

        if key is None:
            raise JWTError("No matching signing key found in JWKS")

        payload = jwt.decode(
            token=token,
            key=key,
            algorithms=["RS256"],
            audience=COGNITO_CLIENT_ID,
            issuer=COGNITO_ISSUER,
        )
        return payload

    except JWTError:
        logger.warning("JWT validation failed", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired authentication token",
        )