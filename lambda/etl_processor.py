"""
ETL data transformation pipeline.

Applies a sequence of cleaning and standardization operations
to a pandas DataFrame:
  1. Drop fully-empty rows
  2. Remove duplicate records
  3. Impute missing values (numeric → median, categorical → mode/fill)
  4. Remove statistical outliers via the IQR method
  5. Strip whitespace from text columns
  6. Append processing metadata columns
"""

import numpy as np
import pandas as pd
from datetime import datetime

from config import (
    OUTLIER_IQR_MULTIPLIER,
    NUMERIC_FILL_STRATEGY,
    CATEGORICAL_FILL_VALUE,
)


def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply the full cleaning pipeline to *df* and return the result.

    The original DataFrame is **not** mutated — a copy is made first.
    """
    df_clean = df.copy()

    df_clean = _drop_empty_rows(df_clean)
    df_clean = _drop_duplicates(df_clean)
    df_clean = _impute_missing_values(df_clean)
    df_clean = _remove_outliers(df_clean)
    df_clean = _strip_text_columns(df_clean)
    df_clean = _append_metadata(df_clean)

    return df_clean


# ------------------------------------------------------------------
# Private helpers
# ------------------------------------------------------------------

def _drop_empty_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Remove rows where every column is NaN."""
    return df.dropna(how="all")


def _drop_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """Remove exact-duplicate rows."""
    return df.drop_duplicates()


def _impute_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Fill missing values by column type:
      - Numeric  → median (default) or mean
      - Object   → configurable fill value
      - Datetime → forward-fill
    """
    for column in df.columns:
        dtype = df[column].dtype

        if pd.api.types.is_numeric_dtype(dtype):
            if df[column].isnull().any():
                fill = (
                    df[column].median()
                    if NUMERIC_FILL_STRATEGY == "median"
                    else df[column].mean()
                )
                df[column] = df[column].fillna(fill)

        elif pd.api.types.is_object_dtype(dtype):
            df[column] = df[column].fillna(CATEGORICAL_FILL_VALUE)

        elif pd.api.types.is_datetime64_any_dtype(dtype):
            if df[column].isnull().any():
                df[column] = df[column].ffill()

    return df


def _remove_outliers(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove rows with values outside ``Q1 - k*IQR .. Q3 + k*IQR``
    for every numeric column (k = OUTLIER_IQR_MULTIPLIER).
    """
    k = OUTLIER_IQR_MULTIPLIER

    for column in df.select_dtypes(include=[np.number]).columns:
        q1 = df[column].quantile(0.25)
        q3 = df[column].quantile(0.75)
        iqr = q3 - q1

        lower = q1 - k * iqr
        upper = q3 + k * iqr

        df = df[(df[column] >= lower) & (df[column] <= upper)]

    return df


def _strip_text_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Strip leading/trailing whitespace from all object columns."""
    for column in df.select_dtypes(include=["object"]).columns:
        df[column] = df[column].str.strip()
    return df


def _append_metadata(df: pd.DataFrame) -> pd.DataFrame:
    """Add audit columns indicating when and how the data was processed."""
    df["_processed_at"] = datetime.utcnow().isoformat()
    df["_source_file"] = "uploaded"
    return df
