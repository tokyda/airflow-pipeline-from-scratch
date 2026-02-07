import pandas as pd
from pathlib import Path


def clean_online_retail():
    """
    Minimal, safe cleaning for Online Retail dataset.

    Goal:
    - Preserve rows
    - Ensure CustomerID exists
    - Do NOT over-filter
    """

    raw_path = Path("/opt/airflow/data/raw/online_retail.csv")
    clean_path = Path("/opt/airflow/data/clean/online_retail_clean.csv")

    # Load raw data
    df = pd.read_csv(raw_path)

    # Normalize column names (CRITICAL)
    df.columns = df.columns.str.lower()

    # Keep only rows with a customerid
    # (Online Retail has many NaNs here — this is expected)
    df = df[df["customerid"].notna()]

    # Do NOT cast to int (will break floats like 17850.0)
    df["customerid"] = df["customerid"].astype(str)

    # Write cleaned output
    df.to_csv(clean_path, index=False)


if __name__ == "__main__":
    clean_online_retail()
