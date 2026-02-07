import pandas as pd
from pathlib import Path


def clean_online_retail():
    """
    Cleans Online Retail transactional data and writes a clean version.
    """

    raw_path = Path("/opt/airflow/data/raw/online_retail_extract.csv")
    clean_path = Path("/opt/airflow/data/clean/online_retail_clean.csv")

    df = pd.read_csv(raw_path)

    # Standardise column names
    df.columns = [c.lower() for c in df.columns]

    # Drop rows without customer id
    df = df.dropna(subset=["customerid"])

    # Enforce positive quantities and prices
    df = df[df["quantity"] > 0]
    df = df[df["unitprice"] > 0]

    # Write cleaned data
    df.to_csv(clean_path, index=False)


if __name__ == "__main__":
    clean_online_retail()
