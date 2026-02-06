import pandas as pd
from pathlib import Path


def clean_imdb_reviews():
    """
    Cleans raw IMDb review data and writes a cleaned version.
    """

    raw_path = Path("/opt/airflow/data/raw/imdb_reviews.csv")
    clean_path = Path("/opt/airflow/data/clean/imdb_reviews_clean.csv")

    # Read raw data
    df = pd.read_csv(raw_path)

    # Rename columns for clarity
    df = df.rename(columns={"review": "review_text"})

    # Standardise sentiment values
    df["sentiment"] = df["sentiment"].str.lower()

    # Basic validation
    assert set(df["sentiment"].unique()).issubset({"positive", "negative"})

    # Write cleaned output
    df.to_csv(clean_path, index=False)


if __name__ == "__main__":
    clean_imdb_reviews()
