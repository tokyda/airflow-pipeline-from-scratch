import pandas as pd
from pathlib import Path


def build_customer_metrics():
    """
    Builds customer-level analytics metrics by combining cleaned datasets.
    """

    retail_path = Path("/opt/airflow/data/clean/online_retail_clean.csv")
    reviews_path = Path("/opt/airflow/data/clean/imdb_reviews_clean.csv")
    output_path = Path("/opt/airflow/data/analytics/customer_metrics.csv")

    retail_df = pd.read_csv(retail_path)
    reviews_df = pd.read_csv(reviews_path)

    # --- Retail metrics ---
    retail_metrics = (
        retail_df
        .groupby("customerid")
        .agg(
            total_spent=("unitprice", "sum"),
            num_transactions=("invoiceno", "nunique")
        )
        .reset_index()
    )

    # Ensure join key has consistent type
    retail_metrics["customerid"] = retail_metrics["customerid"].astype(str)


    # --- Review metrics (synthetic user ids) ---
    reviews_df["customerid"] = reviews_df.index % retail_metrics.shape[0]

    review_metrics = (
        reviews_df
        .groupby("customerid")
        .agg(
            num_positive_reviews=("sentiment", lambda x: (x == "positive").sum())
        )
        .reset_index()
    )

    review_metrics["customerid"] = review_metrics["customerid"].astype(str)


    # --- Join ---
    final_df = retail_metrics.merge(
        review_metrics,
        on="customerid",
        how="left"
    ).fillna(0)

    final_df.to_csv(output_path, index=False)


if __name__ == "__main__":
    build_customer_metrics()
