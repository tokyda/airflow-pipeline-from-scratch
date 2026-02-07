import pandas as pd
from pathlib import Path

# Number of synthetic customer groups used for joining
N_GROUPS = 200


def build_customer_metrics():
    """
    Builds customer-level analytics metrics by combining cleaned datasets.

    Since the retail and IMDb datasets do not share a real customer ID,
    we create a synthetic 'customer_group' key in both datasets to
    demonstrate an end-to-end join → metrics → dashboard pipeline.
    """

    # ---------------- Paths ----------------
    retail_path = Path("/opt/airflow/data/clean/online_retail_clean.csv")
    reviews_path = Path("/opt/airflow/data/clean/imdb_reviews_clean.csv")
    output_path = Path("/opt/airflow/data/analytics/customer_metrics.csv")

    # ---------------- Load data ----------------
    retail_df = pd.read_csv(retail_path)
    reviews_df = pd.read_csv(reviews_path)

    # ---------------- Create synthetic join key ----------------
    # Retail: group customers into deterministic buckets
    retail_df["customerid"] = retail_df["customerid"].astype(float).astype(int)
    retail_df["customer_group"] = retail_df["customerid"] % N_GROUPS

    # Reviews: assign each review to a customer group
    reviews_df["customer_group"] = reviews_df.index % N_GROUPS

    # ---------------- Retail metrics ----------------
    # Create line-level spend if not already present
    if "line_total" not in retail_df.columns:
        retail_df["line_total"] = retail_df["quantity"] * retail_df["unitprice"]

    retail_metrics = (
        retail_df
        .groupby("customer_group")
        .agg(
            total_spent=("line_total", "sum"),
            num_transactions=("invoiceno", "nunique")
        )
        .reset_index()
    )

    # ---------------- Review metrics ----------------
    review_metrics = (
        reviews_df
        .groupby("customer_group")
        .agg(
            num_positive_reviews=("sentiment", lambda x: (x == "positive").sum())
        )
        .reset_index()
    )

    # ---------------- Join datasets ----------------
    final_df = (
        retail_metrics
        .merge(review_metrics, on="customer_group", how="left")
        .fillna(0)
    )

    # ---------------- Save output ----------------
    final_df.to_csv(output_path, index=False)


if __name__ == "__main__":
    build_customer_metrics()

