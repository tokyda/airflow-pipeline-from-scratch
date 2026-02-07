import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

import matplotlib
matplotlib.use("Agg")



def generate_dashboard():
    """
    Generates a simple analytics dashboard from customer metrics.
    """

    input_path = Path("/opt/airflow/data/analytics/customer_metrics.csv")
    output_path = Path("/opt/airflow/dashboards/customer_metrics_dashboard.html")

    df = pd.read_csv(input_path)

    # --- Simple plots ---
    fig, axes = plt.subplots(1, 2, figsize=(12, 4))

    df["total_spent"].hist(bins=30, ax=axes[0])
    axes[0].set_title("Distribution of Total Spend")

    df["num_positive_reviews"].hist(bins=20, ax=axes[1])
    axes[1].set_title("Distribution of Positive Reviews")

    plt.tight_layout()

    # Save figure
    image_path = Path("/opt/airflow/dashboards/metrics.png")
    plt.savefig(image_path)
    plt.close()

    # --- Simple HTML ---
    html = f"""
    <html>
        <head>
            <title>Customer Metrics Dashboard</title>
        </head>
        <body>
            <h1>Customer Metrics Dashboard</h1>
            <p>Generated from Airflow analytics pipeline.</p>

            <h2>Overview</h2>
            {df.head(20).to_html(index=False)}

            <h2>Distributions</h2>
            <img src="metrics.png" width="800"/>
        </body>
    </html>
    """

    with open(output_path, "w") as f:
        f.write(html)


if __name__ == "__main__":
    generate_dashboard()
