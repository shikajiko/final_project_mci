from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

from datetime import datetime

with DAG(
    dag_id="groceria_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False
) as dag:

    start = EmptyOperator(
        task_id="start"
    )

    ingest_orders = BashOperator(
        task_id="ingest_orders",
        bash_command=(
            "python /opt/airflow/spark_jobs/ingest/load_csv_to_raw.py "
            "/datasets/orders.csv raw.orders"
        )
    )

    ingest_customers = BashOperator(
        task_id="ingest_customers",
        bash_command=(
            "python /opt/airflow/spark_jobs/ingest/load_csv_to_raw.py "
            "/datasets/customers.csv raw.customers"
        )
    )

    ingest_geolocation = BashOperator(
        task_id="ingest_geolocation",
        bash_command=(
            "python /opt/airflow/spark_jobs/ingest/load_csv_to_raw.py "
            "/datasets/geolocation.csv raw.geolocation"
        )
    )

    ingest_payments = BashOperator(
        task_id="ingest_payments",
        bash_command=(
            "python /opt/airflow/spark_jobs/ingest/load_csv_to_raw.py "
            "/datasets/order_payments.csv raw.order_payments"
        )
    )

    ingest_products = BashOperator(
        task_id="ingest_products",
        bash_command=(
            "python /opt/airflow/spark_jobs/ingest/load_csv_to_raw.py "
            "/datasets/products.csv raw.products"
        )
    )

    ingest_reviews = BashOperator(
        task_id="ingest_reviews",
        bash_command=(
            "python /opt/airflow/spark_jobs/ingest/load_csv_to_raw.py "
            "/datasets/order_reviews.csv raw.order_reviews"
        )
    )

    ingest_items = BashOperator(
        task_id="ingest_items",
        bash_command=(
            "python /opt/airflow/spark_jobs/ingest/load_csv_to_raw.py "
            "/datasets/order_items.csv raw.order_items"
        )
    )

    ingest_sellers = BashOperator(
        task_id="ingest_sellers",
        bash_command=(
            "python /opt/airflow/spark_jobs/ingest/load_csv_to_raw.py "
            "/datasets/sellers.csv raw.sellers"
        )
    )

    ingest_translation = BashOperator(
        task_id="ingest_translation",
        bash_command=(
            "python /opt/airflow/spark_jobs/ingest/load_csv_to_raw.py "
            "/datasets/product_category_translation.csv "
            "raw.product_category_translation"
        )
    )

    end = EmptyOperator(
        task_id="end"
    )

    start >> [
        ingest_orders,
        ingest_customers,
        ingest_geolocation,
        ingest_payments,
        ingest_products,
        ingest_reviews,
        ingest_items,
        ingest_sellers,
        ingest_translation
    ] >> end