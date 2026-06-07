from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.empty import EmptyOperator

from datetime import datetime

with DAG(
    dag_id="groceria_pipeline",
    start_date=datetime(2025,1,1),
    schedule=None,
    catchup=False
) as dag:

    start = EmptyOperator(
        task_id="start"
    )

    ingest_orders = SparkSubmitOperator(
        task_id="ingest_orders",
        application="/opt/airflow/spark_jobs/ingest/load_csv_to_raw.py",
        application_args=[
            "/datasets/orders.csv",
            "raw.orders"
        ]
    )

    ingest_customers = SparkSubmitOperator(
        task_id="ingest_customers",
        application="/opt/airflow/spark_jobs/ingest/load_csv_to_raw.py",
        application_args=[
            "/datasets/customers.csv",
            "raw.customers"
        ]
    )

    ingest_geolocation = SparkSubmitOperator(
        task_id="ingest_geolocation",
        application="/opt/airflow/spark_jobs/ingest/load_csv_to_raw.py",
        application_args=[
            "/datasets/geolocation.csv",
            "raw.geolocation"
        ]
    )

    ingest_payments = SparkSubmitOperator(
        task_id="ingest_payments",
        application="/opt/airflow/spark_jobs/ingest/load_csv_to_raw.py",
        application_args=[
            "/datasets/order_payments.csv",
            "raw.order_payments"
        ]
    )

    ingest_products = SparkSubmitOperator(
        task_id="ingest_products",
        application="/opt/airflow/spark_jobs/ingest/load_csv_to_raw.py",
        application_args=[
            "/datasets/products.csv",
            "raw.products"
        ]
    )

    ingest_reviews = SparkSubmitOperator(
        task_id="ingest_reviews",
        application="/opt/airflow/spark_jobs/ingest/load_csv_to_raw.py",
        application_args=[
            "/datasets/order_reviews.csv",
            "raw.order_reviews"
        ]
    )

    ingest_items = SparkSubmitOperator(
        task_id="ingest_items",
        application="/opt/airflow/spark_jobs/ingest/load_csv_to_raw.py",
        application_args=[
            "/datasets/order_items.csv",
            "raw.order_items"
        ]
    )

    ingest_sellers = SparkSubmitOperator(
        task_id="ingest_sellers",
        application="/opt/airflow/spark_jobs/ingest/load_csv_to_raw.py",
        application_args=[
            "/datasets/sellers.csv",
            "raw.sellers"
        ]
    )

    ingest_translation = SparkSubmitOperator(
        task_id="ingest_translation",
        application="/opt/airflow/spark_jobs/ingest/load_csv_to_raw.py",
        application_args=[
            "/datasets/product_category_translation.csv",
            "raw.product_category_translation"
        ]
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