from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from utils.dq_checks import run_all_checks
from datetime import datetime

SPARK_JOBS = "/opt/airflow/spark_jobs"
LOAD_CSV   = f"{SPARK_JOBS}/ingest/load_csv_to_raw.py"
DATASETS   = "/datasets"
TRANSFORM = f"{SPARK_JOBS}/transform"

with DAG(
    dag_id="groceria_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False
) as dag:

    start = EmptyOperator(task_id="start")

    ingest_orders = BashOperator(
        task_id="ingest_orders",
        bash_command=f"python {LOAD_CSV} {DATASETS}/orders.csv raw.orders"
    )

    ingest_customers = BashOperator(
        task_id="ingest_customers",
        bash_command=f"python {LOAD_CSV} {DATASETS}/customers.csv raw.customers"
    )
    ingest_geolocation = BashOperator(
        task_id="ingest_geolocation",
        bash_command=f"python {LOAD_CSV} {DATASETS}/geolocation.csv raw.geolocation"
    )
    ingest_payments = BashOperator(
        task_id="ingest_payments",
        bash_command=f"python {LOAD_CSV} {DATASETS}/order_payments.csv raw.order_payments"
    )
    ingest_products = BashOperator(
        task_id="ingest_products",
        bash_command=f"python {LOAD_CSV} {DATASETS}/products.csv raw.products"
    )
    ingest_reviews = BashOperator(
        task_id="ingest_reviews",
        bash_command=f"python {LOAD_CSV} {DATASETS}/order_reviews.csv raw.order_reviews"
    )
    ingest_items = BashOperator(
        task_id="ingest_items",
        bash_command=f"python {LOAD_CSV} {DATASETS}/order_items.csv raw.order_items"
    )
    ingest_sellers = BashOperator(
        task_id="ingest_sellers",
        bash_command=f"python {LOAD_CSV} {DATASETS}/sellers.csv raw.sellers"
    )
    ingest_translation = BashOperator(
        task_id="ingest_translation",
        bash_command=f"python {LOAD_CSV} {DATASETS}/category_translation.csv raw.product_category_translation"
    )


    run_dq_checks = PythonOperator(
        task_id="run_dq_checks",
        python_callable=run_all_checks,
        op_args=["raw"]
    )

    transform_orders = BashOperator(
        task_id="transform_orders",
        bash_command=f"spark-submit {TRANSFORM}/transform_orders.py"
    )

    transform_payments = BashOperator(
        task_id="transform_payments",
        bash_command=f"spark-submit {TRANSFORM}/transform_payments.py"
    )

    transform_items = BashOperator(
        task_id="transform_items",
        bash_command=f"spark-submit {TRANSFORM}/transform_items.py"
    )

    transform_customers = BashOperator(
        task_id="transform_customers",
        bash_command=f"spark-submit {TRANSFORM}/transform_customers.py"
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
    ] >> run_dq_checks >> [
        transform_orders,
        transform_payments,
        transform_items,
        transform_customers,
    ] >> end