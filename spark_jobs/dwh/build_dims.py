from pyspark.sql import functions as F
from common.ch_spark_utils import get_spark, get_client

spark = get_spark()
client_ods = get_client("ods")
client_dwh = get_client("dwh")

cust_data = client_ods.query("SELECT * FROM customers")
customers = spark.createDataFrame(cust_data.result_rows, schema=cust_data.column_names)

dim_customer = customers.select(
    "customer_id",
    "customer_unique_id",
    "customer_city",
    "customer_state",
    "customer_zip_code_prefix",
    "lat",
    "lng"
)

client_dwh.command("TRUNCATE TABLE IF EXISTS dwh.dim_customer")
client_dwh.insert_df("dim_customer", dim_customer.toPandas())

orders_data = client_ods.query("SELECT order_purchase_timestamp FROM orders WHERE order_purchase_timestamp IS NOT NULL")
orders = spark.createDataFrame(orders_data.result_rows, schema=orders_data.column_names)

dim_date = (
    orders
    .withColumn("order_purchase_timestamp", F.to_timestamp("order_purchase_timestamp"))
    .withColumn("full_date", F.to_date("order_purchase_timestamp"))
    .select("full_date")
    .distinct()
    .withColumn("date_id",     F.date_format("full_date", "yyyy-MM-dd"))
    .withColumn("year",        F.year("full_date").cast("int"))
    .withColumn("quarter",     F.quarter("full_date").cast("int"))
    .withColumn("month",       F.month("full_date").cast("int"))
    .withColumn("month_name",  F.date_format("full_date", "MMMM"))
    .withColumn("day",         F.dayofmonth("full_date").cast("int"))
    .withColumn("day_of_week", F.dayofweek("full_date").cast("int"))
    .withColumn("day_name",    F.date_format("full_date", "EEEE"))
)

client_dwh.command("TRUNCATE TABLE IF EXISTS dwh.dim_date")
client_dwh.insert_df("dim_date", dim_date.toPandas())

pay_data = client_ods.query("SELECT DISTINCT payment_type FROM order_payments WHERE payment_type IS NOT NULL")
payments = spark.createDataFrame(pay_data.result_rows, schema=pay_data.column_names)

dim_payment_method = (
    payments
    .withColumn(
        "is_installment_type",
        F.when(F.col("payment_type") == "credit_card", 1).otherwise(0)
    )
    .withColumn("payment_method_id", F.monotonically_increasing_id().cast("int"))
    .select("payment_method_id", "payment_type", "is_installment_type")
)

client_dwh.command("TRUNCATE TABLE IF EXISTS dwh.dim_payment_method")
client_dwh.insert_df("dim_payment_method", dim_payment_method.toPandas())

spark.stop()