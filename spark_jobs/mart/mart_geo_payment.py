from pyspark.sql import functions as F
from common.ch_spark_utils import get_spark, get_client

spark = get_spark()
client_dwh  = get_client("dwh")
client_mart = get_client("mart")

pay_data  = client_dwh.query("SELECT * FROM fact_payments")
payments  = spark.createDataFrame(pay_data.result_rows, schema=pay_data.column_names)

cust_data = client_dwh.query("SELECT customer_id, customer_unique_id FROM dim_customer")
customers = spark.createDataFrame(cust_data.result_rows, schema=cust_data.column_names)

payments_with_uid = payments.join(customers, on="customer_id", how="left")

customer_spend = (
    payments_with_uid
    .groupBy("customer_unique_id")
    .agg(F.sum("payment_value").alias("total_payment_value"))
)

threshold = (
    customer_spend
    .select(F.expr("percentile_approx(total_payment_value, 0.75)").alias("p75"))
    .collect()[0]["p75"]
)

hvc_labels = customer_spend.withColumn(
    "is_hvc",
    F.when(F.col("total_payment_value") >= threshold, 1).otherwise(0)
)

payments_labeled = payments_with_uid.join(
    hvc_labels.select("customer_unique_id", "is_hvc"),
    on="customer_unique_id",
    how="left"
)

base = (
    payments_labeled
    .groupBy("customer_state", "payment_type")
    .agg(
        F.count("order_id").alias("total_transactions"),
        F.sum("payment_value").alias("total_payment_value"),
        F.avg("payment_value").alias("avg_payment_value"),
        F.avg("lat").alias("avg_lat"),
        F.avg("lng").alias("avg_lng"),
    )
)

hvc_counts = (
    payments_labeled
    .filter(F.col("is_hvc") == 1)
    .groupBy("customer_state", "payment_type")
    .agg(F.countDistinct("customer_unique_id").alias("high_value_customer_count"))
)

total_counts = (
    payments_labeled
    .groupBy("customer_state", "payment_type")
    .agg(F.countDistinct("customer_unique_id").alias("total_customer_count"))
)

df = (
    base
    .join(hvc_counts,   on=["customer_state", "payment_type"], how="left")
    .join(total_counts, on=["customer_state", "payment_type"], how="left")
    .fillna(0, subset=["high_value_customer_count"])
    .withColumn(
        "pct_high_value",
        F.round(F.col("high_value_customer_count") / F.col("total_customer_count") * 100, 2)
    )
    .select(
        "customer_state",
        "payment_type",
        "total_transactions",
        "total_payment_value",
        "avg_payment_value",
        "high_value_customer_count",
        "pct_high_value",
        "avg_lat",
        "avg_lng",
    )
)

client_mart.command("TRUNCATE TABLE IF EXISTS mart.geo_payment")
client_mart.insert_df("geo_payment", df.toPandas())
spark.stop()