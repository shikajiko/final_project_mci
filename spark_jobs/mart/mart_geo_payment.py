from pyspark.sql import functions as F
from common.ch_spark_utils import get_spark, get_client

spark = get_spark()
client_dwh  = get_client("dwh")
client_mart = get_client("mart")

pay_data = client_dwh.query("SELECT * FROM fact_payments")
payments = spark.createDataFrame(pay_data.result_rows, schema=pay_data.column_names)

df = (
    payments
    .groupBy("customer_state", "payment_type")
    .agg(
        F.count("order_id").alias("total_transactions"),
        F.sum("payment_value").alias("total_payment_value"),
        F.avg("payment_value").alias("avg_payment_value"),
        F.sum("is_high_value_payment").alias("high_value_customer_count"),
        F.avg("lat").alias("avg_lat"),
        F.avg("lng").alias("avg_lng"),
    )
)

state_totals = (
    df
    .groupBy("customer_state")
    .agg(F.sum("total_transactions").alias("state_total"))
)

df = (
    df
    .join(state_totals, on="customer_state", how="left")
    .withColumn(
        "pct_high_value",
        F.col("high_value_customer_count") / F.col("state_total") * 100
    )
    .drop("state_total")
)

client_mart.command("TRUNCATE TABLE IF EXISTS mart.geo_payment")
client_mart.insert_df("geo_payment", df.toPandas())
spark.stop()