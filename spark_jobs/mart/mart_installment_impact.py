from pyspark.sql import functions as F
from common.ch_spark_utils import get_spark, get_client

spark = get_spark()
client_dwh  = get_client("dwh")
client_mart = get_client("mart")

pay_data   = client_dwh.query("SELECT * FROM fact_payments")
payments   = spark.createDataFrame(pay_data.result_rows, schema=pay_data.column_names)

order_data = client_dwh.query("SELECT order_id, total_order_value FROM fact_orders")
orders     = spark.createDataFrame(order_data.result_rows, schema=order_data.column_names)

joined = payments.join(orders, on="order_id", how="left")

total_revenue = joined.agg(F.sum("payment_value")).collect()[0][0]

df = (
    joined
    .groupBy("installment_bucket")
    .agg(
        F.count("order_id").alias("total_transactions"),
        F.avg("payment_value").alias("avg_payment_value"),
        F.avg("total_order_value").alias("avg_order_value"),
        F.avg("payment_installments").alias("avg_installments"),
        F.sum("payment_value").alias("total_revenue"),
    )
    .withColumn(
        "pct_of_total_revenue",
        F.round(F.col("total_revenue") / total_revenue * 100, 2)
    )
)

client_mart.command("TRUNCATE TABLE IF EXISTS mart.installment_impact")
client_mart.insert_df("installment_impact", df.toPandas())
spark.stop()