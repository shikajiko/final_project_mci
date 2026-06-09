from pyspark.sql import functions as F
from common.ch_spark_utils import get_spark, get_client

spark = get_spark()
client_dwh  = get_client("dwh")
client_mart = get_client("mart")

pay_data  = client_dwh.query("SELECT * FROM fact_payments")
payments  = spark.createDataFrame(pay_data.result_rows, schema=pay_data.column_names)

date_data = client_dwh.query("SELECT date_id, year, month, month_name FROM dim_date")
dates     = spark.createDataFrame(date_data.result_rows, schema=date_data.column_names)

df = (
    payments
    .join(dates, on="date_id", how="left")
    .groupBy("payment_type", "installment_bucket", "year", "month", "month_name")
    .agg(
        F.count("order_id").alias("total_transactions"),
        F.sum("payment_value").alias("total_payment_value"),
        F.avg("payment_value").alias("avg_payment_value"),
        F.max("payment_value").alias("max_payment_value"),
        F.sum("is_installment").alias("total_installment_users"),
    )
    .withColumn(
        "pct_installment",
        F.round(F.col("total_installment_users") / F.col("total_transactions") * 100, 2)
    )
    .orderBy("year", "month", "payment_type")
)

client_mart.command("TRUNCATE TABLE IF EXISTS mart.payment_summary")
client_mart.insert_df("payment_summary", df.toPandas())
spark.stop()