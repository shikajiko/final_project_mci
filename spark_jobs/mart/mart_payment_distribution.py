from pyspark.sql import functions as F
from common.ch_spark_utils import get_spark, get_client

spark       = get_spark()
client_dwh  = get_client("dwh")
client_mart = get_client("mart")

pay_data = client_dwh.query("SELECT payment_type, payment_value FROM fact_payments")
payments = spark.createDataFrame(pay_data.result_rows, schema=pay_data.column_names)

df = (
    payments
    .groupBy("payment_type")
    .agg(
        F.count("payment_value").alias("transaction_count"),
        F.round(F.mean("payment_value"), 2).alias("mean_payment_value"),
        F.round(F.stddev("payment_value"), 2).alias("stddev_payment_value"),
        F.round(F.expr("percentile_approx(payment_value, 0.10)"), 2).alias("p10"),
        F.round(F.expr("percentile_approx(payment_value, 0.25)"), 2).alias("p25"),
        F.round(F.expr("percentile_approx(payment_value, 0.50)"), 2).alias("p50_median"),
        F.round(F.expr("percentile_approx(payment_value, 0.75)"), 2).alias("p75"),
        F.round(F.expr("percentile_approx(payment_value, 0.90)"), 2).alias("p90"),
        F.round(F.max("payment_value"), 2).alias("max_payment_value"),
        F.round(F.min("payment_value"), 2).alias("min_payment_value"),
    )
)

client_mart.command("TRUNCATE TABLE IF EXISTS mart.payment_distribution")
client_mart.insert_df("payment_distribution", df.toPandas())

spark.stop()