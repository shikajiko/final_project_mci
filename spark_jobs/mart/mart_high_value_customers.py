from pyspark.sql import functions as F
from common.ch_spark_utils import get_spark, get_client

spark = get_spark()
client_dwh  = get_client("dwh")
client_mart = get_client("mart")

pay_data  = client_dwh.query("SELECT * FROM fact_payments")
payments  = spark.createDataFrame(pay_data.result_rows, schema=pay_data.column_names)

cust_data = client_dwh.query("SELECT customer_id, customer_unique_id FROM dim_customer")
customers = spark.createDataFrame(cust_data.result_rows, schema=cust_data.column_names)

threshold = (
    payments
    .select(F.expr("percentile_approx(payment_value, 0.75)").alias("p75"))
    .collect()[0]["p75"]
)

high_value = payments.filter(F.col("payment_value") >= threshold)

payment_mode = (
    high_value
    .groupBy("customer_id", "payment_type")
    .agg(F.count("order_id").alias("type_count"))
    .withColumn(
        "rank",
        F.row_number().over(
            __import__("pyspark.sql.window", fromlist=["Window"])
            .Window.partitionBy("customer_id")
            .orderBy(F.col("type_count").desc())
        )
    )
    .filter(F.col("rank") == 1)
    .select("customer_id", F.col("payment_type").alias("preferred_payment_type"))
)

df = (
    high_value
    .groupBy("customer_id", "customer_state")
    .agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.sum("payment_value").alias("total_payment_value"),
        F.avg("payment_value").alias("avg_payment_value"),
        F.avg("is_installment").alias("pct_installment_usage"),
        F.avg("payment_installments").alias("avg_installments"),
    )
    .withColumn("pct_installment_usage", F.col("pct_installment_usage") * 100)
    .join(payment_mode, on="customer_id", how="left")
    .join(customers,    on="customer_id", how="left")
    .select(
        "customer_unique_id",
        "customer_state",
        "total_orders",
        "total_payment_value",
        "avg_payment_value",
        "preferred_payment_type",
        "pct_installment_usage",
        "avg_installments",
    )
)

client_mart.command("TRUNCATE TABLE IF EXISTS mart.high_value_customers")
client_mart.insert_df("high_value_customers", df.toPandas())
spark.stop()