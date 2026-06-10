from pyspark.sql import functions as F
from pyspark.sql.window import Window
from common.ch_spark_utils import get_spark, get_client

spark       = get_spark()
client_dwh  = get_client("dwh")
client_mart = get_client("mart")

pay_data  = client_dwh.query("SELECT * FROM fact_payments")
payments  = spark.createDataFrame(pay_data.result_rows, schema=pay_data.column_names)

cust_data = client_dwh.query("SELECT customer_id, customer_unique_id FROM dim_customer")
customers = spark.createDataFrame(cust_data.result_rows, schema=cust_data.column_names)

ord_data    = client_dwh.query("SELECT order_id, date_id FROM fact_orders")
fact_orders = spark.createDataFrame(ord_data.result_rows, schema=ord_data.column_names)

payments_with_uid = payments.join(customers, on="customer_id", how="left")

reference_date = fact_orders.agg(F.max("date_id")).collect()[0][0]

order_dates = (
    fact_orders
    .join(
        payments_with_uid.select("order_id", "customer_unique_id").distinct(),
        on="order_id",
        how="left"
    )
    .groupBy("customer_unique_id")
    .agg(
        F.max("date_id").alias("last_order_date"),
        F.min("date_id").alias("first_order_date"),
    )
    .withColumn(
        "recency_days",
        F.datediff(
            F.lit(reference_date).cast("date"),
            F.col("last_order_date").cast("date")
        )
    )
)

customer_spend = (
    payments_with_uid
    .groupBy("customer_unique_id", "customer_state")
    .agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.sum("payment_value").alias("total_payment_value"),
        F.avg("payment_value").alias("avg_payment_value"),
        F.avg("is_installment").alias("pct_installment_usage"),
        F.avg("payment_installments").alias("avg_installments"),
    )
    .withColumn("pct_installment_usage", F.col("pct_installment_usage") * 100)
)

threshold = (
    customer_spend
    .select(F.expr("percentile_approx(total_payment_value, 0.75)").alias("p75"))
    .collect()[0]["p75"]
)

high_value_customers = customer_spend.filter(F.col("total_payment_value") >= threshold)
qualifying_ids       = high_value_customers.select("customer_unique_id")

payment_mode = (
    payments_with_uid
    .join(qualifying_ids, on="customer_unique_id", how="inner")
    .groupBy("customer_unique_id", "payment_type")
    .agg(F.count("order_id").alias("type_count"))
    .withColumn(
        "rank",
        F.row_number().over(
            Window.partitionBy("customer_unique_id")
            .orderBy(F.col("type_count").desc())
        )
    )
    .filter(F.col("rank") == 1)
    .select("customer_unique_id", F.col("payment_type").alias("preferred_payment_type"))
)

avg_interpurchase_days = F.when(
    F.col("total_orders") > 1,
    F.datediff(F.col("last_order_date").cast("date"), F.col("first_order_date").cast("date"))
    / (F.col("total_orders") - 1)
).otherwise(F.lit(180.0))


df = (
    high_value_customers
    .join(payment_mode, on="customer_unique_id", how="left")
    .join(order_dates,  on="customer_unique_id", how="left")
    .withColumn(
        "clv_estimate",
        F.round(
            F.col("avg_payment_value")
            * (F.lit(365.0) / F.greatest(avg_interpurchase_days, F.lit(30.0))),
            2)
        )
    .select(
        "customer_unique_id",
        "customer_state",
        "total_orders",
        F.round("total_payment_value", 2).alias("total_payment_value"),
        F.round("avg_payment_value",   2).alias("avg_payment_value"),
        "preferred_payment_type",
        F.round("pct_installment_usage", 2).alias("pct_installment_usage"),
        F.round("avg_installments",      2).alias("avg_installments"),
        "last_order_date",
        "first_order_date",
        "recency_days",
        "clv_estimate",
    )
)

client_mart.command("TRUNCATE TABLE IF EXISTS mart.high_value_customers")
client_mart.insert_df("high_value_customers", df.toPandas())

spark.stop()