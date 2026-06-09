from pyspark.sql import functions as F
from pyspark.sql.window import Window
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
    .groupBy("customer_unique_id", "customer_state")
    .agg(
        F.sum("payment_value").alias("total_payment_value"),
        F.countDistinct("order_id").alias("total_orders"),
    )
)

payment_mode = (
    payments_with_uid
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

customer_spend = customer_spend.join(payment_mode, on="customer_unique_id", how="left")

total_customers = customer_spend.count()
total_revenue   = customer_spend.agg(F.sum("total_payment_value")).collect()[0][0]

rank_window       = Window.orderBy(F.col("total_payment_value").desc())
cumulative_window = Window.orderBy(F.col("total_payment_value").desc()).rowsBetween(
    Window.unboundedPreceding, Window.currentRow
)

df = (
    customer_spend
    .withColumn("revenue_rank", F.row_number().over(rank_window))
    .withColumn("total_customers", F.lit(total_customers).cast("long"))
    .withColumn("total_revenue", F.lit(total_revenue))
    .withColumn(
        "rank_pct",
        F.round(F.col("revenue_rank") / total_customers * 100, 4)
    )
    .withColumn(
        "cumulative_revenue",
        F.sum("total_payment_value").over(cumulative_window)
    )
    .withColumn(
        "cumulative_revenue_pct",
        F.round(F.col("cumulative_revenue") / total_revenue * 100, 4)
    )
    .select(
        "customer_unique_id",
        "customer_state",
        "total_payment_value",
        "revenue_rank",
        "total_customers",
        "rank_pct",
        "cumulative_revenue",
        "total_revenue",
        "cumulative_revenue_pct",
        "preferred_payment_type",
        "total_orders",
    )
)

client_mart.command("TRUNCATE TABLE IF EXISTS mart.revenue_concentration")
client_mart.insert_df("revenue_concentration", df.toPandas())
spark.stop()