from pyspark.sql import functions as F
from pyspark.sql.window import Window
from common.ch_spark_utils import get_spark, get_client

spark = get_spark()
client_raw = get_client("raw")
client_ods = get_client("ods")

data = client_raw.query("SELECT * FROM order_payments")
df = spark.createDataFrame(data.result_rows, schema=data.column_names)

window = Window.partitionBy("order_id", "payment_sequential").orderBy(F.lit(1))
df = (
    df
    .withColumn("row_num", F.row_number().over(window))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

df = df.filter(F.col("order_id").isNotNull() & (F.col("order_id") != ""))

valid_types = ["credit_card", "boleto", "voucher", "debit_card", "not_defined"]
df = df.filter(F.col("payment_type").isin(valid_types))

df = (
    df
    .withColumn("payment_sequential",  F.col("payment_sequential").cast("int"))
    .withColumn("payment_installments", F.col("payment_installments").cast("int"))
    .withColumn("payment_value",        F.col("payment_value").cast("double"))
    .withColumn(
        "is_installment",
        F.when(F.col("payment_installments") > 1, 1).otherwise(0)
    )
    .withColumn(
        "installment_bucket",
        F.when(F.col("payment_installments") == 1,  F.lit("1"))
         .when(F.col("payment_installments") <= 3,  F.lit("2-3"))
         .when(F.col("payment_installments") <= 6,  F.lit("4-6"))
         .when(F.col("payment_installments") <= 12, F.lit("7-12"))
         .otherwise(F.lit("12+"))
    )
    .filter(F.col("order_id").isNotNull() & (F.col("order_id") != ""))
)

stats = df.select(
    F.expr("percentile_approx(payment_value, 0.75)").alias("p75"),
    F.expr("percentile_approx(payment_value, 0.90)").alias("p90"),
).collect()[0]

threshold = stats["p75"]

df = df.withColumn(
    "is_high_value_payment",
    F.when(F.col("payment_value") >= threshold, 1).otherwise(0)
)

client_ods.command("TRUNCATE TABLE IF EXISTS ods.order_payments")
client_ods.insert_df("order_payments", df.toPandas())
spark.stop()