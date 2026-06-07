from pyspark.sql import functions as F
from common.ch_spark_utils import get_spark, get_client

spark = get_spark()
client_raw = get_client("raw")
client_ods = get_client("ods")

cust_data = client_raw.query("SELECT * FROM customers")
customers = spark.createDataFrame(cust_data.result_rows, schema=cust_data.column_names)

geo_data = client_raw.query("SELECT * FROM geolocation")
geo = spark.createDataFrame(geo_data.result_rows, schema=geo_data.column_names)

geo = (
    geo
    .withColumn("geolocation_lat", F.col("geolocation_lat").cast("double"))
    .withColumn("geolocation_lng", F.col("geolocation_lng").cast("double"))
    .filter(
        F.col("geolocation_lat").isNotNull() &
        F.col("geolocation_lng").isNotNull() &
        F.col("geolocation_lat").between(-35.0, 5.5) &
        F.col("geolocation_lng").between(-74.0, -34.0)
    )
    .groupBy("geolocation_zip_code_prefix")
    .agg(
        F.percentile_approx("geolocation_lat", 0.5).alias("lat"),
        F.percentile_approx("geolocation_lng", 0.5).alias("lng"),
    )
)

df = (
    customers
    .join(
        geo,
        customers["customer_zip_code_prefix"] == geo["geolocation_zip_code_prefix"],
        how="left"
    )
    .drop("geolocation_zip_code_prefix")
    .filter(F.col("customer_id").isNotNull() & (F.col("customer_id") != ""))
)

client_ods.command("TRUNCATE TABLE IF EXISTS ods.customers")
client_ods.insert_df("customers", df.toPandas())
spark.stop()