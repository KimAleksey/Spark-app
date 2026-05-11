from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("S3 Pipeline").getOrCreate()

# 📥 Чтение из S3 (MinIO)
df = spark.read.parquet("s3a://nyc-taxi/yellow_tripdata/2025/*")

# 🔄 трансформация
df_clean = df.dropna()

# silver
df_silver = (
    df_clean
    .withColumn(
        "trip_duration_min",
        (unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("tpep_pickup_datetime")) / 60
    )
    .withColumn("pickup_hour", hour("tpep_pickup_datetime"))
    .withColumn("pickup_day", to_date("tpep_pickup_datetime"))
    .withColumn("day_of_week", dayofweek("tpep_pickup_datetime"))
)

# metrics
df_silver = df_silver.withColumn(
    "revenue",
    col("fare_amount") + col("extra") + col("mta_tax") + col("congestion_surcharge")
)

df_silver = df_silver.withColumn(
    "tip_ratio",
    col("tip_amount") / col("total_amount")
)

df_silver = df_silver.withColumn(
    "fare_per_mile",
    col("fare_amount") / col("trip_distance")
)

agg_daily = (
    df_silver
    .groupBy(
        "pickup_day",
        "VendorID"
    )
    .agg(
        count("*").alias("total_trips"),
        avg("trip_distance").alias("avg_distance"),
        avg("trip_duration_min").alias("avg_duration"),
        avg("total_amount").alias("avg_total"),
        avg("tip_amount").alias("avg_tip"),
        sum("revenue").alias("total_revenue")
    )
)

agg_zone = (
    df_silver
    .groupBy("PULocationID")
    .agg(
        count("*").alias("trips"),
        avg("total_amount").alias("avg_revenue"),
        avg("trip_distance").alias("avg_distance"),
        avg("trip_duration_min").alias("avg_duration")
    )
)

agg_payment = (
    df_silver
    .groupBy("payment_type", "VendorID")
    .agg(
        count("*").alias("trips"),
        avg("tip_amount").alias("avg_tip"),
        avg("total_amount").alias("avg_total")
    )
)

# 📤 запись обратно в S3
agg_daily.write.mode("overwrite").parquet("s3a://nyc-taxi/yellow_tripdata/agg/2025/daily")
agg_zone.write.mode("overwrite").parquet("s3a://nyc-taxi/yellow_tripdata/agg/2025/zone")
agg_payment.write.mode("overwrite").parquet("s3a://nyc-taxi/yellow_tripdata/agg/2025/payment")

spark.stop()