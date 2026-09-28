from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, sum as _sum, count

# 1. Initialize Spark
spark = SparkSession.builder.appName("StreamShop-SilverToGold").getOrCreate()

# 2. Define Paths
s3_silver_path = "s3://streamshop-raw-bronze/ecommerce/silver/streaming_transactions_cleaned/"
s3_gold_path = "s3://streamshop-raw-bronze/ecommerce/gold/streaming_platform_revenue/"
gold_checkpoint = "s3://streamshop-raw-bronze/ecommerce/gold/checkpoints/streaming_platform_revenue/"

print("Reading from Silver Stream...")

# 3. Read the Silver Delta table as a stream
df_silver_stream = spark.readStream \
    .format("delta") \
    .load(s3_silver_path)

# 4. Apply Watermark and Window Aggregation
df_gold_stream = df_silver_stream \
    .withWatermark("event_timestamp", "10 minutes") \
    .groupBy(
        col("platform"),
        window(col("event_timestamp"), "5 minutes") # Group data into 5-minute chunks
    ) \
    .agg(
        _sum("checkout_amount").alias("total_revenue"),
        count("event_id").alias("total_events")
    ) \
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("platform"),
        col("total_revenue"),
        col("total_events")
    )

print(f"Writing aggregated stream to Gold Layer: {s3_gold_path}")

# 5. Write to Gold using Serverless micro-batching
gold_query = df_gold_stream.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", gold_checkpoint) \
    .trigger(availableNow=True) \
    .start(s3_gold_path)

gold_query.awaitTermination()