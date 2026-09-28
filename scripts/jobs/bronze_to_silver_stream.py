from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pyspark.sql.functions import col, from_json

# 1. Initialize Spark
spark = SparkSession.builder.appName("StreamShop-BronzeToSilver").getOrCreate()

# 2. Define Paths
s3_bronze_path = "s3://streamshop-raw-bronze/ecommerce/bronze/streaming_transactions/"
s3_silver_path = "s3://streamshop-raw-bronze/ecommerce/silver/streaming_transactions_cleaned/"
silver_checkpoint = "s3://streamshop-raw-bronze/ecommerce/silver/checkpoints/streaming_transactions/"

# 3. Define the exact schema of your Python generator's JSON payload
json_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_timestamp", TimestampType(), True),
    StructField("platform", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("checkout_amount", DoubleType(), True)
])

print("Reading from Bronze Stream...")

# 4. Read the Bronze Delta table as a stream
df_bronze_stream = spark.readStream \
    .format("delta") \
    .load(s3_bronze_path)

# 5. Transform: Crack the JSON string into native columns
df_silver_stream = df_bronze_stream \
    .withColumn("parsed_data", from_json(col("json_payload"), json_schema)) \
    .select(
        col("parsed_data.event_id").alias("event_id"),
        col("parsed_data.event_timestamp").alias("event_timestamp"),
        col("parsed_data.platform").alias("platform"),
        col("parsed_data.user_id").alias("user_id"),
        col("parsed_data.checkout_amount").alias("checkout_amount"),
        col("kafka_timestamp") # Keeping the original ingestion time for auditing
    )

print(f"Writing stream to Silver Layer: {s3_silver_path}")

# 6. Write to Silver using Serverless micro-batching
silver_query = df_silver_stream.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", silver_checkpoint) \
    .trigger(availableNow=True) \
    .start(s3_silver_path)

silver_query.awaitTermination()