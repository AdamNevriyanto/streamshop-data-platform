import os
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
from pyspark.sql.avro.functions import from_avro

# 1. Define Credentials (In production, these come from dbutils.secrets)
kafka_key = os.getenv("CONFLUENT_API_KEY")
kafka_secret = os.getenv("CONFLUENT_API_SECRET")
sr_url = os.getenv("SCHEMA_REGISTRY_URL")
sr_key = os.getenv("SCHEMA_REGISTRY_KEY")
sr_secret = os.getenv("SCHEMA_REGISTRY_SECRET")

# 2. Dynamically Fetch the Data Contract from Schema Registry
print("Fetching latest Avro Schema from Registry...")
subject = "ecommerce_transactions_avro-value"
api_url = f"{sr_url}/subjects/{subject}/versions/latest"
response = requests.get(api_url, auth=(sr_key, sr_secret))
response.raise_for_status() # Fails fast if credentials are wrong
avro_schema_str = response.json()["schema"]

# 3. Initialize Spark
spark = SparkSession.builder.appName("StreamShop-AvroBronze").getOrCreate()

s3_bronze_avro_path = "s3a://streamshop-raw-bronze/ecommerce/bronze/avro_transactions/"
bronze_checkpoint = "s3a://streamshop-raw-bronze/ecommerce/bronze/checkpoints/avro_transactions/"

print("Connecting to Kafka...")

# 4. Read the Binary Stream from Kafka
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "pkc-oz2po.ap-southeast-3.aws.confluent.cloud:9092") \
    .option("subscribe", "ecommerce_transactions_avro") \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config", f"org.apache.kafka.common.security.plain.PlainLoginModule required username='{kafka_key}' password='{kafka_secret}';") \
    .option("startingOffsets", "earliest") \
    .load()

# 5. The Magic Slice & Deserialize
# Spark SUBSTRING is 1-indexed. Starting at index 6 skips the 5-byte Confluent header.
df_parsed = df_kafka \
    .withColumn("pure_avro_binary", expr("SUBSTRING(value, 6)")) \
    .withColumn("data", from_avro(col("pure_avro_binary"), avro_schema_str)) \
    .select(
        col("data.event_id").alias("event_id"),
        col("data.event_timestamp").alias("event_timestamp"),
        col("data.platform").alias("platform"),
        col("data.user_id").alias("user_id"),
        col("data.checkout_amount").alias("checkout_amount"),
        col("timestamp").alias("kafka_timestamp")
    )

print(f"Writing decoded stream to Bronze Layer: {s3_bronze_avro_path}")

# 6. Write cleanly structured data directly to Bronze!
query = df_parsed.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", bronze_checkpoint) \
    .trigger(availableNow=True) \
    .start(s3_bronze_avro_path)

query.awaitTermination()