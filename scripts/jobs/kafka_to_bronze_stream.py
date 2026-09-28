from pyspark.sql import SparkSession

# 1. Initialize Spark 
spark = SparkSession.builder.appName("StreamShop-KafkaToBronze").getOrCreate()

# 2. Fetch Credentials Securely
api_key = dbutils.secrets.get(scope="aws-auth", key="confluent-api-key")
api_secret = dbutils.secrets.get(scope="aws-auth", key="confluent-api-secret")

# 3. Configure Kafka Options
kafka_options = {
  "kafka.bootstrap.servers": "pkc-oz2po.ap-southeast-3.aws.confluent.cloud:9092",
  "kafka.security.protocol": "SASL_SSL",
  "kafka.sasl.mechanism": "PLAIN",
  "kafka.sasl.jaas.config": f"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='{api_key}' password='{api_secret}';",
  "startingOffsets": "earliest",
  "subscribe": "ecommerce_transactions_live" # Pointing to the live topic
}

print("Connecting to Confluent Cloud Stream...")

# 4. Read the Real-Time Stream
df_stream = spark.readStream \
    .format("kafka") \
    .options(**kafka_options) \
    .load()

# 5. Extract the JSON payload 
# (Kafka stores data in binary by default, so we cast the 'value' column to a string)
df_bronze = df_stream.selectExpr("CAST(value AS STRING) as json_payload", "timestamp as kafka_timestamp")

# 6. Write the Stream to the Bronze Layer
s3_bronze_streaming_path = "s3://streamshop-raw-bronze/ecommerce/bronze/streaming_transactions/"
checkpoint_path = "s3://streamshop-raw-bronze/ecommerce/bronze/checkpoints/streaming_transactions/"

print(f"Writing stream to {s3_bronze_streaming_path}")

# Write the Stream to the Bronze Layer with a Serverless Trigger
streaming_query = df_bronze.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", checkpoint_path) \
    .trigger(availableNow=True) \
    .start(s3_bronze_streaming_path)

# You must add this line when using availableNow so the cluster knows to shut down
# after the micro-batch finishes processing, rather than hanging open.
streaming_query.awaitTermination()