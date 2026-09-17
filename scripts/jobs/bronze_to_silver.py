from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp

# 1. Initialize Spark Session
spark = SparkSession.builder.appName("StreamShop-BronzeToSilver").getOrCreate()

# 2. Securely fetch AWS credentials from Databricks Secrets
access_key = dbutils.secrets.get(scope="aws-auth", key="access-key")
secret_key = dbutils.secrets.get(scope="aws-auth", key="secret-key")

# 3. Configure Hadoop/Spark to talk to AWS S3 securely
sc = spark.sparkContext
sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_key)
sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret_key)
sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.ap-southeast-3.amazonaws.com")

# 4. Read the raw JSON from the Bronze layer in S3
s3_bronze_path = "s3a://streamshop-raw-bronze/ecommerce/dummy_stream.json"
print(f"Reading raw data from: {s3_bronze_path}")

df_bronze = spark.read.json(s3_bronze_path)

# 5. Transform to Silver (Clean data types, filter out anomalies)
df_silver = df_bronze.withColumn("event_time", to_timestamp(col("event_time"))) \
                     .filter(col("amount") >= 0)

# 6. Display results
print(f"Total valid events processed: {df_silver.count()}")
df_silver.display()