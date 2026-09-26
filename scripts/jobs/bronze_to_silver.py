from pyspark.sql import SparkSession

# Add modular function transformer.py
from scripts.utils.transformer import clean_bronze_data

# 1. Initialize Spark Session
spark = SparkSession.builder.appName("StreamShop-BronzeToSilver").getOrCreate()

# 2. Securely fetch AWS credentials from Databricks Secrets
# access_key = dbutils.secrets.get(scope="aws-auth", key="access-key")
# secret_key = dbutils.secrets.get(scope="aws-auth", key="secret-key")

# 3. Configure Hadoop/Spark to talk to AWS S3 securely
# sc = spark.sparkContext
# sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_key)
# sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret_key)
# sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.ap-southeast-3.amazonaws.com")

# 4. Read the raw JSON from the Bronze layer in S3
# s3_bronze_path = "s3a://streamshop-raw-bronze/ecommerce/dummy_stream.json"

# Because the databricks workspace using serverless and already register as external location
# We can bypass the access and directly read from s3
s3_bronze_path = "s3://streamshop-raw-bronze/ecommerce/dummy_stream.json"
print(f"Reading raw data from: {s3_bronze_path}")

df_bronze = spark.read.json(s3_bronze_path)

# 5. Transform to Silver (Clean data types, filter out anomalies)
df_silver = clean_bronze_data(df_bronze)

# 6. Write to Silver layer in Delta format
s3_silver_path = "s3://streamshop-raw-bronze/ecommerce/silver/"
print(f"Writing Silver data to: {s3_silver_path}")
    
# Delta format adds ACID transactions and time-travel to our data lake
df_silver.write.format("delta").mode("append").save(s3_silver_path)