from pyspark.sql import SparkSession

# 1. Initialize Spark
spark = SparkSession.builder.appName("StreamShop-GoldToSnowflake").getOrCreate()

# 2. Fetch Credentials from Databricks Secret Scope
sf_url = dbutils.secrets.get(scope="aws-auth", key="snowflake-url")
sf_user = dbutils.secrets.get(scope="aws-auth", key="snowflake-user")
#sf_password = dbutils.secrets.get(scope="aws-auth", key="snowflake-password")
raw_private_key = dbutils.secrets.get(scope="aws-auth", key="snowflake-private-key")

sf_private_key = raw_private_key.replace("-----BEGIN PRIVATE KEY-----", "") \
                                .replace("-----END PRIVATE KEY-----", "") \
                                .replace("\n", "") \
                                .replace("\r", "") \
                                .strip()

# Configure Snowflake Connection Options
# sf_options = {
#   "sfUrl": sf_url,
#   "sfUser": sf_user,
#   "sfPassword": sf_password,
#   "sfDatabase": "STREAMSHOP_DB",
#   "sfSchema": "ANALYTICS",
#   "sfWarehouse": "STREAMSHOP_WH",
#   "sfRole": "STREAMSHOP_ETL_ROLE"
# }

# Configure Snowflake Connection Options (serverless-compatible)
sf_options = {
  "host": sf_url,
  "sfuser": sf_user,
  "pem_private_key": sf_private_key,
  "sfdatabase": "STREAMSHOP_DB",
  "sfschema": "ANALYTICS",
  "sfwarehouse": "STREAMSHOP_WH",
  "sfRole": "STREAMSHOP_ETL_ROLE"
}

# 3. Read Gold Data from S3
s3_gold_path = "s3://streamshop-raw-bronze/ecommerce/gold/platform_revenue_athena_serverless/"
print(f"Reading Gold data from: {s3_gold_path}")
df_gold = spark.read.format("delta").load(s3_gold_path)

# 4. Write to Snowflake
print("Pushing data to Snowflake...")
df_gold.write \
    .format("snowflake") \
    .options(**sf_options) \
    .option("dbtable", "GOLD_PLATFORM_REVENUE") \
    .mode("overwrite") \
    .save()

print("Successfully loaded Gold data into Snowflake.")