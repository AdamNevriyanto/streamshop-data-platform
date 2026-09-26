from pyspark.sql import SparkSession
from scripts.utils.transformers import aggregate_platform_revenue

# 1. Initialize Spark
spark = SparkSession.builder.appName("StreamShop-SilverToGold").getOrCreate()

# 2. AWS Credentials
#access_key = dbutils.secrets.get(scope="aws-auth", key="access-key")
#secret_key = dbutils.secrets.get(scope="aws-auth", key="secret-key")

#sc = spark.sparkContext
#sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_key)
#sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret_key)
#sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.ap-southeast-3.amazonaws.com")

# 3. Paths
#s3_silver_path = "s3a://streamshop-raw-bronze/ecommerce/silver/"
#s3_gold_path = "s3a://streamshop-raw-bronze/ecommerce/gold/platform_revenue/"

#Because we are using serverless and the environment databricks already configures external location
#We can directly read without using spark context
s3_silver_path = "s3://streamshop-raw-bronze/ecommerce/silver/"
s3_gold_path = "s3://streamshop-raw-bronze/ecommerce/gold/platform_revenue/"

# 4. Read Silver Data
print(f"Reading Silver data from: {s3_silver_path}")
df_silver = spark.read.format("delta").load(s3_silver_path)

# 5. Transform to Gold
df_gold = aggregate_platform_revenue(df_silver)

# 6. Write to Gold layer (Overwrite because it's a daily aggregate)
print(f"Writing Gold data to: {s3_gold_path}")
df_gold.write.format("delta").mode("overwrite").save(s3_gold_path)