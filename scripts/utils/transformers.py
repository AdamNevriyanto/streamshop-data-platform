from pyspark.sql.functions import col, to_timestamp

def clean_bronze_data(df):
    return df.withColumn("event_time", to_timestamp(col("event_time"))) \
             .filter(col("amount") >= 0)
