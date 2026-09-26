from pyspark.sql.functions import col, to_timestamp

def clean_bronze_data(df):
    """
    Cleans raw bronze e-commerce data by casting timestamps 
    and filtering out negative amounts.
    """
    return df.withColumn("event_time", to_timestamp(col("event_time"))) \
             .filter(col("amount") >= 0)