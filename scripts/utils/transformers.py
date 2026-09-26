from pyspark.sql.functions import col, to_timestamp, to_date, sum as _sum, count

def clean_bronze_data(df):
    """
    Cleans raw bronze e-commerce data by casting timestamps 
    and filtering out negative amounts.
    """
    return df.withColumn("event_time", to_timestamp(col("event_time"))) \
             .filter(col("amount") >= 0)

def aggregate_platform_revenue(df_silver):
    """
    Aggregates clean silver data into daily revenue and event counts per platform.
    """
    # Extract just the date from the timestamp
    df_with_date = df_silver.withColumn("event_date", to_date(col("event_time")))
    
    # Group and aggregate
    return df_with_date.groupBy("event_date", "platform") \
                       .agg(
                           _sum("amount").alias("total_revenue"),
                           count("event_id").alias("total_events")
                       )