import pytest
from pyspark.sql import SparkSession
from scripts.utils.transformers import clean_bronze_data

# Create a local Spark session for testing
@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.appName("TestSession").master("local[1]").getOrCreate()

def test_clean_bronze_data(spark):
    # 1. Create dummy input data
    dummy_data = [
        {"event_time": "2023-01-01T12:00:00Z", "amount": 100.0},  # Valid
        {"event_time": "2023-01-01T12:05:00Z", "amount": -50.0}   # Invalid amount
    ]
    df_input = spark.createDataFrame(dummy_data)

    # 2. Run the transformation
    df_output = clean_bronze_data(df_input)
    
    # 3. Validate the results
    results = df_output.collect()
    
    # It should filter out the negative amount, leaving only 1 row
    assert len(results) == 1
    assert results[0]["amount"] == 100.0
    # Ensure event_time was successfully casted to a timestamp object
    assert str(results[0]["event_time"].__class__.__name__) == "datetime"

def test_aggregate_platform_revenue(spark):
    # 1. Create dummy Silver data (already clean)
    dummy_data = [
        {"event_id": "1", "event_time": datetime.datetime(2023, 1, 1, 10, 0, 0), "platform": "iOS", "amount": 100.0},
        {"event_id": "2", "event_time": datetime.datetime(2023, 1, 1, 11, 0, 0), "platform": "iOS", "amount": 50.0},
        {"event_id": "3", "event_time": datetime.datetime(2023, 1, 1, 12, 0, 0), "platform": "Web", "amount": 25.0}
    ]
    df_input = spark.createDataFrame(dummy_data)
    
    # 2. Run the Gold aggregation
    df_gold = aggregate_platform_revenue(df_input)
    results = df_gold.collect()
    
    # 3. Validate iOS aggregated correctly (100 + 50 = 150 revenue, 2 events)
    ios_result = next(row for row in results if row["platform"] == "iOS")
    assert ios_result["total_revenue"] == 150.0
    assert ios_result["total_events"] == 2