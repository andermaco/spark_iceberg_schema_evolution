import pytest
from src.utils.spark_utils import Utils
from pyspark.sql.types import StructType, StructField, StringType


def test_end_to_end_flow(spark):
    # Create test data
    test_data = [
        (1, "test1", "2023-01-01"),
        (2, "test2", "2023-01-02")
    ]
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("date", StringType(), True)
    ])
    df = spark.createDataFrame(test_data, schema)
    
    # Test schema alignment
    target_schema = StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("date", StringType(), True),
        StructField("new_col", StringType(), True)
    ])
    
    # Test schema alignment
    target_schema = df.schema
    aligned_df = Utils.align_schema(df, target_schema)
    assert aligned_df.count() == 2