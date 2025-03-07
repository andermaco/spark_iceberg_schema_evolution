import pytest
from src.utils.spark_utils import Utils
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def test_create_spark_session(spark):
    assert spark is not None
    assert spark.sparkContext.appName == "TestSession"

def test_align_schema(spark):
    # Create source DataFrame with a simple schema
    source_data = [("1", "John"), ("2", "Jane")]
    source_schema = StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True)
    ])
        
    source_df = spark.createDataFrame(data=source_data, schema=source_schema)
    
    # Create target schema with an additional column
    target_schema = StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True)  # Additional column
    ])
    
    # Align schema
    result_df = Utils.align_schema(source_df, target_schema)
    
    # Verify results
    assert "age" in result_df.columns
    assert len(result_df.columns) == 3
    assert result_df.schema["id"].dataType == StringType()
    assert result_df.schema["name"].dataType == StringType()
    assert result_df.schema["age"].dataType == IntegerType()
    
    # Verify data is preserved
    result_data = result_df.collect()
    assert len(result_data) == 2
    assert result_data[0]["id"] == "1"
    assert result_data[0]["name"] == "John"
    assert result_data[0]["age"] is None  # New column should be null