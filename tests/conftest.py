import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    """Fixture for creating a spark session."""
    spark = (SparkSession.builder
            .appName("TestSession")
            .master("local[*]")
            .getOrCreate())
    yield spark
    spark.stop() 