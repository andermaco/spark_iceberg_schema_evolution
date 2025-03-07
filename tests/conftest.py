import pytest
from pyspark.sql import SparkSession
import os


@pytest.fixture(scope="session", autouse=True)
def setup_test_env():
    """Setup test environment variables before each test."""
    os.environ.update({
        "AWS_S3_PATH": "s3://test-bucket/test-path",
        "GLUE_DATABASE": "test_db",
        "GLUE_TABLE": "test_table",
        "AWS_IAM_ROLE": "arn:aws:iam::123456789012:role/test-role",
        "WORKGROUP": "test_workgroup",
        "WORKGROUP_S3_PATH": "s3://test-workgroup",
        "AWS_REGION": "eu-west-1",
        
        # PySpark & Java Environment variables
        "PYSPARK_SUBMIT_ARGS": "--jars deploy/jar_libraries/iceberg-spark-runtime-3.3_2.12-1.6.1.jar,deploy/jar_libraries/bundle-2.17.161.jar,deploy/jar_libraries/url-connection-client-2.17.161.jar pyspark-shell",
        "JAVA_HOME": "/usr/lib/jvm/java-11-openjdk",
        "SPARK_HOME": "/opt/spark",
        "PYSPARK_PYTHON": "/home/cammac/.conda/envs/schema_evolution/bin/python",
        "PYSPARK_DRIVER_PYTHON": "/home/cammac/.conda/envs/schema_evolution/bin/python"
        
    })
    yield
    for key in ["AWS_S3_PATH", "GLUE_DATABASE", "GLUE_TABLE", 
               "AWS_IAM_ROLE", "WORKGROUP", "WORKGROUP_S3_PATH", "AWS_REGION"]:
        os.environ.pop(key, None)

@pytest.fixture(scope="session")
def spark():
    """Fixture for creating a spark session."""
    spark = (SparkSession.builder
            .appName("TestSession")
            .master("local[*]")
            .getOrCreate())
    yield spark
    spark.stop()
    
@pytest.fixture(scope="session")
def aws_config():    
    """Fixture for creating AWS config."""
    from src.config.settings import AWSConfig    
    print("Creating AWS config")
    return AWSConfig()