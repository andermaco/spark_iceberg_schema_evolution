from src.utils.spark_utils import Utils
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from unittest.mock import patch, MagicMock
import pytest
from src.config.settings import AWSConfig


def test_create_spark_session(spark):
    assert spark is not None
    assert spark.sparkContext.appName == "TestSession"

def test_configure_aws_glue_catalog(spark):
    # Configure AWS Glue catalog
    Utils.configure_aws_glue_catalog(spark)
    
    # Verify configuration settings
    assert spark.conf.get("spark.sql.catalog.AwsGlueCatalog") == "org.apache.iceberg.spark.SparkCatalog"
    assert spark.conf.get("spark.sql.catalog.AwsGlueCatalog.catalog-impl") == "org.apache.iceberg.aws.glue.GlueCatalog"
    assert spark.conf.get("spark.sql.catalog.AwsGlueCatalog.warehouse") == "s3a://bd-datawarehouse/"
    assert spark.conf.get("spark.sql.catalog.AwsGlueCatalog.io-impl") == "org.apache.iceberg.aws.s3.S3FileIO"
    assert spark.conf.get("spark.sql.sources.partitionOverwriteMode") == "dynamic"
    
def test_compare_schemas():
    # Create source schema
    source_schema = StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True)
    ])

    # Create target schema with an additional column
    target_schema = StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True)  # Additional column
    ])
    
    # Compare schemas
    missing_in_1, missing_in_2, diff_types = Utils.compare_schemas(source_schema, target_schema)
    
    # Verify results
    assert missing_in_1 == {"age"}
    assert missing_in_2 == set()
    assert diff_types == {}
    
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
    

@pytest.fixture
def mock_dataframe(spark_session):
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True)
    ])
    data = [("1", "John"), ("2", "Jane")]
    return spark_session.createDataFrame(data, schema)

@pytest.fixture
def mock_aws_config():
    return AWSConfig(
        s3_path="s3://test-bucket/test-path",
        glue_database="test_db",
        glue_table="test_table",        
        iam_role="arn:aws:iam::123456789012:role/TestRole",
        workgroup="test_wg",
        workgroup_s3_path="s3://test-workgroup",
        region="eu-west-1"
    )
    
@patch("boto3.client")
@patch("boto3.Session")
@patch("awswrangler.athena.to_iceberg")
def test_write_to_s3_glue(mock_to_iceberg, mock_boto3_session, mock_boto3_client, mock_dataframe, mock_aws_config):
    # Mock boto3 client responses
    mock_sts_client = MagicMock()
    mock_sts_client.assume_role.return_value = {
        "Credentials": {
            "AccessKeyId": "test_access_key",
            "SecretAccessKey": "test_secret_key",
            "SessionToken": "test_session_token"
        }
    }
    mock_boto3_client.return_value = mock_sts_client

    # Mock boto3 session
    mock_session = MagicMock()
    mock_boto3_session.return_value = mock_session

    # Call the function
    Utils.write_to_s3_glue(mock_dataframe, mock_aws_config, ["id"])

    # Verify boto3 interactions
    mock_boto3_client.assert_called_once_with("sts")
    mock_sts_client.assume_role.assert_called_once_with(
        RoleArn=mock_aws_config.iam_role,
        RoleSessionName="WriteToS3GlueSession"
    )
    mock_boto3_session.assert_called_once_with(
        aws_access_key_id="test_access_key",
        aws_secret_access_key="test_secret_key",
        aws_session_token="test_session_token"
    )

    # Verify awswrangler call
    assert mock_to_iceberg.call_count == 1
    call_kwargs = mock_to_iceberg.call_args.kwargs
    
    # Verify all arguments except the DataFrame
    assert call_kwargs['database'] == mock_aws_config.glue_database
    assert call_kwargs['table'] == mock_aws_config.glue_table
    assert call_kwargs['temp_path'] == "s3://bd-test-tq-wg/temp/"
    assert call_kwargs['table_location'] == "s3://bd-datawarehouse/customers_db/customers_table/"
    assert call_kwargs['workgroup'] == mock_aws_config.workgroup
    assert call_kwargs['schema_evolution'] is True
    assert call_kwargs['keep_files'] is False
    assert call_kwargs['fill_missing_columns_in_df'] is True
    assert call_kwargs['partition_cols'] == ["id"]