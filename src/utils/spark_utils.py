# from narwhals import DataFrame
import gc
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, DataType
from pyspark.sql.functions import lit
from dataclasses import dataclass
from typing import List, Tuple, Set, Dict
import awswrangler as wr
import boto3
import pandas as pd
from pyspark.sql.types import TimestampType
from src.config.settings import AWSConfig



@dataclass
class Utils():    
    
    @staticmethod
    def create_spark_session(app_name: str) -> SparkSession:
        """
        Create a SparkSession with the given application name.
        """
        return SparkSession.builder \
                .appName(app_name) \
                .getOrCreate()

    @staticmethod
    def configure_aws_glue_catalog(spark: SparkSession, glue_db: str, glue_table: str) -> None:
        """
        Configure the AWS Glue Catalog and S3 for the Spark session.
        """       
        spark.conf.set("spark.sql.catalog.AwsGlueCatalog", "org.apache.iceberg.spark.SparkCatalog") 
        spark.conf.set("spark.sql.catalog.AwsGlueCatalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") 
        spark.conf.set("spark.sql.catalog.AwsGlueCatalog.warehouse", "s3a://bd-datawarehouse/") 
        spark.conf.set("spark.sql.catalog.AwsGlueCatalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        
        
        # Enable Arrow conversion -> https://docs.tecton.ai/docs/tips-and-tricks/troubleshooting/conversion-from-pyspark-dataframe-to-pandas-dataframe-with-pandas-2-0
        # spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")  
        

    @staticmethod
    def compare_schemas(schema1: StructType, schema2: StructType) -> Tuple[Set[str], Set[str], Dict[str, Tuple[DataType, DataType]]]:
        """
        Compara dos esquemas y devuelve:
        - Conjunto de campos que faltan en schema1 respecto a schema2.
        - Conjunto de campos que faltan en schema2 respecto a schema1.
        - Diferencias de tipos para campos comunes.
        """
        fields1 = {f.name: f.dataType for f in schema1.fields}
        fields2 = {f.name: f.dataType for f in schema2.fields}

        missing_in_1 = set(fields2.keys()) - set(fields1.keys())
        missing_in_2 = set(fields1.keys()) - set(fields2.keys())

        diff_types = {
            name: (fields1[name], fields2[name])
            for name in set(fields1.keys()) & set(fields2.keys())
            if fields1[name] != fields2[name]
        }

        return missing_in_1, missing_in_2, diff_types


    @staticmethod
    def align_schema(new_df: DataFrame, target_schema: StructType) -> DataFrame:
        """
        Aligns the schema of a DataFrame to a target schema.
        Args:
            new_df: The DataFrame to align.
            target_schema: The target schema to align to.
        Returns:
            A new DataFrame with the aligned schema.
        """
        new_fields = new_df.schema.fields
        target_fields = target_schema.fields
        aligned_fields = []

        # Add fields from the target schema
        for target_field in target_fields:
            if target_field.name in new_df.columns:
                # If the column exists in the new DataFrame, use its existing type
                aligned_fields.append(new_df[target_field.name].cast(target_field.dataType).alias(target_field.name))
            else:
                # If the column is missing, add a null column with the target type
                aligned_fields.append(lit(None).cast(target_field.dataType).alias(target_field.name))

        # Add any extra fields from the new DataFrame that are not in the target schema.
        for new_field in new_fields:
            if new_field.name not in target_schema.fieldNames():
                aligned_fields.append(new_df[new_field.name])

        return new_df.select(*aligned_fields)
       
           
    @staticmethod
    def write_to_s3_glue(df: DataFrame, aws_config: AWSConfig, partition_cols: list):
        """
        Write DataFrame to S3 and create/update Glue catalog table using specified IAM role
        """
        # Create session with role assumption
        sts_client = boto3.client('sts')
        assumed_role = sts_client.assume_role(
            RoleArn=aws_config.iam_role,
            RoleSessionName='WriteToS3GlueSession'
        )
        
        # Create boto3 session with temporary credentials
        boto3_session = boto3.Session(
            aws_access_key_id=assumed_role['Credentials']['AccessKeyId'],
            aws_secret_access_key=assumed_role['Credentials']['SecretAccessKey'],
            aws_session_token=assumed_role['Credentials']['SessionToken']
        )
        
        # Convert to pandas and handle schema evolution
        pandas_df = df.toPandas()

        # Write to table using awswrangler
        wr.athena.to_iceberg(
            df=pandas_df,
            database=aws_config.glue_database,
            table=aws_config.glue_table,
            # temp_path=aws_config.workgroup_s3_path,
            temp_path="s3://bd-test-tq-wg/temp/",
            # table_location=aws_config.workgroup_s3_path,
            table_location="s3://bd-datawarehouse/customers_db/customers_table/",
            # mode="overwrite",
            # data_source="AwsDataCatalog",
            workgroup=aws_config.workgroup,
            schema_evolution=True,
            keep_files=False,
            fill_missing_columns_in_df=True,
            partition_cols=partition_cols
        )
        

    @staticmethod
    def convert_datetime_columns(df: DataFrame) -> DataFrame:
        """
        Converts datetime columns to TimestampType.
        """
        for col_name, data_type in df.dtypes:
            if "date" in data_type.lower():  # Check for date or timestamp
                df = df.withColumn(col_name, df[col_name].cast(TimestampType()))
        
        print(df.dtypes)
        print(df['SubscriptionDate'].dtype) 
        
        return df      
        
    def create_iceberg_table(spark: SparkSession, database_name: str, table_name: str,
                             bucket_path: str, partition_cols: list):
        """
        Creates an Iceberg table in AWS Glue Catalog.
        """
        spark.sql(f"""    
            CREATE TABLE IF NOT EXISTS AwsGlueCatalog.{database_name}.{table_name} (
                created_at timestamp
            )            
            PARTITIONED BY (month(created_at))
            LOCATION '{bucket_path}'
            TBLPROPERTIES (
                'table_type' = 'ICEBERG',
                'format'='parquet',        
                'write_compression'='ZSTD',        
                'optimize_rewrite_data_file_threshold'='5',
                'optimize_rewrite_delete_file_threshold'='2',
                'vacuum_min_snapshots_to_keep'='5'
            )
        """)
        # PARTITIONED BY ({', '.join([f'{col}' for col in partition_cols]) if partition_cols else ''})

    @staticmethod
    def get_glue_iceberg_schema(spark: SparkSession, glue_db: str, glue_table: str) -> Tuple[StructType, DataFrame]:
        """
        Retrieves the schema and DataFrame of an Iceberg table from AWS Glue Catalog.
        """
        try:
            df = spark.read.format("iceberg").load(f"AwsGlueCatalog.{glue_db}.{glue_table}")
            return df.schema, df
        except Exception as e:
            print(f"Error retrieving schema: {e}")
            return None, None  # Return None if the table doesn't exist or there's an error

    @staticmethod
    def align_column_types(df: DataFrame, target_schema: StructType) -> DataFrame:
        """
        Aligns column types to match target schema.
        """
        for field in target_schema.fields:
            if field.name in df.columns:
                df = df.withColumn(field.name, df[field.name].cast(field.dataType))
        return df
