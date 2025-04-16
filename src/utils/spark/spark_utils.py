# from narwhals import DataFrame
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, DataType, StringType
from pyspark.sql.functions import lit
from dataclasses import dataclass
from typing import Tuple, Set, Dict
from pyspark.sql.types import TimestampType
from src.config.settings import AWSConfig
import awswrangler as wr
import boto3
import pandas as pd
from src.utils.spark.type_utils import TypeUtils



@dataclass
class SparkUtils():

    @staticmethod
    def create_spark_session(app_name: str) -> SparkSession:
        """
        Create a SparkSession with the given application name.
        Args:
            app_name: The name of the application.
        Returns:
            A SparkSession.
        """
        return SparkSession.builder \
            .appName(app_name) \
            .getOrCreate()


    @staticmethod
    def get_boto3_session() -> boto3.Session:
        """
        Returns a boto3 session with temporary credentials.
        """
        return boto3.Session()


    @staticmethod
    def configure_aws_glue_catalog(spark: SparkSession) -> None:
        """
        Configure the AWS Glue Catalog and S3 for the Spark session.
        Args:
            spark: The SparkSession.
        """
        spark.conf.set("spark.sql.catalog.AwsGlueCatalog",
                       "org.apache.iceberg.spark.SparkCatalog")
        spark.conf.set("spark.sql.catalog.AwsGlueCatalog.catalog-impl",
                       "org.apache.iceberg.aws.glue.GlueCatalog")
        spark.conf.set("spark.sql.catalog.AwsGlueCatalog.warehouse",
                       "s3a://bd-datawarehouse/")
        spark.conf.set("spark.sql.catalog.AwsGlueCatalog.io-impl",
                       "org.apache.iceberg.aws.s3.S3FileIO")
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        

    # @staticmethod
    # def compare_schemas(schema1: StructType, schema2: StructType) -> Tuple[Set[str], Set[str], Dict[str, Tuple[DataType, DataType]]]:
    #     """
    #     Compara dos esquemas y devuelve:
    #     - Conjunto de campos que faltan en schema1 respecto a schema2.
    #     - Conjunto de campos que faltan en schema2 respecto a schema1.
    #     - Diferencias de tipos para campos comunes.
    #     """
    #     fields1 = {f.name: f.dataType for f in schema1.fields}
    #     fields2 = {f.name: f.dataType for f in schema2.fields}

    #     missing_in_1 = set(fields2.keys()) - set(fields1.keys())
    #     missing_in_2 = set(fields1.keys()) - set(fields2.keys())

    #     diff_types = {
    #         name: (fields1[name], fields2[name])
    #         for name in set(fields1.keys()) & set(fields2.keys())
    #         if fields1[name] != fields2[name]
    #     }

    #     return missing_in_1, missing_in_2, diff_types

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
                aligned_fields.append(new_df[target_field.name].cast(
                    target_field.dataType).alias(target_field.name))
            else:
                # If the column is missing, add a null column with the target type
                aligned_fields.append(lit(None).cast(
                    target_field.dataType).alias(target_field.name))

        # Add any extra fields from the new DataFrame that are not in the target schema.
        for new_field in new_fields:
            if new_field.name not in target_schema.fieldNames():
                aligned_fields.append(new_df[new_field.name])

        return new_df.select(*aligned_fields)

    @staticmethod
    def write_to_s3_glue(df: pd.DataFrame, aws_config: AWSConfig, partition_cols: list, schema_dict: dict = None) -> None:
        """
        Write DataFrame to S3 and create/update Glue catalog table using specified IAM role
        Args:
            df: The PandasDataFrame to write.
            aws_config: The AWS configuration.
            partition_cols: The partition columns.
        """
        # Convert to pandas and handle schema evolution
        pandas_df = df.toPandas()

        # Write to table using awswrangler
        wr.athena.to_iceberg(
            df=pandas_df,
            database=aws_config.glue_database,
            table=aws_config.glue_table,
            temp_path="s3://bd-test-tq-wg/temp/",
            table_location="s3://bd-datawarehouse/customers_db/customers_table/",
            workgroup=aws_config.workgroup,
            catalog_id=AWSConfig().catalog_id,
            schema_evolution=True,
            keep_files=False,
            fill_missing_columns_in_df=True,
            partition_cols=partition_cols,
            dtype=schema_dict,
            boto3_session=AWSConfig().session
        )

    @staticmethod
    def create_glue_database(spark: SparkSession, database_name: str, bucket_path: str):
        """
        Creates a Glue database in AWS Glue Catalog.
        """
        spark.sql(f"""
            CREATE DATABASE IF NOT EXISTS AwsGlueCatalog.{database_name}
            LOCATION '{bucket_path}'
        """)


    @staticmethod
    def create_glue_iceberg_table(spark: SparkSession, database_name: str, table_name: str,
                             bucket_path: str, partition_cols: list):
        """
        Creates an Iceberg table in AWS Glue Catalog.
        """
        spark.sql(f"""    
            CREATE TABLE IF NOT EXISTS AwsGlueCatalog.{database_name}.{table_name} (
                created_at timestamp
            )                        
            PARTITIONED BY ({', '.join([f'{col}' for col in partition_cols]) if partition_cols else ''})
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
        
    @staticmethod
    def get_glue_iceberg_schema(spark: SparkSession, glue_db: str, glue_table: str) -> StructType:
        """
        Retrieves the schema and DataFrame of an Iceberg table from AWS Glue Catalog.
        Args:
            spark: The SparkSession.
            glue_db: The Glue database.
            glue_table: The Glue table.
        Returns:
            A tuple containing the schema and DataFrame.
        """
        try:
            df = spark.read.format("iceberg").load(
                f"AwsGlueCatalog.{glue_db}.{glue_table}")
            return df.schema
        except Exception as e:
            print(f"Error retrieving schema: {e}")
            return None  # Return None if the table doesn't exist or there's an error
    
    @staticmethod
    def ensure_schema_types_match(df: DataFrame, database: str, table: str) -> DataFrame:
        """
        Ensures the schema of a DataFrame matches a target schema.
        """
        iceberg_schema = SparkUtils.get_iceberg_schema(database, table)
        if iceberg_schema is None:
            return df
        # Ensure DataFrame matches the schema     
        for field in df.columns:
            if field in iceberg_schema.keys():
                df = df.withColumn(
                    field, df[field].cast(iceberg_schema[field]))
            else:                
                df = df.withColumn(
                    field, lit(df[field]).cast(TypeUtils().datatype_to_str(df.schema[field].dataType)))
        return df
    
    @staticmethod
    def ensure_iceberg_schema_order_and_types(df: DataFrame, schema: StructType) -> DataFrame:
        """
        Ensures the order of columns in a DataFrame matches a target schema.
        Adds missing columns as null values if they are not present in the DataFrame.
        
        :param df: Input DataFrame
        :param schema: Target schema (StructType) to enforce
        :return: DataFrame with ordered columns matching the schema
        """
        # Extract existing columns from the DataFrame avoiding duplicates
        df_columns = set(df.columns)
                        
        # Ensure all schema columns exist in the DataFrame, adding missing ones as nulls
        for field in schema:
            if field.name not in df_columns:
                df = df.withColumn(field.name, lit(None).cast(field.dataType))
                        
        # Get those columns not in the schema
        missing_columns = df_columns - set(schema.fieldNames())
 
        # Reorder DataFrame to match schema
        # return df.select([field.name for field in schema])
        return df.select([field.name for field in schema] + [df[col] for col in missing_columns])
    
    @staticmethod
    def get_iceberg_schema(database: str, table: str) -> dict:
        """
        Retrieves the schema of an Iceberg table from AWS Glue Catalog.
        """
        return wr.catalog.get_table_types(database=database, table=table, boto3_session=AWSConfig().session)
    
