import datetime
from src.config.settings import AWSConfig
from src.utils.spark_utils import Utils
from pyspark.sql.functions import date_format, to_timestamp, lit, coalesce
from pyspark.storagelevel import StorageLevel
from functools import reduce
import os
from pathlib import Path


if __name__ == "__main__":

    # Configure AWS connection
    aws_config = AWSConfig()

    # Create Spark session
    spark = Utils.create_spark_session("schema_evolution_sample")

    # Configure AWS Glue Catalog
    Utils.configure_aws_glue_catalog(spark)

    # Create Iceberg table
    # Utils.create_iceberg_table(spark, aws_config.glue_database, aws_config.glue_table, aws_config.s3_path, ["SubscriptionDate"])

    # # csv_list = ["data/raw/customers-2.csv"]
    csv_list = []
    # for i in range(16, 21):
    #     print(f"processingdata/raw/customers-{i}.csv")
    #     csv_list.append(f"data/raw/customers-{i}.csv")
        
    # Create data/raw directory if it doesn't exist
    raw_dir = Path("data/raw")
    raw_dir.mkdir(parents=True, exist_ok=True)
    
    # Get all CSV files in the data/raw directory
    for f in raw_dir.glob("*.csv"):
        csv_list.append(str(f))
    if csv_list == []:
        raise ValueError("No CSV files found in the data/raw directory")
    
    df_csv_union = None
    
    try:      
        #########################################
        # Read sorce CSV files and process them #
        #########################################

        # Load all CSV files into a list of DataFrames
        dfs = [spark.read.csv(csv_path, header=True, inferSchema=True)
               for csv_path in csv_list]

        # # Persist the DataFrames at disc in order to save memory and avoid multiple reads JUST for development purposes
        # [df.persist(StorageLevel.DISK_ONLY) for df in dfs]

        # Align schemas
        target_schema = dfs[0].schema
        aligned_dfs = [Utils.align_schema(df, target_schema) for df in dfs]

        # Reduce and union all source/received DataFrames
        df_csv_union = reduce(lambda df1, df2: df1.unionByName(
            df2, allowMissingColumns=True), aligned_dfs)

        # # Rename columns: lowercase and remove spaces, due to avoid issues with Glue
        # df_csv_union = df_csv_union.toDF(
        #     *[col.lower().replace(" ", "_") for col in df_csv_union.columns])
        
        # Rename columns: lowercase, remove spaces and special characters for Glue/Iceberg compatibility
        df_csv_union = df_csv_union.toDF(
            *[col.lower()
            .replace(" ", "_")
            .replace("-", "_")
            .replace("(", "")
            .replace(")", "")
            for col in df_csv_union.columns]
        )

        # Add created_at timestamp and reorder columns
        df_csv_union = df_csv_union.withColumn("created_at",
                                               to_timestamp((lit(datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")))))
        columns = ["created_at"] + \
            [col for col in df_csv_union.columns if col != "created_at"]
        df_csv_union = df_csv_union.select(columns)
        
        # Getting string and boolean columns to apply coalesce to them (replacing null with a default value)
        string_cols = [col for col,dtype in df_csv_union.dtypes if dtype == "string"]
        boolean_cols = [col for col,dtype in df_csv_union.dtypes if dtype == "boolean"]        
        ## Apply coalesce to all string columns (replacing null with a default value)
        df_csv_union = df_csv_union.select([
            coalesce(df_csv_union[col], lit("Unknown")).alias(col) if col in string_cols else df_csv_union[col].alias(col)
            for col in df_csv_union.columns
        ])
        ## Apply coalesce to all boolean columns (replacing null with a default 0 value)
        df_csv_union = df_csv_union.select(
            [coalesce(df_csv_union[col], lit(False)).alias(col) if col in boolean_cols else df_csv_union[col] 
            for col in df_csv_union.columns])
        
        ########################################################
        # Read glue table to cast df source -> df target types #
        ########################################################

        # Get glue table schema
        schema_aws_glue, df_aws_glue = Utils.get_glue_iceberg_schema(
            spark, aws_config.glue_database, aws_config.glue_table)

        # Cast common columns to schema_aws_glue columns types
        if schema_aws_glue and df_aws_glue is not None:
            df_csv_union = Utils.align_column_types(
                df_csv_union, schema_aws_glue)

        # Persist the DataFrame at disc in order to save memory and avoid multiple reads JUST for development purposes
        df_csv_union.persist(StorageLevel.DISK_ONLY)

        # Partition columns are just set when table is created
        partition_cols = None if schema_aws_glue and df_aws_glue else [
            "month(created_at)"]



        # Write the final DataFrame
        Utils.write_to_s3_glue(df_csv_union, aws_config, partition_cols)

    except Exception as e:
        print(e)
        raise e
    finally:
        # Marks the DataFrame as non-persistent, and remove all blocks for it from memory and disk
        [df.unpersist() for df in dfs]
        # if df_csv_union is not None:
        # df_csv_union.unpersist()
        spark.stop()
