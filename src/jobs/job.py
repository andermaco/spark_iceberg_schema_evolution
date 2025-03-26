import datetime
from src.config.settings import AWSConfig
from src.utils.spark.spark_utils import SparkUtils
from src.utils.spark.type_utils import TypeUtils
from pyspark.sql.functions import date_format, to_timestamp, lit, coalesce
from pyspark.storagelevel import StorageLevel
from functools import reduce
from pathlib import Path


if __name__ == "__main__":

    # Configure AWS connection
    aws_config = AWSConfig()

    # Create Spark session
    spark = SparkUtils.create_spark_session("schema_evolution_sample")
    # spark.sparkContext.setLogLevel("DEBUG") 

    # Configure AWS Glue Catalog
    SparkUtils.configure_aws_glue_catalog(spark)

    # Iceberg table creation is done automatically at the SparkUtils.write_to_s3_glue function, due
    # to compatibility issues betweeen tables created at athena vs awswrangler, so let awswrangler handle it.
    # Create Iceberg table
    # SparkUtils.create_glue_iceberg_table(spark, aws_config.glue_database, aws_config.glue_table, aws_config.s3_path, ["month(created_at)"])
            
    # Create data/raw directory if it doesn't exist
    raw_dir = Path("data/raw")
    raw_dir.mkdir(parents=True, exist_ok=True)
    
    # Get all CSV files in the data/raw directory
    csv_list = []    
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

        # Persist the DataFrames at disc in order to save memory and avoid multiple reads JUST for development purposes
        [df.persist(StorageLevel.DISK_ONLY) for df in dfs]

        # Align schemas
        target_schema = dfs[0].schema
        aligned_dfs = [SparkUtils.align_schema(df, target_schema) for df in dfs]

        # Reduce and union all source/received DataFrames
        df_csv_union = reduce(lambda df1, df2: df1.unionByName(
            df2, allowMissingColumns=True), aligned_dfs)
        
        # Persist the DataFrame at disc in order to save memory and avoid multiple reads JUST for development purposes
        df_csv_union.persist(StorageLevel.DISK_ONLY)
        
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
                                               to_timestamp(date_format(lit(datetime.datetime.now()), "yyyy-MM-dd HH:mm:ss.SSSSSS")))
        
        # Reorder columns
        columns = ["created_at"] + \
            [col for col in df_csv_union.columns if col != "created_at"]
        df_csv_union = df_csv_union.select(columns)
        
        # Getting string and boolean columns to apply coalesce to them (replacing null with a default value)
        string_cols = [col for col,dtype in df_csv_union.dtypes if dtype == "string"]
        boolean_cols = [col for col,dtype in df_csv_union.dtypes if dtype == "boolean"]
        int_cols = [col for col,dtype in df_csv_union.dtypes if (dtype == "int" or dtype == "bigint")] 
        
        # ## Apply coalesce to all string columns (replacing null with a default value)
        # df_csv_union = df_csv_union.select([
        #     coalesce(df_csv_union[col], lit("Unknown")).alias(col) if col in string_cols else df_csv_union[col].alias(col)
        #     for col in df_csv_union.columns
        # ])
        # ## Apply coalesce to all boolean columns (replacing null with a default 0 value)
        # df_csv_union = df_csv_union.select(
        #     [coalesce(df_csv_union[col], lit(False)).alias(col) if col in boolean_cols else df_csv_union[col] 
        #     for col in df_csv_union.columns])
        
        # ## Apply coalesce to all int columns (replacing null with a default 0 value)
        # df_csv_union = df_csv_union.select(
        #     [coalesce(df_csv_union[col], lit(0)).alias(col) if col in int_cols else df_csv_union[col] 
        #     for col in df_csv_union.columns])
        
        #######################################################
        # Read glue table to cast source df-> target df types #
        #######################################################

        # Get glue table schema
        schema_aws_glue = SparkUtils.get_glue_iceberg_schema(
            spark, aws_config.glue_database, aws_config.glue_table)
        
        # Ensure the schema of the DataFrame matches the target schema
        df_csv_union = SparkUtils.ensure_schema_types_match(
            df_csv_union, aws_config.glue_database, aws_config.glue_table)       
        
        schema_dict = None
        # Ensure the order of columns in the DataFrame matches the target schema
        if schema_aws_glue is not None:
            # Ensure the order of columns in the DataFrame matches the target schema
            df_csv_union = SparkUtils.ensure_iceberg_schema_order_and_types(df_csv_union, schema_aws_glue)
            # StructType to dict in order to be passed to the write_to_s3_glue function
            schema_dict = {field.name: TypeUtils.datatype_to_str(field.dataType) for field in schema_aws_glue}
        
        
        # Partition columns are just set when table is created        
        partition_cols = ["month(created_at)"]
        
        # Write the final DataFrame
        SparkUtils.write_to_s3_glue(df_csv_union, aws_config, partition_cols, schema_dict)        

    except Exception as e:
        print(e)
        raise e
    finally:
        # Marks the DataFrame as non-persistent, and remove all blocks for it from memory and disk
        [df.unpersist() for df in dfs]
        if df_csv_union is not None:
            df_csv_union.unpersist()
        spark.stop()
