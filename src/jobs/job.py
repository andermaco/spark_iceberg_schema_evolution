import datetime
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from src.utils import run_query
from src.config.settings import AWSConfig
from src.utils.spark_utils import Utils
from pyspark.sql import SparkSession
from pyspark.sql.types import DateType, StringType, TimestampType
from pyspark.sql.functions import date_format, to_timestamp, lit
from pyspark.storagelevel import StorageLevel
from functools import reduce
import uuid
import pandas as pd
import random

if __name__ == "__main__":    
        
    # Configure AWS connection
    aws_config = AWSConfig()
    
    # Create Spark session
    spark = Utils.create_spark_session("schema_evolution_sample")
    
    
    # Configure AWS Glue Catalog
    Utils.configure_aws_glue_catalog(spark, aws_config.glue_database, aws_config.glue_table)
     
        
    # Create Iceberg table
    # Utils.create_iceberg_table(spark, aws_config.glue_database, aws_config.glue_table, aws_config.s3_path, ["SubscriptionDate"])
     
    # csv_list = ["data/raw/customers-2.csv"]
    csv_list = []
    for i in range(1, 2):
        print(f"processingdata/raw/customers-{i}.csv")
        csv_list.append(f"data/raw/customers-{i}.csv")
    
    try:
        
        #################################
        # Read CSV files and process them
        #################################
        
        # Load all CSV files into a list of DataFrames
        dfs = [spark.read.csv(csv_path, header=True, inferSchema=True) for csv_path in csv_list]
        
        # Persist the DataFrames at disc in order to save memory and avoid multiple reads JUST for development purposes
        [df.persist(StorageLevel.DISK_ONLY) for df in dfs]
                
        # Align schemas
        target_schema = dfs[0].schema
        aligned_dfs = [Utils.align_schema(df, target_schema) for df in dfs]
        
        # Union all received DataFrames
        df_csv = reduce(lambda df1, df2: df1.unionByName(df2, allowMissingColumns=True), aligned_dfs)
        # Persist the DataFrame at disc in order to save memory and avoid multiple reads JUST for development purposes        
        df_csv.persist(StorageLevel.DISK_ONLY)
  
        # Reduce to 1 dataframe
        df_csv_union = reduce(lambda df1, df2: df1.unionByName(df2, allowMissingColumns=True), aligned_dfs)
                      
        # Rename columns: lowercase and remove spaces, due to avoid issues with Glue
        df_csv_union = df_csv_union.toDF(*[col.lower().replace(" ", "_") for col in df_csv_union.columns])
        
        # Reorder columns to put CreatedAt first        
        df_csv_union = df_csv_union.withColumn("created_at", lit(datetime.datetime.now()))
        # df_csv_union = df_csv_union.withColumn("created_at", lit(date_format(datetime.datetime.now(), "yyyy-MM-dd HH:mm:ss.SSSSSS")))
        # # df_csv_union = df_csv_union.withColumn("created_at", 
        # #     date_format(lit(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")), "yyyy-MM-dd HH:mm:ss.SSSSSS"))
        # df_csv_union = df_csv_union.withColumn("created_at_month", date_format(df_csv_union["created_at"], "yyyy-MM"))
        
        # df_csv_union = df_csv_union.withColumn("created_at", lit(datetime.datetime.now() - datetime.timedelta(weeks=random.randint(1, 48))))
        
        # df_csv_union = df_csv_union.withColumn("created_at", lit(datetime.datetime.now()))
        df_csv_union = df_csv_union.withColumn("created_at", 
                                                to_timestamp(date_format(lit(datetime.datetime.now()), "yyyy-MM-dd HH:mm:ss.SSSSSS")))

        # columns = ["created_at"] + ["created_at_month"] + [col for col in df_csv_union.columns if col != "created_at" and col != "created_at_month"]        
        columns = ["created_at"] + [col for col in df_csv_union.columns if col != "created_at"]        
        
        df_csv_union = df_csv_union.select(columns)
        
        
        ##########################################################
        # Read glue table to compare with the csv dataframe schema
        ##########################################################
        
        # Get glue table schema
        schema_aws_glue, df_aws_glue = Utils.get_glue_iceberg_schema(spark, aws_config.glue_database, aws_config.glue_table)
        if schema_aws_glue and df_aws_glue is not None:
            print(schema_aws_glue.fields)
            print(df_aws_glue.printSchema())
        # Add columns from csv dataframe to glue table
        # df_aws_glue = df_aws_glue.join(df_csv_union, on=df_csv_union.columns, how="left")
        
        # schema_union = schema_aws_glue.union(df_csv_union.schema)
        # print(schema_union.fieldNames())
        
        # # # Align csv received DataFrames and current glue table schema
        # # aligned_dfs = [Utils.align_schema(df_csv_union, df_aws_glue) for df in aligned_dfs]  

        # # Align types with existing table schema
        # df_csv_union = Utils.align_column_types(df_csv_union, df_aws_glue)
        
        # # Convert any timestamp columns to proper format
        # for field in df_aws_glue.fields:
        #     if isinstance(field.dataType, TimestampType):
        #         df_csv_union = df_csv_union.withColumn(field.name, 
        #             to_timestamp(df_csv_union[field.name]))
               
        
        # Align common columns types with existing table schema
        # df_csv_union = Utils.align_column_types(df_csv_union, schema_aws_glue)
        
        # # Convert any timestamp columns to proper format
        # for field in schema_aws_glue.fields:
        #     if isinstance(field.dataType, TimestampType):
        #         df_csv_union = df_csv_union.withColumn(field.name, to_timestamp(df_csv_union[field.name]))
    
        
        # Partition columns are just set when table is created
        partition_cols = None if schema_aws_glue and df_aws_glue else ["month(created_at)"] 

        # Write the final DataFrame
        Utils.write_to_s3_glue(df_csv_union, aws_config, partition_cols) 
        # Utils.write_to_s3_glue(df_csv_union, aws_config, ["created_at_month"]) 
                
    except Exception as e:
        print(e)
        raise e
    finally:
        # Marks the DataFrame as non-persistent, and remove all blocks for it from memory and disk
        [df.unpersist() for df in dfs]
        df_csv.unpersist()
        
        spark.stop()