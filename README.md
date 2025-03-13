# Spark Schema Evolution Demo

This project demonstrates schema evolution handling in Apache Spark, AWS Glue, AWS s3, and Iceberg, showing how to manage and process data with changing schemas over time.

## Overview

The application processes CSV files containing customer data, handles schema evolution, and writes the data to an Apache Iceberg table in AWS Glue catalog.

## Features

- CSV file processing with schema inference
- Schema evolution support
- Column type alignment
- Timestamp handling
- Partition management
- AWS Glue catalog integration
- S3 storage with Iceberg format

## Key Components

- **AWSConfig**: Handles AWS configuration and validation
- **Utils**: Contains utility functions for:
  - Spark session management
  - AWS Glue catalog configuration
  - Schema comparison and alignment
  - Data type normalization
  - S3 writing with Iceberg format

## Notes

- The application uses Apache Iceberg for table format
- Data is stored in Parquet format with ZSTD compression
- Schema evolution is enabled by default
- Partitioning is done by month(created_at)
- Temporary files are automatically cleaned up  

## Prerequisites

- Python 3.10+
- Apache Spark 3.3+
- AWS Account with appropriate permissions
- Conda for environment management

## Required Dependencies

- python=3.10
- pyspark~=3.3.0
- pandas~=1.3.2
- boto3~=1.36.3
- awswrangler~=3.11.0
- faker~=30.8.1

## The following JAR files need to be placed in `deploy/jar_libraries/`:
- iceberg-spark-runtime-3.3_2.12-1.6.1.jar
- bundle-2.17.161.jar
- url-connection-client-2.17.161.jar


## Installation

1. Install the package in development mode with dev dependencies:
    ```bash
    conda env create -f conda_env.yml
    ```
    or update the environment:
    ```bash
    conda env update -f conda_env.yml
    ```

2. Activate the conda environment:
    ```bash
    conda activate schema_evolution
    ```

3. Install the package in development mode with dev dependencies:
    ```bash
    pip install -e ".[dev]"
    ```

4. I'm using a launch.json file for running or debugging the job in VSCode. Configure AWS credentials and settings in `.vscode/launch.json`:

    ```json
    "env": {                                
                // AWS settings
                "AWS_S3_PATH": "s3://<YOUR_BUCKET>/customers_db/customers_table",
                "GLUE_DATABASE": "customers_db",
                "GLUE_TABLE": "customers_table",
                "AWS_IAM_ROLE": "arn:aws:iam::<YOUR_AWS_ACCOUNT_ID>:role/<YOUR_AWS_ROLE>",
                "WORKGROUP": "<YOUR_WORKGROUP>",
                "WORKGROUP_S3_PATH": "s3://<YOUR_BUCKET>",
                "AWS_REGION": "<YOUR_AWS_REGION>",

                // PySpark & Java Environment variables
                "PYSPARK_SUBMIT_ARGS": "--jars deploy/jar_libraries/iceberg-spark-runtime-3.3_2.12-1.6.1.jar,deploy/jar_libraries/bundle-2.17.161.jar,deploy/jar_libraries/url-connection-client-2.17.161.jar pyspark-shell",
                "JAVA_HOME": "/usr/lib/jvm/java-11-openjdk",
                "SPARK_HOME": "/opt/spark",
                "PYSPARK_PYTHON": "<YOUR_PYTHON_PATH>",
                "PYSPARK_DRIVER_PYTHON": "<YOUR_PYTHON_PATH>"
            }
    ```
    
    [Optional] If you are using different python versions at Driver VS Executors, configure your Spark environment:
    
    ```json
    "env": {                
            ...
            "PYSPARK_PYTHON": "$HOME.conda/envs/schema_evolution/bin/python",
            ...
    }
    ```


## Usage

For testing purposes, you can generate fake data using the src/utils/fake_data.py script or use you own csv files:

1. Place your CSV files in `data/raw/` directory, or **you can generate some example files using the src/utils/fake_data.py script**, you can use them to test the job, even you can add more files to the directory later on to test the schema evolution, take care, **the job will process all the files in the directory**.


    - Generate files 1-20 (default)
        ```bash    
        python src/utils/fake_data.py
        ```
    - Generate specific range (e.g., files 16-20)
        ```bash
        python src/utils/fake_data.py --start 16 --end 20
        ```
    - Specify max records per file
        ```bash
        python src/utils/fake_data.py --start 16 --end 20 --records 5
        ```  

2. Configure AWS settings in `.vscode/launch.json`

3. Now you can run the job in two ways:
    - From VS Code:
        - Press F5 or
        - Click the "Run and Debug" sidebar icon (Ctrl+Shift+D)
        - Select "Schema Evolution Job" from the dropdown
        - Click the green play button or press F5
    - Or use the VS Code Command Palette:
        - Press Ctrl+Shift+P
        - Type "Debug: Select and Start Debugging"
        - Select "Schema Evolution Job"


## Running Tests
Run the test suite:
```bash
pytest
```

For test coverage report:
```bash
pytest --cov=src
```


## Contributing
1. Fork the repository
2. Create your feature branch
3. Commit your changes
4. Push to the branch
5. Create a new Pull Request

## License
[Add your license here]

## Dependencies
The following JAR files need to be downloaded to deploy/jar_libraries/:
- iceberg-spark-runtime-3.3_2.12-1.6.1.jar
- bundle-2.17.161.jar
- url-connection-client-2.17.161.jar

These can be downloaded from Maven Central or AWS SDK repositories.
