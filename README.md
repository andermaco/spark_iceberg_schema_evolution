# Spark Schema Evolution Demo

This project demonstrates schema evolution handling in Apache Spark, showing how to manage and process data with changing schemas over time.

## Setup

### Prerequisites
- Python 3.13+
- Apache Spark
- pip

### Installation

1. Install the package in development mode with dev dependencies:
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

4. [Optional] If you are using different python versions at Driver VS Executors, configure your Spark environment:
    ```bash
    export SPARK_HOME=/path/to/your/spark
    export PYTHONPATH=$SPARK_HOME/python:$PYTHONPATH
    ```

## Usage

### Running the Demo

Execute the example job to see schema evolution in action:

```bash
python -m src.jobs.example_job
```

### Running Tests
Run the test suite:
```bash
pytest
```

For test coverage report:
```bash
pytest --cov=src
```


## Features
- Demonstrates handling of schema changes in Parquet files
- Shows how to merge different schema versions
- Includes error handling and schema validation
- Provides utility functions for Spark operations

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
