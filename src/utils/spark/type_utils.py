from pyspark.sql.types import DataType, StringType, IntegerType, LongType, FloatType, DoubleType, BooleanType, TimestampType, DateType, DecimalType, BinaryType, ArrayType, MapType, StructType
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, DataType, StringType
from pyspark.sql.types import TimestampType
import awswrangler as wr



class TypeUtils:
        
    @staticmethod
    def convert_datetime_columns(df: DataFrame) -> DataFrame:
        """
        Converts datetime columns to TimestampType.
        Args:
        df: The DataFrame to convert.
        Returns:
        A new DataFrame with the converted datetime columns.
        """
        for col_name, data_type in df.dtypes:
            if "date" in data_type.lower():  # Check for date or timestamp
                df = df.withColumn(
                col_name, df[col_name].cast(TimestampType()))

        print(df.dtypes)
        print(df['SubscriptionDate'].dtype)

        return df
    
    
    @staticmethod
    def datatype_to_str(data_type: DataType) -> str:
        """
        Maps PySpark data types to Athena/Iceberg data types.

        Args:
        data_type (DataType): The PySpark data type.

        Returns:
        str: The corresponding Athena/Iceberg data type.
        """
        if isinstance(data_type, StringType):
                return "string"
        elif isinstance(data_type, IntegerType):
                return "int"
        elif isinstance(data_type, LongType):
                return "bigint"
        elif isinstance(data_type, FloatType):
                return "float"
        elif isinstance(data_type, DoubleType):
                return "double"
        elif isinstance(data_type, BooleanType):
                return "boolean"
        elif isinstance(data_type, TimestampType):
                return "timestamp"
        elif isinstance(data_type, DateType):
                return "date"
        elif isinstance(data_type, DecimalType):
                return f"decimal({data_type.precision}, {data_type.scale})"
        elif isinstance(data_type, BinaryType):
                return "binary"
        elif isinstance(data_type, ArrayType):
                element_type = SparkUtils.cast_pyspark_type_to_athena_iceberg_type(data_type.elementType)
                return f"array<{element_type}>"
        elif isinstance(data_type, MapType):
                key_type = SparkUtils.cast_pyspark_type_to_athena_iceberg_type(data_type.keyType)
                value_type = SparkUtils.cast_pyspark_type_to_athena_iceberg_type(data_type.valueType)
                return f"map<{key_type}, {value_type}>"
        elif isinstance(data_type, StructType):
                fields = []
                for field in data_type.fields:
                        field_type = SparkUtils.cast_pyspark_type_to_athena_iceberg_type(field.dataType)
                        fields.append(f"{field.name}:{field_type}")
                return f"struct<{','.join(fields)}>"
        else:
                raise ValueError(f"Unsupported PySpark data type: {data_type}")
