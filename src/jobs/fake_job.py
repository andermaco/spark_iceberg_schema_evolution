import os
import sys
from pyspark.sql import SparkSession

# Configura las variables de entorno
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.environ['JAVA_HOME'] = '/usr/lib/jvm/java-11-openjdk'  # Tu JAVA_HOME actual
os.environ['SPARK_HOME'] = '/opt/spark'  # Tu SPARK_HOME actual

# Crea la SparkSession con configuración específica para tu entorno
spark = SparkSession.builder \
    .appName("MyApp") \
    .getOrCreate()
    
data = [("Alice", 1)]
schema = ["name", "id"]
df = spark.createDataFrame(data, schema)
df.show()

# Prueba simple usando RDD en lugar de DataFrame directamente
# rdd = spark.sparkContext.parallelize([("Alice", 1)])
# df = rdd.toDF(["name", "id"])
# df.show()

# Cierra la sesión
spark.stop()
    