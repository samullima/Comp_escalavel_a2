from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType

def read_csv(spark: SparkSession, path: str, schema: StructType) -> DataFrame:
    return (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .schema(schema)
        .csv(path)
    )
