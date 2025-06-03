from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, FloatType, StringType, DateType
)

accounts_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("account_id", IntegerType(), True),
    StructField("current_balance", FloatType(), True),
    StructField("account_type", StringType(), True),
    StructField("opening_date", DateType(), True),
    StructField("account_status", StringType(), True),
    StructField("account_location", StringType(), True),
])

customers_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("address", StringType(), True),
    StructField("phone", StringType(), True),
])

transactions_schema = StructType([
    StructField("transaction_id", IntegerType(), True),
    StructField("account_id", IntegerType(), True),
    StructField("recipient_id", IntegerType(), True),
    StructField("amount", FloatType(), True),
    StructField("type", StringType(), True),
    StructField("location", StringType(), True),
    StructField("time_start", StringType(), True),
    StructField("time_end", StringType(), True),
    StructField("date", DateType(), True),
])
