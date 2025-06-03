from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    stddev,
    avg,
    abs as sql_abs,
    to_date,
    hour,
    count,
    when,
)


def identify_abnormal_transactions(
    transactions: DataFrame, multiplier: float = 4.0
) -> DataFrame:
    stats = (
        transactions.groupBy("account_id")
        .agg(
            avg(col("amount")).alias("mean_amount"),
            stddev(col("amount")).alias("std_amount"),
        )
    )
    joined = transactions.join(stats, on="account_id", how="inner")
    return joined.filter(
        sql_abs(col("amount") - col("mean_amount")) > multiplier * col("std_amount")
    )

def compute_account_mean(transactions: DataFrame) -> DataFrame:
    return transactions.groupBy("account_id").agg(avg(col("amount")).alias("mean_amount"))

def join_with_accounts(account_mean: DataFrame, accounts: DataFrame) -> DataFrame:
    return account_mean.join(accounts.select("account_id", "customer_id", "current_balance"), on="account_id", how="inner")

def classify_accounts(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "classification",
        when(col("mean_amount") > col("current_balance"), "HighSpender").otherwise("Normal"),
    )

def top_10_cities(transactions: DataFrame) -> DataFrame:
    return (
        transactions.groupBy("location")
        .agg(count("*").alias("txn_count"))
        .orderBy(col("txn_count").desc())
        .limit(10)
    )

def summary_stats(transactions: DataFrame) -> DataFrame:
    return transactions.select("amount").describe()

def mean_transactions_per_hour(transactions: DataFrame) -> DataFrame:
    num_days = transactions.select(to_date(col("time_start"))).distinct().count()
    hourly = (
        transactions.withColumn("hour", hour(col("time_start")))
        .groupBy("hour")
        .agg(count("*").alias("txn_count"))
        .withColumn("avg_per_day", col("txn_count") / num_days)
    )
    return hourly.orderBy("hour")
