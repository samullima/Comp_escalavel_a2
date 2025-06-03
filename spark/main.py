import multiprocessing
import time
from pyspark.sql import SparkSession

from schemas import accounts_schema, customers_schema, transactions_schema
from loader import read_csv
from functions import (
    identify_abnormal_transactions,
    compute_account_mean,
    join_with_accounts,
    classify_accounts,
    top_10_cities,
    summary_stats,
    mean_transactions_per_hour,
)

if __name__ == "__main__":
    
    num_cores: int = multiprocessing.cpu_count()
    
    spark = (
        SparkSession.builder
        .appName("PySparkPipeline")
        .master(f"local[{num_cores}]")                  # use all cores
        .config("spark.sql.shuffle.partitions", str(num_cores))
        .getOrCreate()
    )
    
    start = time.time()

    tx_path = "data/transactions/transactions.csv"
    acc_path = "data/accounts/accounts.csv"
    cust_path = "data/customers/customers.csv"

    t0 = time.time()
    transactions = read_csv(spark, tx_path, transactions_schema)
    t1 = time.time()

    accounts = read_csv(spark, acc_path, accounts_schema)
    t2 = time.time()

    customers = read_csv(spark, cust_path, customers_schema)
    t3 = time.time()

    print(f"Transactions loaded: {transactions.count()} rows ({int((t1-t0)*1000)} ms)")
    print(f"Accounts loaded: {accounts.count()} rows ({int((t2-t1)*1000)} ms)")
    print(f"Customers loaded: {customers.count()} rows ({int((t3-t2)*1000)} ms)\n")

    # Identify abnormal transactions (threshold = 4 stddevs)
    t4 = time.time()
    abnormal_df = identify_abnormal_transactions(transactions)
    t5 = time.time()
    abnormal_df.write.mode("overwrite").csv("output/abnormal_transactions", header=True)
    print(f"Abnormal: {abnormal_df.count()} rows ({int((t5-t4)*1000)} ms)")

    # Compute mean per account and classify
    t6 = time.time()
    account_mean = compute_account_mean(transactions)
    joined_acc = join_with_accounts(account_mean, accounts)
    classified_acc = classify_accounts(joined_acc)
    t7 = time.time()
    classified_acc.write.mode("overwrite").csv("output/classified_accounts", header=True)
    print(f"Classified accounts: {classified_acc.count()} rows ({int((t7-t6)*1000)} ms)")

    # Top 10 cities by transaction count
    t8 = time.time()
    top_cities_df = top_10_cities(transactions)
    t9 = time.time()
    top_cities_df.write.mode("overwrite").csv("output/top_10_cities", header=True)
    print(f"Top cities: {top_cities_df.count()} rows ({int((t9-t8)*1000)} ms)")

    # Summary statistics on amount
    t10 = time.time()
    summary_df = summary_stats(transactions)
    t11 = time.time()
    summary_df.write.mode("overwrite").csv("output/summary_stats", header=True)
    print(f"Summary stats: {summary_df.count()} rows ({int((t11-t10)*1000)} ms)")

    # Mean transactions per hour
    t12 = time.time()
    hourly_df = mean_transactions_per_hour(transactions)
    t13 = time.time()
    hourly_df.write.mode("overwrite").csv("output/num_transactions_per_hour", header=True)
    print(f"Hourly avg: {hourly_df.count()} rows ({int((t13-t12)*1000)} ms)\n")

    total = time.time() - start
    print(f"Total elapsed: {int(total*1000)} ms")
    spark.stop()
