#include <iostream>
#include <future>
#include "../include/df.h"
#include "../include/tratadores.h"
#include "../include/csv_extractor.h"
#include "../include/sql_extractor.h"
#include "../include/threads.h"

using namespace std;

// transactions
// transation_id,account_id,recipient_id,amount,type,time_start,location,processed_at
// 0,1,9810,4467.93,retirada,13:10:01.847319,Abbottchester,13:10:49.847319

// accounts
// customer_id,account_id,current_balance,account_type,opening_date,account_status,account_location
// 0,0,11958.77,poupança,2025-03-21,desbloqueada,Maceió

// customers
// customer_id,name,email,address,phone
// 0,Christopher West,rachelshannon@example.org,"1626 Miranda Canyon Suite 013 - New Michael, FL 05173",78600532445

enum PoolID {
    READ_TRANSACTIONS = 1,
    READ_ACCOUNTS = 2,
    READ_CUSTOMERS = 3,
    ABNORMAL = 4
};

int main() {
    const int NUM_THREADS = thread::hardware_concurrency();
    ThreadPool pool(NUM_THREADS);
    vector<string> transactionsColTypes = {"int", "int", "int", "float", "string", "string", "string", "string"};
    vector<string> accountsColTypes = {"int", "int", "float", "string", "string", "string", "string"};
    vector<string> customersColTypes = {"int", "string", "string", "string", "string"};

    future<DataFrame*> transactionsFuture = pool.enqueue(READ_TRANSACTIONS, [&]() {
        return readCSV("data/transactions/transactions.csv", NUM_THREADS, transactionsColTypes);
    });
    pool.isReady(READ_TRANSACTIONS);
    future<DataFrame*> accountsFuture = pool.enqueue(READ_ACCOUNTS, [&]() {
        return readCSV("data/accounts/accounts.csv", NUM_THREADS, accountsColTypes);
    });
    pool.isReady(READ_ACCOUNTS);
    future<DataFrame*> customersFuture = pool.enqueue(READ_CUSTOMERS, [&]() {
        return readCSV("data/customers/customers.csv", NUM_THREADS, customersColTypes);
    });
    pool.isReady(READ_CUSTOMERS);


    DataFrame* transactions = transactionsFuture.get();
    DataFrame* accounts = accountsFuture.get();
    DataFrame* customers = customersFuture.get();
    
    
    cout << transactions->getNumRecords() << endl;
    cout << accounts->getNumRecords() << endl;
    cout << customers->getNumRecords() << endl;

    auto abnormals = pool.enqueue(ABNORMAL, [&]() {
        return abnormal_transactions(*transactions, *accounts, 4, NUM_THREADS, "transation_id", "amount", "location", "account_id", "account_id", "account_location", pool);
    });
    pool.isReady(ABNORMAL);

    DataFrame abnormal = abnormals.get();
    cout << "Abnormal transactions: " << abnormal.getNumRecords() << endl;
    abnormal.printDF();

    return 0;
}
