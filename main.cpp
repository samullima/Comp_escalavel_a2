#include <iostream>
#include <future>
#include "include/df.h"
#include "include/tratadores.h"
#include "include/csv_extractor.h"
#include "include/sql_extractor.h"
#include "include/threads.h"

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

// Define o ID para cada pool de threads
// Isso é importante para que o pool saiba qual tarefa está sendo executada
enum PoolID {
    READ_TRANSACTIONS = 1,
    READ_ACCOUNTS = 2,
    READ_CUSTOMERS = 3,
    ABNORMAL = 4,
    AMOUNT_MEAN = 5,
    JOIN = 6,
    CLASSIFICATION = 7,
    TOP_CITIES = 8,
    STATS = 9
};

int main() {
    // Número de threads concorrentes do sistema
    const int NUM_THREADS = thread::hardware_concurrency();
    ThreadPool pool(NUM_THREADS);
    cout << "Quantidade de threads concorrentes disponíveis no sistema: " << NUM_THREADS << endl;
    cout << "--------------------------\n\n" << endl;

    // Tipagem das colunas
    vector<string> transactionsColTypes = {"int", "int", "int", "float", "string", "string", "string", "string"};
    vector<string> accountsColTypes = {"int", "int", "float", "string", "string", "string", "string"};
    vector<string> customersColTypes = {"int", "string", "string", "string", "string"};

    // Leitura do arquivo de transações
    auto transactionsTime = chrono::high_resolution_clock::now();
    future<DataFrame*> transactionsFuture = pool.enqueue(READ_TRANSACTIONS, [&]() {
        return readCSV("data/transactions/transactions.csv", NUM_THREADS, transactionsColTypes);
    });
    pool.isReady(READ_TRANSACTIONS);
    DataFrame* transactions = transactionsFuture.get();
    auto transactionsEndTime = chrono::high_resolution_clock::now();
    auto transactionsDuration = chrono::duration_cast<chrono::milliseconds>(transactionsEndTime - transactionsTime);
    cout << "Dataframe de transações lido com sucesso." << endl;

    // Leitura do arquivo de contas
    auto accountsTime = chrono::high_resolution_clock::now();
    future<DataFrame*> accountsFuture = pool.enqueue(READ_ACCOUNTS, [&]() {
        return readCSV("data/accounts/accounts.csv", NUM_THREADS, accountsColTypes);
    });
    pool.isReady(READ_ACCOUNTS);
    DataFrame* accounts = accountsFuture.get();
    auto accountsEndTime = chrono::high_resolution_clock::now();
    auto accountsDuration = chrono::duration_cast<chrono::milliseconds>(accountsEndTime - accountsTime);
    cout << "Dataframe de contas lido com sucesso." << endl;

    // Leitura do arquivo de clientes
    auto customersTime = chrono::high_resolution_clock::now();
    future<DataFrame*> customersFuture = pool.enqueue(READ_CUSTOMERS, [&]() {
        return readCSV("data/customers/customers.csv", NUM_THREADS, customersColTypes);
    });
    pool.isReady(READ_CUSTOMERS);
    DataFrame* customers = customersFuture.get();
    auto customersEndTime = chrono::high_resolution_clock::now();
    auto customersDuration = chrono::duration_cast<chrono::milliseconds>(customersEndTime - customersTime);
    cout << "Dataframe de clientes lido com sucesso." << endl;
    
    cout << "\n--------------------------" << endl;
    cout << "Quantidade de registros por dataframe:\n" << endl;
    cout << "Dataframe de transações: " << transactions->getNumRecords() << " (" << transactionsDuration.count() << "ms)" << endl;
    cout << "Dataframe de contas: " << accounts->getNumRecords() << " (" << accountsDuration.count() << " ms)" << endl;
    cout << "Dataframe de clientes: " << customers->getNumRecords() << " (" << customersDuration.count() << "ms)" << endl;

    cout << "\n\n--------------------------" << endl;
    cout << "[ IDENTIFICANDO TRANSAÇÕES ANÔMALAS ]" << endl;
    // Começa a identificar transações anômalas	
    auto abnormals = pool.enqueue(ABNORMAL, [&]() {
        return abnormal_transactions(*transactions, *accounts, 4, NUM_THREADS, "transation_id", "amount", "location", "account_id", "account_id", "account_location", pool);
    });
    pool.isReady(ABNORMAL);

    cout << "Identificando transações anômalas..." << endl;
    DataFrame abnormal = abnormals.get();
    cout << abnormal.getNumRecords() << "transações anômalas" << endl;

    cout << "\n Você quer ver o dataframe com a classificação das transações anômalas? (y/n)" << endl;
    cout << "(Aviso: esse dataframe pode ser muito grande)" << endl;
    string answer;
    cin >> answer;
    if (answer == "y" || answer == "Y") {
        abnormal.printDF();
    }

    cout << "\n\n--------------------------" << endl;
    cout << "[ CLASSIFICANDO CLIENTES ]" << endl;
    
    auto amountMean = pool.enqueue(AMOUNT_MEAN, [&]() {
        return groupby_mean(*transactions, 5, NUM_THREADS, "account_id", "amount", pool);
    });
    pool.isReady(AMOUNT_MEAN);

    cout << "Agrupando as contas de fazendo a média dos valores..." << endl;
    DataFrame grouped = amountMean.get();

    auto join = pool.enqueue(JOIN, [&](){
        return join_by_key(grouped, *accounts, 6, NUM_THREADS, "account_id", pool);
    });
    pool.isReady(JOIN);

    cout << "Juntando DataFrame agrupado com accounts..." << endl;
    DataFrame dfJoin = join.get();

    auto classifications = pool.enqueue(CLASSIFICATION, [&]() {
        return classify_accounts_parallel(dfJoin, 7, NUM_THREADS, "customer_id", "mean_amount", "current_balance", pool);
    });
    pool.isReady(CLASSIFICATION);

    cout << "Classificando clientes..." << endl;
    DataFrame classified = classifications.get();
    cout << classified.getNumRecords() << "clientes classificados com sucesso." << endl; 

    cout << "\n Você quer ver o dataframe com a classificação dos clientes? (y/n)" << endl;
    cout << "(Aviso: esse dataframe pode ser muito grande)" << endl;
    cin >> answer;
    if (answer == "y" || answer == "Y") {
        classified.printDF();
    }

    cout << "\n\n--------------------------" << endl;
    cout << "[ IDENTIFICANDO LOCALIDADES MAIS ATIVAS ]" << endl;
    auto top_10_cities = pool.enqueue(TOP_CITIES, [&]() {
        return top_10_cidades_transacoes(*transactions, 8, NUM_THREADS, "location", pool);
    });
    pool.isReady(TOP_CITIES);
    DataFrame top_cities = top_10_cities.get();
    cout << top_cities.getNumRecords() << "cidades mais ativas classificadas com sucesso." << endl;
    cout << "\n Você quer ver o dataframe com as 10 cidades mais ativas? (y/n)" << endl;
    cin >> answer;
    if (answer == "y" || answer == "Y") {
        top_cities.printDF();
    }

    cout << "\n\n--------------------------" << endl;
    cout << "[ CALCULANDO ESTATÍSTICAS DESCRITIVAS ]" << endl;
    auto stats = pool.enqueue(STATS, [&]() {
        return summaryStats(*transactions, 9, NUM_THREADS, "amount", pool);
    });
    pool.isReady(STATS);
    DataFrame summary = stats.get();
    cout << summary.getNumRecords() << "estatísticas descritivas calculadas com sucesso." << endl;
    cout << "\n Você quer ver o dataframe com as estatísticas descritivas? (y/n)" << endl;
    cin >> answer;
    if (answer == "y" || answer == "Y") {
        summary.printDF();
    }

    return 0;
}
