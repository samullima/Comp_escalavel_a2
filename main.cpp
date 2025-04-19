#include <iostream>
#include <chrono>

#include "include/df.h"
#include "include/threads.h"
#include "include/csv_extractor.h"
#include "include/tratadores.h"

using namespace std::chrono;

mutex cout_mutex;

int main() {
    string file1 = "data/transactions/transaction_4_2.csv";
    string file2 = "data/transactions/transaction_4_2.csv";
    string file3 = "data/transactions/transactions.csv";

    vector<int> thread_counts = {1, 2, 4, 8, 16};

    for (int num_threads : thread_counts) {
        cout << "\n======================" << endl;
        cout << "   Threads: " << num_threads << endl;
        cout << "======================\n" << endl;

        ThreadPool tp(num_threads);

        auto start = high_resolution_clock::now();
        DataFrame* df1 = readCSV(file1, num_threads, {"int", "int", "int", "float", "string", "string", "string", "string"});
        DataFrame* df2 = readCSV(file2, num_threads, {"int", "int", "int", "float", "string", "string", "string", "string"});
        DataFrame* df = readCSV(file3, num_threads, {"int", "int", "int", "float", "string", "string", "string", "string"});
        auto end = high_resolution_clock::now();

        cout << "[readCSV] Tempo total: "
             << duration_cast<milliseconds>(end - start).count() << " ms" << endl;

        start = high_resolution_clock::now();
        DataFrame joined = join_by_key(*df1, *df2, "account_id", tp);
        end = high_resolution_clock::now();
        cout << "[join_by_key] Tempo: "
             << duration_cast<milliseconds>(end - start).count() << " ms" << endl;

        auto condition = [df](const vector<ElementType>& row) -> bool {
            float amount = get<float>(row[df->getColumnIndex("amount")]);
            string type = get<string>(row[df->getColumnIndex("type")]);
            return amount > 1000 && type == "depósito";
        };

        start = high_resolution_clock::now();
        DataFrame filtered = filter_records(*df, condition, num_threads, tp);
        end = high_resolution_clock::now();
        cout << "[filter_records] Tempo: "
             << duration_cast<milliseconds>(end - start).count() << " ms" << endl;

        start = high_resolution_clock::now();
        DataFrame grouped = groupby_mean(*df, "location", "amount", tp);
        end = high_resolution_clock::now();
        cout << "[groupby_mean] Tempo: "
             << duration_cast<milliseconds>(end - start).count() << " ms" << endl;

        if (num_threads == thread_counts.back()) {
            joined.DFtoCSV("joined" + num_threads);
            filtered.DFtoCSV("filtered" + num_threads);
            grouped.DFtoCSV("grouped" + num_threads);
        }

        delete df1;
        delete df2;
        delete df;
    }

    return 0;
}