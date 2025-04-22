#include <iostream>
#include <string>
#include <thread>
#include <chrono>
#include <limits.h>
#include "../include/df.h"
#include "../include/csv_extractor.h"
#include "../include/threads.h"

int MAX_NUM_THREADS = 2;

using namespace std;

void benchmarkingCSV(int maxThreads)
{
    // Nome do arquivo CSV
    string filename = "data/transactions/transactions.csv";    

    vector<string> colTypes = {"int", "int", "int", "float", "string", "string", "string", "string", "string"};

    DataFrame benchmarkDF({"numThreads", "meanTime", "minTime", "maxTime"}, {"int", "float", "float", "float"});
    for(int t = 2; t <= maxThreads; t++)
    {
        cout << "Testando com " << t << " threads..." << endl;
        int maxTime = 0;
        int minTime = INT_MAX;
        double meanTime = 0;
        for(int i = 0; i < 10; i++)
        {
            auto start = chrono::high_resolution_clock::now();
            DataFrame * df = readCSV(filename, t, colTypes);
            auto end = chrono::high_resolution_clock::now();
            auto duration = chrono::duration_cast<chrono::milliseconds>(end - start).count();
            meanTime += duration;
            if(duration > maxTime) maxTime = duration;
            if(duration < minTime) minTime = duration;
            delete df;
        }
        meanTime /= 10;
        benchmarkDF.addRecord({to_string(t), to_string(meanTime), to_string(minTime), to_string(maxTime)});
    }
    cout << "Benchmarking concluído." << endl;
    cout << "Resultados do benchmarking:" << endl;
    benchmarkDF.printDF();
    benchmarkDF.DFtoCSV("data/benchmarkingCSV");
}


int main(int argc, char* argv[]) {
    benchmarkingCSV(thread::hardware_concurrency());
    return 0;
}