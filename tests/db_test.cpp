#include <iostream>
#include <string>
#include <vector>
#include <thread>
#include <chrono>
#include <limits.h>
#include "../include/df.h"
#include "../include/sql_extractor.h"
#include "../include/threads.h"

using namespace std;
using namespace std::chrono;


void benchmarkingDB(int maxThreads)
{
    // Nome do arquivo DB
    string filename = "data/data.db";    
    string tableName = "transactions";
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
            DataFrame * df = readDB(filename, tableName, t, colTypes);
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
    benchmarkDF.DFtoCSV("data/benchmarkingDB");
}

int bah(int argc, char* argv[]) {
    int numThreads = 2;
    // Verifica se o número de threads foi passado como argumento
    if (argc > 1) {
        numThreads = atoi(argv[1]);
        if (numThreads < 2) {
            cout << "Número de threads deve ser maior ou igual a 2. Usando 2 threads." << endl;
            numThreads = 2;
        }
    }
    cout << "Número de threads: " << numThreads << endl;
    ThreadPool pool(numThreads);

    string file1 = "data/data.db";
    string tableName = "transactions";
    vector<string> colTypes = {"int", "int", "int", "float", "string", "string", "string", "string", "string"};
    chrono::high_resolution_clock::time_point start = chrono::high_resolution_clock::now();
    DataFrame *df = readDB(1, file1, tableName, numThreads, colTypes, pool);
    chrono::high_resolution_clock::time_point end = chrono::high_resolution_clock::now();
    auto duration = chrono::duration_cast<chrono::milliseconds>(end - start);
    cout << "Tempo de execução: " << duration.count() << " ms" << endl;
    cout << "Número de colunas: " << df->numCols << endl;
    cout << "Número de registros: " << df->numRecords << endl;

    return 0;
}

int main(int argc, char* argv[]) {
    benchmarkingDB(thread::hardware_concurrency());
    // bah(argc, argv);
    return 0;
}