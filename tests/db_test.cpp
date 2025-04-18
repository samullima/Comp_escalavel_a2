#include <iostream>
#include <string>
#include <vector>
#include <thread>
#include "include/df.h"
#include "include/sql_extractor.h"

int main()
{
    string file1 = "data/data.db";
    string tableName = "transactions";
    int numThreads = 4;
    vector<string> colTypes = {"int", "int", "int", "float", "string", "string", "string", "string"};

    DataFrame *df = readDB(file1, tableName, numThreads, colTypes);
    
    if (df) {
        df->printDF();
    }

    return 0;
}