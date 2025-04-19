#include <thread>
#include <mutex>
#include <sqlite3.h>
#include "include/df.h"
#include "include/threads.h"
#include "include/sql_extractor.h"

using namespace std;

int STORAGE_BLOCKSIZE = 30000;
int PROCESS_BLOCKSIZE = 1000;

static int callback(void *data, int argc, char **argv, char **azColName) 
{
    int blocksize = STORAGE_BLOCKSIZE;

    tuple<vector<vector<string>>*, vector<vector<string>>*, mutex*, DataFrame*, bool*> coolData = *static_cast<tuple<vector<vector<string>>*, vector<vector<string>>*, mutex*, DataFrame*, bool*>*>(data);
    vector<vector<string>>* blockRead = get<1>(coolData);
    mutex* mtxDB = get<2>(coolData);
    bool* columnsDone = get<4>(coolData);
    
    // Colocando os nomes das colunas no DataFrame
    if (!(*columnsDone)) {
        DataFrame* df = get<3>(coolData);
        vector<string> colNames(argc);
        *columnsDone = true;
    }
    
    vector<string> record(argc);
    for(int i = 0; i < argc; i++) {
        if (argv[i]) {
            record[i] = argv[i];
        } else {
            record[i] = "NULL";
        }
    }
    (*blockRead).push_back(record);
    
    if((*blockRead).size() >= blocksize) {
        vector<vector<string>>* linesRead = get<0>(coolData);
        mtxDB->lock();
        linesRead->insert(
            linesRead->end(),
            std::make_move_iterator(blockRead->begin()),
            std::make_move_iterator(blockRead->end())
        );
        blockRead->clear();
        mtxDB->unlock();
    }

    return 0;
}


void extractFromDB(sqlite3 *db, 
    const string& sql, 
    DataFrame *df,
    bool& DBAlreadyRead, 
    vector<vector<string>>& linesRead, 
    mutex& mtxDB
)
{
    char *zErrMsg = 0;
    int rc;
    
    vector<vector<string>> blockRead;
    bool columnsDone = false;
    tuple<vector<vector<string>>*, vector<vector<string>>*, mutex*, DataFrame*, bool*> data = {&linesRead, &blockRead, &mtxDB, df, &columnsDone};
    
    /* Execute SQL statement */
    rc = sqlite3_exec(db, sql.data(), callback, &data, &zErrMsg);
    if( rc != SQLITE_OK ) {
        cerr << "SQL error: " << zErrMsg << endl;
        sqlite3_free(zErrMsg);
    } else {
        cerr << "Operation done successfully" << endl;
    }
    sqlite3_close(db);
    DBAlreadyRead = true;
}

void processDBBlocks(const vector<vector<string>>& linesRead, DataFrame* df, int& recordsCount, bool& DBAlreadyRead, mutex& mtxDB, mutex& mtxCounter) {
    /*
    Esse método processa os blocos lidos do DB e preenche o DataFrame.
    */
    int blocksize = PROCESS_BLOCKSIZE;
    while(!DBAlreadyRead || recordsCount < linesRead.size()){
        bool canProceed = false;
        mtxCounter.lock();
        canProceed = (recordsCount < linesRead.size());
        int firstLine = recordsCount;
        int lastLine = min(recordsCount + blocksize, (int)linesRead.size());
        recordsCount = lastLine;
        mtxCounter.unlock();
        
        if(!canProceed) {
            continue;
        }

        if(!DBAlreadyRead)
            mtxDB.lock();
        vector<vector<string>>::const_iterator first = linesRead.begin() + firstLine;
        vector<vector<string>>::const_iterator last = linesRead.begin() + lastLine;
        vector<vector<string>> blockRead(first, last);
        mtxDB.unlock();
        df->addMultipleRecords(blockRead);
    }
}

void processDBLines(const vector<vector<string>>& linesRead, DataFrame* df, int& recordsCount, bool& DBAlreadyRead, mutex& mtxDB, mutex& mtxCounter) {
    /*
    Esse método processa as linhas lidas do DB e preenche o DataFrame.
    */
   
    while(!DBAlreadyRead || recordsCount < linesRead.size()){
        bool canProceed = false;
        mtxCounter.lock();
        canProceed = (recordsCount < linesRead.size());
        int currentLine = recordsCount;
        recordsCount = recordsCount + canProceed;
        mtxCounter.unlock();
        
        if(!canProceed) {
            continue;
        }

        if(!DBAlreadyRead)
            mtxDB.lock();
        vector<string> line = linesRead[currentLine];
        mtxDB.unlock();
        df->addRecord(line);
    }
}

DataFrame * readDB(const string& filename, string tableName, int numThreads, vector<string> colTypes)
{
    if(numThreads < 2) {
        numThreads = 2;
    }
    
    sqlite3 *db;
    char *zErrMsg = 0;
    int rc;
    string sql;

    /* Open database */
    rc = sqlite3_open(filename.c_str(), &db);
    
    if( rc ) {
        cerr << "Can't open database: " << sqlite3_errmsg(db) << endl;
        return nullptr;
    }

    /* Create SQL statement */
    sql = "SELECT * FROM " + tableName + ";";

    vector<thread> threads;
    vector<string> colNames;
    for(int i = 0; i < colTypes.size(); i++) {
        colNames.push_back("col" + to_string(i));
    }
    DataFrame *df = new DataFrame(colNames, colTypes);
    vector<vector<string>> linesRead;

    bool columnsDone = false;
    bool DBAlreadyRead = false;

    mutex mtxDB;
    mutex mtxCounter;

    threads.emplace_back(extractFromDB, db, sql, df, ref(DBAlreadyRead), ref(linesRead), ref(mtxDB));

    int recordsCount = 0;
    for(int i = 1; i < numThreads; i++) {
        // threads.emplace_back(processDBLines, ref(linesRead), df, ref(recordsCount), ref(DBAlreadyRead), ref(mtxDB), ref(mtxCounter));
        threads.emplace_back(processDBBlocks, ref(linesRead), df, ref(recordsCount), ref(DBAlreadyRead), ref(mtxDB), ref(mtxCounter));
    }
    for(auto& t : threads) {
        t.join();
    }

    // Processa as linhas lidas do DB


    return df;
}