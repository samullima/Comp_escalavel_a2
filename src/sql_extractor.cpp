#include <thread>
#include <mutex>
#include "../include/sqlite3.h"
#include "../include/df.h"
#include "../include/threads.h"
#include "../include/sql_extractor.h"

using namespace std;

int STORAGE_BLOCKSIZE = 30000;
int PROCESS_BLOCKSIZE = 1000;

using holyTuple = tuple<vector<vector<string>>*, vector<vector<string>>*, mutex*, DataFrame*, bool*>;
vector<vector<string>>* getLinesRead(holyTuple& data) {
    return get<0>(data);
}
vector<vector<string>>* getBlockRead(holyTuple& data) {
    return get<1>(data);
}
mutex* getMtxDB(holyTuple& data) {
    return get<2>(data);
}
DataFrame* getDF(holyTuple& data) {
    return get<3>(data);
}
bool* getColumnsDone(holyTuple& data) {
    return get<4>(data);
}

static int callback(void *data, int argc, char **argv, char **azColName) 
{
    int blocksize = STORAGE_BLOCKSIZE;

    holyTuple coolData = *static_cast<holyTuple*>(data);
    vector<vector<string>>* blockRead = getBlockRead(coolData);
    mutex* mtxDB = getMtxDB(coolData);
    bool* columnsDone = getColumnsDone(coolData);
    
    // Colocando os nomes das colunas no DataFrame
    if (!(*columnsDone)) {
        DataFrame* df = getDF(coolData);
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
        vector<vector<string>>* linesRead = getLinesRead(coolData);
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
    holyTuple data = {&linesRead, &blockRead, &mtxDB, df, &columnsDone};
    
    /* Execute SQL statement */
    rc = sqlite3_exec(db, sql.data(), callback, &data, &zErrMsg);
    if( rc != SQLITE_OK ) {
        cerr << "SQL error: " << zErrMsg << endl;
        sqlite3_free(zErrMsg);
    }

    mtxDB.lock();
    if(!blockRead.empty()) {
        linesRead.insert(
            linesRead.end(),
            std::make_move_iterator(blockRead.begin()),
            std::make_move_iterator(blockRead.end())
        );
        blockRead.clear();
    }
    mtxDB.unlock();
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

DataFrame* readDB(const string& filename, string tableName, int numThreads, vector<string> colTypes) {
    if (numThreads < 2) numThreads = 2;

    sqlite3* db;
    int rc = sqlite3_open(filename.c_str(), &db);
    if (rc) {
        cerr << "Can't open database: " << sqlite3_errmsg(db) << endl;
        return nullptr;
    }

    string sql = "SELECT * FROM " + tableName + ";";

    vector<string> colNames;
    for (int i = 0; i < colTypes.size(); ++i)
        colNames.push_back("col" + to_string(i));

    auto* df = new DataFrame(colNames, colTypes);
    vector<vector<string>> linesRead;
    bool DBAlreadyRead = false;
    mutex mtxDB, mtxCounter;
    int recordsCount = 0;

    ThreadPool pool(numThreads);

    // Task 0: leitura do banco
    pool.enqueue(0,
        [df, &DBAlreadyRead, &linesRead, &mtxDB, sql, db]() mutable {
            extractFromDB(db, sql, df, DBAlreadyRead, linesRead, mtxDB);
        });

    // Tasks 1...n-1: processadores
    for (int i = 1; i < numThreads; ++i) {
        pool.enqueue(i,
            [df, &linesRead, &recordsCount, &DBAlreadyRead, &mtxDB, &mtxCounter]() mutable {
                processDBBlocks(linesRead, df, recordsCount, DBAlreadyRead, mtxDB, mtxCounter);
            },
            {0}); // depende da task 0
    }

    // Aguarda todas terminarem
    while (pool.size() > 0 && pool.getActiveThreads() > 0) {
        this_thread::sleep_for(chrono::milliseconds(10));
    }

    return df;
}
