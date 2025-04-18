#include <thread>
#include <mutex>
#include <sqlite3.h>
#include "include/df.h"
#include "include/threads.h"
#include "include/sql_extractor.h"

using namespace std;

static int callback(void *data, int argc, char **argv, char **azColName) 
{
    // cout << "Callback function called with data: " << static_cast<const char*>(data);
    tuple<vector<vector<string>>*, mutex*, DataFrame*, bool*> coolData = *static_cast<tuple<vector<vector<string>>*, mutex*, DataFrame*, bool*>*>(data);
    vector<vector<string>>* linesRead = get<0>(coolData);
    mutex* mtxDB = get<1>(coolData);
    bool* columnsDone = get<3>(coolData);
    
    // Colocando os nomes das colunas no DataFrame
    if (!(*columnsDone)) {
        DataFrame* df = get<2>(coolData);
        vector<string> colNames(argc);
        cout << "Columns: ";
        for(int i = 0; i < argc; i++) {
            cout << azColName[i] << " ";
            df->colNames[i] = azColName[i];
        }
        *columnsDone = true;
    }

    vector<string> record(argc);
    for(int i = 0; i < argc; i++) {
        if (argv[i]) {
            record[i] = argv[i];
            cout << record[i] << endl;
        } else {
            record[i] = "NULL";
        }
    }
    (*mtxDB).lock();
    (*linesRead).push_back(record);
    (*mtxDB).unlock();
    // df->addRecord(record);

    for(int i = 0; i < argc; i++) {
        cout << azColName[i] << " = " << (argv[i] ? argv[i] : "NULL") << endl;
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
    
    bool columnsDone = false;
    tuple<vector<vector<string>>*, mutex*, DataFrame*, bool*> data = {&linesRead, &mtxDB, df, &columnsDone};
    
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

void processDBLines(const vector<vector<string>>& linesRead, DataFrame* df, int& recordsCount, bool& DBAlreadyRead, mutex& mtxDB, mutex& mtxCounter) {
    /*
    Esse método processa as linhas lidas do CSV e preenche o DataFrame.
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
        // fprintf(stderr, "Can't open database: %s\n", sqlite3_errmsg(db));
        return nullptr;
    } else {
        cerr << "Opened database successfully" << endl;
    }

    /* Create SQL statement */
    sql = "SELECT * FROM " + tableName + ";";

    vector<thread> threads;

    DataFrame df(colTypes, colTypes);
    vector<vector<string>> linesRead;

    bool columnsDone = false;
    bool DBAlreadyRead = false;

    mutex mtxDB;
    mutex mtxCounter;

    threads.emplace_back(extractFromDB, db, sql, &df, ref(DBAlreadyRead), ref(linesRead), ref(mtxDB));

    cout << df.getNumCols() << endl;
    cout << df.colNames[0] << endl;
    cout << df.getNumRecords() << endl;

    int recordsCount = 0;
    for(int i = 1; i < numThreads; i++) {
        threads.emplace_back(processDBLines, ref(linesRead), &df, ref(recordsCount), ref(DBAlreadyRead), ref(mtxDB), ref(mtxCounter));
    }
    for(auto& t : threads) {
        t.join();
    }

    // Processa as linhas lidas do DB


    return nullptr;
}