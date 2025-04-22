#include <iostream>
#include "../include/df.h"
#include "../include/tratadores.h"
#include "../include/csv_extractor.h"
#include "../include/threads.h"

using namespace std;

mutex cout_mutex;

int main() {
    vector<string> colNames = {"ID", "Nome", "Salario"};
    vector<string> colTypes = {"int", "string", "float"};
    DataFrame df(colNames, colTypes);

    df.addRecord({"1", "Camacho", "5000.5"});
    df.addRecord({"2", "Bebel", "6200.0"});
    df.addRecord({"3", "Yuri", "4700.75"});

    cout << "\nDataFrame original:\n";
    df.printDF();

    vector<ElementType> isHighSalary = {true, true, false};
    string newColName = "Rico?";
    string newColType = "bool";
    df.addColumn(isHighSalary, newColName, newColType);

    cout << "\nDataFrame com coluna bool adicionada:\n";
    df.printDF();

    cout << "\n--- Metadados do DataFrame ---\n";
    cout << "Num de registros: " << df.numRecords << endl;
    cout << "Num de colunas: " << df.numCols << endl;

    cout << "\nNomes das colunas:\n";
    for (const auto& name : df.colNames)
        cout << "- " << name << endl;

    cout << "\nIdx das colunas:\n";
    for (const auto& [name, idx] : df.idxColumns)
        cout << "- " << name << ": " << idx << endl;

    cout << "\nTipos das colunas:\n";
    for (const auto& [name, type] : df.colTypes)
        cout << "- " << name << ": " << type << endl;

    ////// TRATADORES ///////
    const int NUM_THREADS = thread::hardware_concurrency();
    ThreadPool pool(NUM_THREADS);
    
    // Teste para filtrar registros
    int idFilter = 1;
    auto cond = [&](const vector<ElementType>& row) -> bool {
        return get<float>(row[2]) > 5000.0f;
    };

    // Teste para o groupby
    int idGroupBy = 2;
    string groupCol = "account_id";
    string targetCol = "amount";
    vector<string> colNames_2 = {groupCol, targetCol};
    vector<string> colTypes_2 = {"int", "float"};
    DataFrame df_2(colNames_2, colTypes_2);

    df_2.addRecord({"1", "100.0"});
    df_2.addRecord({"2", "200.0"});
    df_2.addRecord({"1", "300.0"});
    df_2.addRecord({"2", "400.0"});
    df_2.addRecord({"3", "150.0"});

    // Teste para join
    int idJoin = 3;
    string keyCol = "id";
    vector<string> colNames_3 = {keyCol, "nome"};
    vector<string> colTypes_3 = {"int", "string"};
    DataFrame df_3(colNames_3, colTypes_3);
    df_3.addRecord({"1", "Alice"});
    df_3.addRecord({"2", "Bob"});
    df_3.addRecord({"3", "Carlos"});

    vector<string> colNames_4 = {keyCol, "idade"};
    vector<string> colTypes_4 = {"int", "int"};
    DataFrame df_4(colNames_4, colTypes_4);
    df_4.addRecord({"1", "23"});
    df_4.addRecord({"2", "30"});
    df_4.addRecord({"4", "40"});

    // Teste para get_hour_by_time e count_values
    int idGetHour = 4;
    int idCount = 6;
    string timeCol = "time_start";
    vector<string> colName_ = {timeCol};
    vector<string> colType_ = {"string"};
    DataFrame df_5(colName_, colType_);

    df_5.addRecord({"01:46:23"});
    df_5.addRecord({"02:35:22"});
    df_5.addRecord({"01:45:06"});
    df_5.addRecord({"02:18:33"});
    df_5.addRecord({"03:23:59"});
    df_5.addRecord({"02:44:33"});
    df_5.addRecord({"02:56:00"});

    // Teste para abnormal_transactions
    int idAbnormal = 5;
    string transacCol = "transaction_id";
    string accountCol = "account_id";
    string amountCol = "amount";
    string locationCol = "location";

    // DF transactions
    vector<string> colName__t = {transacCol, accountCol, amountCol, locationCol};
    vector<string> colType__t = {"int", "int", "float", "string"};
    DataFrame df_6(colName__t, colType__t);

    // DF account
    vector<string> colName__a = {accountCol, locationCol};
    vector<string> colType__a = {"int", "string"};
    DataFrame df_7(colName__a, colType__a);

    df_6.addRecord({"1", "10", "0.8", "Guzmantown"});
    df_6.addRecord({"2", "20", "200", "Josephfort"});
    df_6.addRecord({"3", "10", "500", "Guzmantown"});
    df_6.addRecord({"4", "20", "700", "Josephfort"});
    df_6.addRecord({"5", "20", "200", "Marybury"});
    df_6.addRecord({"6", "30", "500", "Marybury"});
    df_6.addRecord({"7", "30", "20000", "Marybury"});
    df_6.addRecord({"8", "20", "600", "Hartberg"});
    df_6.addRecord({"9", "20", "100", "Hartberg"});
    df_6.addRecord({"10", "10", "0.5", "Guzmantown"});
    df_6.addRecord({"11", "20", "200", "Josephfort"});
    df_6.addRecord({"12", "10", "500", "Guzmantown"});
    df_6.addRecord({"13", "20", "700", "Josephfort"});
    df_6.addRecord({"14", "20", "200", "Marybury"});
    df_6.addRecord({"15", "30", "500", "Marybury"});
    df_6.addRecord({"16", "30", "20000", "Marybury"});
    df_6.addRecord({"17", "20", "600", "Hartberg"});
    df_6.addRecord({"18", "20", "100", "Hartberg"});
    df_6.addRecord({"19", "40", "800", "Hartberg"});
    df_6.addRecord({"20", "50", "0.5", "Marybury"});
    df_6.addRecord({"21", "60", "800", "Hartberg"});

    df_7.addRecord({"10", "Guzmantown"});
    df_7.addRecord({"20", "Josephfort"});
    df_7.addRecord({"30", "Marybury"});
    df_7.addRecord({"40", "Josephfort"});
    df_7.addRecord({"50", "Hartberg"});
    df_7.addRecord({"60", "Marybury"});

    // Fila de tarefas
    // auto fut1 = pool.enqueue(1, [&df, &idFilter, NUM_THREADS, cond, &pool]() { return filter_records(df, idFilter, NUM_THREADS, cond, pool); });
    // auto fut2 = pool.enqueue(2, [&df_2, &idGroupBy, NUM_THREADS, &groupCol, &targetCol, &pool]() { return groupby_mean(df_2, idGroupBy, NUM_THREADS, groupCol, targetCol, pool); });
    // auto fut3 = pool.enqueue(3, [&df_3, &df_4, &idJoin, NUM_THREADS, &keyCol, &pool]() { return join_by_key(df_3, df_4, idJoin, NUM_THREADS, keyCol, pool); });
    //auto fut4 = pool.enqueue(4, [&df_5, &idGetHour, NUM_THREADS, &timeCol, &pool]() { return get_hour_by_time(df_5, idGetHour, NUM_THREADS, timeCol, pool); });
    auto fut5 = pool.enqueue(5, [&df_6, &df_7, &idAbnormal, NUM_THREADS, &transacCol, &amountCol, &locationCol, &accountCol, &pool]() { return abnormal_transactions(df_6, df_7, idAbnormal, NUM_THREADS, transacCol, amountCol, locationCol, accountCol, accountCol, locationCol, pool); });

    cout << "teste2" << endl;

    // pool.isReady(1);
    // pool.isReady(2);
    // pool.isReady(3);
    //pool.isReady(4);
    pool.isReady(5);

    // DataFrame filtrado = fut1.get();
    // DataFrame df_grouped = fut2.get();
    // DataFrame df_result = fut3.get();
    //DataFrame df_hour = fut4.get();
    DataFrame df_abnormal = fut5.get();
    cout << "teste4" << endl;
    

    // Esse depende do df_hour
    // auto fut6 = pool.enqueue(6, [&df_hour, &idCount, NUM_THREADS, &timeCol, &pool]() { return count_values(df_hour, idCount, NUM_THREADS, timeCol, pool); });
    // pool.isReady(6);
    // DataFrame df_count = fut6.get();

    cout << "teste5" << endl;

    // Resultados
    // cout << "\nTeste para filtrar registros";
    // cout << "\nDataFrame filtrado (salario > 5000):\n";
    // filtrado.printDF();

    // cout << "\nTeste para o groupby:" << endl;
    // df_grouped.printDF();

    // cout << "\nTeste para join:" << endl;
    // df_result.printDF();

    // cout << "\nTeste para get_hour_by_time:" << endl;
    // df_hour.printDF();

    // cout << "\nTeste para count_values:" << endl;
    // df_count.printDF();

    // cout << "\nTeste para abnormal_transactions:" << endl;
    // df_abnormal.printDF();

    return 0;
}
