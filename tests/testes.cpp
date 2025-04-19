#include <iostream>
#include "../include/df.h"
#include "../include/tratadores.h"
#include "../include/csv_extractor.h"
#include "../include/threads.h"

using namespace std;

mutex cout_mutex;

int main() {
    const int NUM_THREADS = thread::hardware_concurrency();
    ThreadPool pool(NUM_THREADS);

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

    cout << "\nTESTE PARA FILTRAR REGISTROS:\n";
    auto cond = [&](const vector<ElementType>& row) -> bool {
        return get<float>(row[2]) > 5000.0f;
    };

    DataFrame filtrado = filter_records(df, cond, NUM_THREADS, pool);

    cout << "\nDataFrame filtrado (salario > 5000):\n";
    filtrado.printDF();

    cout << "\nTeste para o groupby\n";
    vector<string> colNames_2 = {"account_id", "amount"};
    vector<string> colTypes_2 = {"int", "float"};
    DataFrame df_2(colNames_2, colTypes_2);

    df_2.addRecord({"1", "100.0"});
    df_2.addRecord({"2", "200.0"});
    df_2.addRecord({"1", "300.0"});
    df_2.addRecord({"2", "400.0"});
    df_2.addRecord({"3", "150.0"});

    DataFrame df_grouped = groupby_mean(df_2, "account_id", "amount", pool);
    df_grouped.printDF();

    vector<string> colNames_3 = {"id", "nome"};
    vector<string> colTypes_3 = {"int", "string"};
    DataFrame df_3(colNames_3, colTypes_3);
    df_3.addRecord({"1", "Alice"});
    df_3.addRecord({"2", "Bob"});
    df_3.addRecord({"3", "Carlos"});

    vector<string> colNames_4 = {"id", "idade"};
    vector<string> colTypes_4 = {"int", "int"};
    DataFrame df_4(colNames_4, colTypes_4);
    df_4.addRecord({"1", "23"});
    df_4.addRecord({"2", "30"});
    df_4.addRecord({"4", "40"});

    DataFrame df_result = join_by_key(df_3, df_4, "id", pool);

    cout << "Resultado do join:" << endl;
    df_result.printDF();

    // cout << "\nTeste para transactions.csv\n";
    // DataFrame* df_teste = readCSV("data/tests/teste.csv", pool);

    // cout << "\nTeste groupby_mean para transactions.csv\n";
    // DataFrame df_grouped_t = groupby_mean(*df_teste, "account_id", "amount", pool);
    // df_grouped_t.printDF();

    // delete df_teste;

    vector<string> colName_ = {"time_start"};
    vector<string> colType_ = {"string"};
    DataFrame df_5(colName_, colType_);

    df_5.addRecord({"01:46:23"});
    df_5.addRecord({"02:35:22"});
    df_5.addRecord({"01:45:06"});
    df_5.addRecord({"02:18:33"});
    df_5.addRecord({"03:23:59"});
    df_5.addRecord({"02:44:33"});
    df_5.addRecord({"02:56:00"});

    cout << "\nResultado do get_hour_by_time:" << endl;
    DataFrame df_hour = get_hour_by_time(df_5, "time_start", pool);
    df_hour.printDF();

    cout << "\nResultado do count_values:" << endl;
    DataFrame df_count = count_values(df_hour, "time_start", pool);
    df_count.printDF();


    return 0;
}
