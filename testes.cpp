#include <iostream>
#include "df.h"
#include "tratadores_S.hpp"
#include "csv_extractor.h"


using namespace std;

int main() {
    // Nome das colunas e tipo
    vector<string> colNames = {"ID", "Nome", "Salario"};
    vector<string> colTypes = {"int", "string", "float"};

    DataFrame df(colNames, colTypes);

    // Adição de registros
    df.addRecord({"1", "Camacho", "5000.5"});
    df.addRecord({"2", "Bebel", "6200.0"});
    df.addRecord({"3", "Yuri", "4700.75"});

    // Dataframe 
    cout << "\nDataFrame original:\n";
    df.printDF();

    // Criação de nova coluna bool
    vector<ElementType> isHighSalary = {true, true, false};
    string newColName = "Rico?";
    string newColType = "bool";

    // Adição da coluna bool
    df.addColumn(isHighSalary, newColName, newColType);

    // Print do df
    cout << "\nDataFrame com coluna bool adicionada:\n";
    df.printDF();

    // Impressão dos metadados
    cout << "\n--- Metadados do DataFrame ---\n";
    cout << "Num de registros: " << df.numRecords << endl;
    cout << "Num de colunas: " << df.numCols << endl;

    cout << "\nNomes das colunas:\n";
    for (const auto& name : df.colNames) {
        cout << "- " << name << endl;
    }

    cout << "\nIdx das colunas:\n";
    for (const auto& [name, idx] : df.idxColumns) {
        cout << "- " << name << ": " << idx << endl;
    }

    cout << "\nTipos das colunas:\n";
    for (const auto& [name, type] : df.colTypes) {
        cout << "- " << name << ": " << type << endl;
    }

    // Teste de filtrar registros
    const int NUM_THREADS = thread::hardware_concurrency();

    cout << "\nTESTE PARA FILTRAR REGISTROS:\n";
    // Define condição para filtrar: idade > 5000
    auto cond = [&](const vector<ElementType>& row) -> bool {
        return get<float>(row[2]) > 5000.0f;
    };
    
    DataFrame filtrado = filter_records(df, cond, NUM_THREADS);

    cout << "\nDataFrame filtrado (idade > 5000):\n";
    filtrado.printDF();

    // Teste do groupby
    cout << "\nTeste para o groupby\n";
    vector<string> colNames_2 = {"account_id", "amount"};
    vector<string> colTypes_2 = {"int", "float"};

    DataFrame df_2(colNames_2, colTypes_2);

    df_2.addRecord({"1", "100.0"});
    df_2.addRecord({"2", "200.0"});
    df_2.addRecord({"1", "300.0"});
    df_2.addRecord({"2", "400.0"});
    df_2.addRecord({"3", "150.0"});

    DataFrame df_grouped = groupby_mean(df_2, "account_id", "amount", NUM_THREADS);
    df_grouped.printDF();

    /////////////////////////////

    // Definição das colunas e tipos do primeiro DataFrame
    vector<string> colNames_3 = {"id", "nome"};
    vector<string> colTypes_3 = {"int", "string"};
    DataFrame df_3(colNames_3, colTypes_3);

    // Adicionando registros no df1
    df_3.addRecord({"1", "Alice"});
    df_3.addRecord({"2", "Bob"});
    df_3.addRecord({"3", "Carlos"});

    // Definição das colunas e tipos do segundo DataFrame
    vector<string> colNames_4 = {"id", "idade"};
    vector<string> colTypes_4 = {"int", "int"};
    DataFrame df_4(colNames_4, colTypes_4);

    // Adicionando registros no df2
    df_4.addRecord({"1", "23"});
    df_4.addRecord({"2", "30"});
    df_4.addRecord({"4", "40"}); // Essa chave (id=4) não tem correspondência

    // Fazendo o join pelo campo "id"
    DataFrame df_result = join_by_key(df_3, df_4, "id", NUM_THREADS);

    // Imprimindo o resultado do join
    cout << "Resultado do join:" << endl;
    df_result.printDF();

    // Teste
    cout << "\nTeste para transactions.csv\n";
    DataFrame *df_teste = readCSV("teste.csv", NUM_THREADS);

    cout << "\nTeste groupby_mean para transactions.csv\n";
    DataFrame df_grouped_t = groupby_mean(*df_teste, "account_id", "amount", 1);
    df_grouped_t.printDF();

    return 0;
}