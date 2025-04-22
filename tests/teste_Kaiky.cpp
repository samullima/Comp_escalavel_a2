#include <iostream>
#include "../include/df.h"

using namespace std;

//Driver Code Test
int main() {
    // Nome das colunas e tipo
    vector<string> colNames = {"ID", "Nome", "Salario"};
    vector<string> colTypes = {"int", "string", "float"};

    DataFrame df(colNames, colTypes);

    // Adição de registros
    df.addRecord({"1", "Camacho", "5000.5"});
    df.addRecord({"2", "Bebel", "6200.0"});
    df.addRecord({"3", "Yuri", "4700.75"});

    // DataFrame original
    cout << "\nDataFrame original:\n";
    df.printDF();

    // Criação de nova coluna bool
    vector<ElementType> isHighSalary = {true, true, false};
    string newColName = "Rico?";
    string newColType = "bool";

    // Adição da coluna bool
    df.addColumn(isHighSalary, newColName, newColType);

    // Print do DataFrame
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

    // Testes do changeColumnName
    cout << "\n\n--- Testes do changeColumnName ---\n";

    try {
        cout << "\nAlterando 'Salario' para 'SalarioBruto'...\n";
        df.changeColumnName("Salario", "SalarioBruto");
        cout << "Alteração feita com sucesso!\n";
    } catch (const exception& e) {
        cout << "Erro: " << e.what() << endl;
    }

    cout << "\nDataFrame após alteração do nome da coluna:\n";
    df.printDF();

    cout << "\nNomes das colunas após alteração:\n";
    for (const auto& name : df.colNames) {
        cout << "- " << name << endl;
    }

    // Tentativa de alterar nome de coluna inexistente
    try {
        cout << "\nTentando alterar coluna 'Bonus' para 'Premio'...\n";
        df.changeColumnName("Bonus", "Premio");
    } catch (const exception& e) {
        cout << "Erro: " << e.what() << endl;
    }

    // Tentativa de alterar para um nome já existente
    try {
        cout << "\nTentando alterar 'Nome' para 'ID'...\n";
        df.changeColumnName("Nome", "ID");
    } catch (const exception& e) {
        cout << "Erro: " << e.what() << endl;
    }

    return 0;
}
