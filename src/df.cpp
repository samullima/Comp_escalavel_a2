
#include <iostream>
#include <vector>
#include <string>
#include <variant>
#include <unordered_map>
#include <mutex>
#include <thread>
#include <deque>
#include <iomanip>
#include <iostream>
#include <memory>
#include <fstream>
#include <sstream>
#include "../include/df.h"
#include <algorithm>
#include <functional>

using namespace std;
using ElementType = variant<int, float, bool, string>; // Tipo possível das variáveis


// Construtor
DataFrame::DataFrame(const vector<string>& colNamesRef, const vector<string>& colTypesRef)
    : colNames(colNamesRef), numCols(colNamesRef.size()), numRecords(0)
{
    if (colNamesRef.size() != colTypesRef.size()) {
        cerr << "Número de elementos entre os vetores incompatível" << endl;
    }

    // Atualização dos Hashes colName-> idx e colName -> colType 
    for (int i = 0; i < colNamesRef.size(); i++) {
        idxColumns[colNamesRef[i]] = i;
        colTypes[colNamesRef[i]] = colTypesRef[i];
    }

    // Adição de colunas vazias 
    columns.resize(numCols);
    columnMutexes.resize(numCols);
    rowMutexes.resize(numRecords);
}


// Destrutor
DataFrame::~DataFrame()
{
    lock_guard<mutex> lock(mutexDF);

    columns.clear();
    colNames.clear();
    idxColumns.clear();
    colTypes.clear();

    numCols = 0;
    numRecords = 0;
}


void DataFrame::addColumn(const vector<ElementType>& col, string colName, string colType)
{
    lock_guard<mutex> lock(mutexDF);

    // Se ainda não há registros, definimos com base nessa coluna
    if (numRecords == 0) {
        numRecords = col.size();
        rowMutexes.resize(numRecords);  // importante para evitar problemas futuros
    } else if (numRecords != col.size()) {
        cerr << "Número de registros incompatível" << endl;
        return;
    }

    // Se a coluna já existe, atualizamos ela
    if (colTypes.find(colName) != colTypes.end() && colTypes[colName] == colType){
        // Atualização da coluna existente
        int idx = idxColumns[colName];
        //lock_guard<mutex> colLock(columnMutexes[idx]);
        columns[idx] = col;
        return;
    }
    
    // Atualização dos metadados
    numCols++;
    colNames.push_back(colName);
    idxColumns[colName] = numCols-1;  
    colTypes[colName] = colType;
    columns.push_back(col);
    columnMutexes.emplace_back(); 
}


void DataFrame::addRecord(const vector<string>& record) {
    
    if (record.size() != numCols) {
        throw invalid_argument("O número de valores no registro deve ser igual ao número de colunas.");
    }
    
    vector<ElementType> newRecord(numCols);
    
    for (size_t i = 0; i < record.size(); i++) {
        // lock_guard<mutex> colLock(columnMutexes[i]); 
        const string& type = colTypes[colNames[i]];
        const string& value = record[i];
        
        if (type == "int") {
            newRecord[i] = (stoi(value));
        } else if (type == "float") {
            newRecord[i] = (stof(value));
        } else if (type == "bool") {
            if (value == "true" || value == "1") {
                newRecord[i] = (true);
            } else if (value == "false" || value == "0") {
                newRecord[i] = (false);
            } else {
                throw invalid_argument("Valor inválido para bool na coluna " + colNames[i]);
            }
        } else if (type == "string") {
            newRecord[i] = (value);
        } else {
            throw invalid_argument("Tipo de dado desconhecido na coluna " + colNames[i]);
        }
    }
    
    lock_guard<mutex> lock(mutexDF);
    for(size_t i = 0; i < numCols; i++) {
        // lock_guard<mutex> colLock(columnMutexes[i]); 
        columns[i].push_back(newRecord[i]);
    }
    numRecords++;
    rowMutexes.emplace_back();
}


DataFrame DataFrame::getRecords(const vector<int>& indexes) const {
    lock_guard<mutex> lock(mutexDF);

    int qtdIndices = indexes.size();

    // Vetor de tipos
    vector<string> tipos;
    for (const string& col : colNames) {
        tipos.push_back(colTypes.at(col));
    }

    // Novo df com os tipos corretos
    DataFrame dfResult(colNames, tipos);
    dfResult.columns.resize(numCols);
    dfResult.columnMutexes.resize(numCols);  

    // Cópia das linhas
    for (int idx : indexes) {
        if (idx < 0 || idx >= numRecords) {
            cerr << "Índice fora dos limites: " << idx << endl;
            continue;
        }

        for (int j = 0; j < numCols; j++) {
            dfResult.columns[j].push_back(columns[j][idx]);
        }

        dfResult.numRecords++;  
        dfResult.rowMutexes.emplace_back();
    }

    return dfResult;
}

void DataFrame::printDF(){
    /* 
    Realiza o print de um DataFrame. 
    */
   lock_guard<mutex> lock(mutexDF);

    if (numRecords == 0) {
        cout << "DataFrame vazio.\n";
        return;
    }

    vector<int> colWidths(numCols, 0);

    // Calcula a largura máxima de cada coluna 
    // TODO : Adicionar paralelismo aqui nesse bloco
    for (size_t j = 0; j < numCols; j++) {
        colWidths[j] = colNames[j].size();
        
        for (size_t i = 0; i < numRecords; i++) {
            int width = variantToString(columns[j][i]).size();
            if (width > colWidths[j]) {
                colWidths[j] = width;
            }
        }
    }

    // Linha separadora
    auto printSeparator = [&]() {
        cout << "+";
        for (size_t j = 0; j < numCols; j++) {
            cout << string(colWidths[j] + 2, '-') << "+";
        }
        cout << endl;
    };

    // Imprime cabeçalho
    printSeparator();
    cout << "|";
    for (size_t j = 0; j < numCols; j++) {
        cout << " " << setw(colWidths[j]) << left << colNames[j] << " |";
    }
    cout << endl;
    printSeparator();

    // Imprime os registros
    for (size_t i = 0; i < numRecords; i++) {
        cout << "|";
        for (size_t j = 0; j < numCols; j++) {
            cout << " " << setw(colWidths[j]) << left << variantToString(columns[j][i]) << " |";
        }
        cout << endl;
    }

    printSeparator();
    // unlock()
}

void DataFrame::DFtoCSV(string csvName) {
    /* 
    Realiza a conversão do objeto DataFrame para CSV.
    */

    lock_guard<mutex> lock(mutexDF);

    ofstream outFile(csvName + ".csv");
    if (!outFile.is_open()) {
        cerr << "Erro ao abrir o arquivo para escrita." << endl;
        return;
    }

    // Escrita do nome das colunas
    for (int i = 0; i < numCols; ++i) {
        outFile << colNames[i];
        if (i < numCols - 1) 
            outFile << ",";
    }
    outFile << "\n";

    // Escrita dos dados das linhas
    for (int row = 0; row < numRecords; ++row) {
        for (int col = 0; col < numCols; ++col) {
            string valStr = variantToString(columns[col][row]);

            // Aspas em torno de strings e escape de aspas internas
            if (colTypes[colNames[col]] == "string") {
                for (size_t pos = 0; (pos = valStr.find('"', pos)) != string::npos; pos += 2) {
                    valStr.insert(pos, "\""); // duplica aspas internas
                }
                outFile << "\"" << valStr << "\"";
            } else {
                outFile << valStr;
            }

            // Adição do separador ","
            if (col < numCols - 1) 
                outFile << ",";
        }
        outFile << "\n";
    }

    outFile.close();
}

vector<ElementType> DataFrame::getRecord(int i) const {
    vector<ElementType> record;
    for (int j = 0; j < numCols; ++j) {
        record.push_back(columns[j][i]);
    }
    return record;
}

vector<string> DataFrame::getColumnNames() const {
    return colNames;
}

string DataFrame::getColumnName(int idxColumn) const {
    string colName = colNames[idxColumn];
    return colName;
}

int DataFrame::getNumCols() const {
    return numCols;
}

vector<ElementType> DataFrame::getColumn(int i) const {
    return columns[i];
}

unordered_map<string, string> DataFrame::getColumnTypes() const {
    return colTypes;
}

string DataFrame::getColumnType(int idxColumn) const {
    string colName = colNames[idxColumn];
    auto it = colTypes.find(colName);
    return it->second;
}

int DataFrame::getNumRecords() const {
    return numRecords;
}

int DataFrame::getColumnIndex(const string& colName) const {
    auto it = idxColumns.find(colName);
    return it->second;
}

void DataFrame::changeColumnName(string pastName, string newName) {
    lock_guard<mutex> lock(mutexDF);  

    // Verificação da existência da coluna
    if (idxColumns.find(pastName) == idxColumns.end()) {
        throw invalid_argument("Coluna '" + pastName + "' não encontrada.");
    }

    // Verificação se o nome novo já existe
    if (idxColumns.find(newName) != idxColumns.end()) {
        throw invalid_argument("Já existe uma coluna com o nome '" + newName + "'.");
    }

    // Atualização de colNames
    int index = idxColumns[pastName];
    colNames[index] = newName;

    // Atualização de idxColumns
    idxColumns.erase(pastName); 
    idxColumns[newName] = index;

    // Atualização de colTypes
    if (colTypes.find(pastName) != colTypes.end()) {
        string type = colTypes[pastName];
        colTypes.erase(pastName);
        colTypes[newName] = type;
    }
}

// //Driver Code Test
// int main() {
//     // Nome das colunas e tipo
//     vector<string> colNames = {"ID", "Nome", "Salario"};
//     vector<string> colTypes = {"int", "string", "float"};

//     DataFrame df(colNames, colTypes);

//     // Adição de registros
//     df.addRecord({"1", "Camacho", "5000.5"});
//     df.addRecord({"2", "Bebel", "6200.0"});
//     df.addRecord({"3", "Yuri", "4700.75"});

//     // DataFrame original
//     cout << "\nDataFrame original:\n";
//     df.printDF();

//     // Criação de nova coluna bool
//     vector<ElementType> isHighSalary = {true, true, false};
//     string newColName = "Rico?";
//     string newColType = "bool";

//     // Adição da coluna bool
//     df.addColumn(isHighSalary, newColName, newColType);

//     // Print do DataFrame
//     cout << "\nDataFrame com coluna bool adicionada:\n";
//     df.printDF();

//     // Impressão dos metadados
//     cout << "\n--- Metadados do DataFrame ---\n";
//     cout << "Num de registros: " << df.numRecords << endl;
//     cout << "Num de colunas: " << df.numCols << endl;

//     cout << "\nNomes das colunas:\n";
//     for (const auto& name : df.colNames) {
//         cout << "- " << name << endl;
//     }

//     cout << "\nIdx das colunas:\n";
//     for (const auto& [name, idx] : df.idxColumns) {
//         cout << "- " << name << ": " << idx << endl;
//     }

//     cout << "\nTipos das colunas:\n";
//     for (const auto& [name, type] : df.colTypes) {
//         cout << "- " << name << ": " << type << endl;
//     }

//     // Testes do changeColumnName
//     cout << "\n\n--- Testes do changeColumnName ---\n";

//     try {
//         cout << "\nAlterando 'Salario' para 'SalarioBruto'...\n";
//         df.changeColumnName("Salario", "SalarioBruto");
//         cout << "Alteração feita com sucesso!\n";
//     } catch (const exception& e) {
//         cout << "Erro: " << e.what() << endl;
//     }

//     cout << "\nDataFrame após alteração do nome da coluna:\n";
//     df.printDF();

//     cout << "\nNomes das colunas após alteração:\n";
//     for (const auto& name : df.colNames) {
//         cout << "- " << name << endl;
//     }

//     // Tentativa de alterar nome de coluna inexistente
//     try {
//         cout << "\nTentando alterar coluna 'Bonus' para 'Premio'...\n";
//         df.changeColumnName("Bonus", "Premio");
//     } catch (const exception& e) {
//         cout << "Erro: " << e.what() << endl;
//     }

//     // Tentativa de alterar para um nome já existente
//     try {
//         cout << "\nTentando alterar 'Nome' para 'ID'...\n";
//         df.changeColumnName("Nome", "ID");
//     } catch (const exception& e) {
//         cout << "Erro: " << e.what() << endl;
//     }

//     return 0;
// }