#include <vector>
#include <string>
#include <variant>
#include <unordered_map>
#include <mutex>
#include <thread>
#include <iomanip>
#include <iostream>
#include <memory>
#include "df.h"

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

    if (numRecords != col.size()){
        cerr << "Número de registros imcompatível" << endl;
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
    lock_guard<mutex> lock(mutexDF);

    if (record.size() != numCols) {
        throw invalid_argument("O número de valores no registro deve ser igual ao número de colunas.");
    }

    for (size_t i = 0; i < record.size(); i++) {
        // lock_guard<mutex> colLock(columnMutexes[i]); 
        const string& type = colTypes[colNames[i]];
        const string& value = record[i];

        if (type == "int") {
            columns[i].push_back(stoi(value));
        } else if (type == "float") {
            columns[i].push_back(stof(value));
        } else if (type == "bool") {
            if (value == "true" || value == "1") {
                columns[i].push_back(true);
            } else if (value == "false" || value == "0") {
                columns[i].push_back(false);
            } else {
                throw invalid_argument("Valor inválido para bool na coluna " + colNames[i]);
            }
        } else if (type == "string") {
            columns[i].push_back(value);
        } else {
            throw invalid_argument("Tipo de dado desconhecido na coluna " + colNames[i]);
        }
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

string variantToString(const ElementType& val) {
    /*Função auxiliar para alterar o tipo variant para string.*/
    return visit([](const auto& arg) -> string {
        if constexpr (is_same_v<decay_t<decltype(arg)>, string>)
            return arg;
        else
            return to_string(arg);
    }, val);
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
}

void DataFrame::printMtx(){
    cout << "Mutexes das COLUNAS:\n";
    for (size_t i = 0; i < columnMutexes.size(); ++i) {
        cout << "  Coluna [" << i << "] mutex @ " << &columnMutexes[i] << "\n";
    }

    cout << "Mutexes das LINHAS:\n";
    for (size_t i = 0; i < rowMutexes.size(); ++i) {
        cout << "  Linha [" << i << "] mutex @ " << &rowMutexes[i] << "\n";
    }
};

// Driver Code Test

