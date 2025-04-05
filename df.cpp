#include "df.h"
#include <iostream>
#include <algorithm>
#include <iomanip>
#include <stdexcept>

DataFrame::DataFrame(const vector<string>& colNamesRef, const vector<string>& colTypesRef) {
    lock_guard<mutex> lock(df_mutex);
    if (colNamesRef.size() != colTypesRef.size()) {
        cerr << "Número de elementos entre os vetores incompatível" << endl; 
    }

    colNames = colNamesRef;
    numCols = colNamesRef.size();
    numRecords = 0;
    
    for (int i = 0; i < colNamesRef.size(); i++) {
        idxColumns[colNamesRef[i]] = i;
        colTypes[colNamesRef[i]] = colTypesRef[i];
    }

    columns.resize(numCols);
}

DataFrame::DataFrame(DataFrame&& other) noexcept {
    lock_guard<mutex> lock(other.df_mutex);
    colNames = std::move(other.colNames);
    columns = std::move(other.columns);
    idxColumns = std::move(other.idxColumns);
    colTypes = std::move(other.colTypes);
    numRecords = other.numRecords;
    numCols = other.numCols;
}

DataFrame& DataFrame::operator=(DataFrame&& other) noexcept {
    if (this != &other) {
        lock_guard<mutex> lock(other.df_mutex);
        colNames = std::move(other.colNames);
        columns = std::move(other.columns);
        idxColumns = std::move(other.idxColumns);
        colTypes = std::move(other.colTypes);
        numRecords = other.numRecords;
        numCols = other.numCols;
    }
    return *this;
}


DataFrame::~DataFrame() {
    lock_guard<mutex> lock(df_mutex);
    columns.clear();
    colNames.clear();
    idxColumns.clear();
    colTypes.clear();
    numCols = 0;
    numRecords = 0;
}

void DataFrame::addColumn(const vector<ElementType>& col, string colName, string colType) {
    lock_guard<mutex> lock(df_mutex);
    if (numRecords != col.size()) {
        cerr << "Número de registros incompatível" << endl;
        return;
    }

    if (colTypes.find(colName) != colTypes.end() && colTypes[colName] == colType) {
        columns[idxColumns[colName]] = col;
        return;
    }
    
    numCols++;
    colNames.push_back(colName);
    idxColumns[colName] = numCols-1;  
    colTypes[colName] = colType;
    columns.push_back(col);
}

void DataFrame::addRecord(const vector<string>& record) {
    lock_guard<mutex> lock(df_mutex);
    if (record.size() != numCols) {
        throw invalid_argument("O número de valores no registro deve ser igual ao número de colunas.");
    }

    for (size_t i = 0; i < record.size(); i++) {
        const string& type = colTypes[colNames[i]];
        const string& value = record[i];

        if (type == "int") {
            columns[i].push_back(stoi(value));
        } else if (type == "float") {
            columns[i].push_back(stof(value));
        } else if (type == "bool") {
            columns[i].push_back(value == "true" || value == "1");
        } else if (type == "string") {
            columns[i].push_back(value);
        } else {
            throw invalid_argument("Tipo de dado desconhecido: " + type);
        }
    }
    numRecords++;
}

DataFrame DataFrame::getRecords(const vector<int>& indexes) {
    lock_guard<mutex> lock(df_mutex);
    
    // Cria novo DataFrame com metadados corretos
    vector<string> resultColTypes;
    for (const auto& colName : colNames) {
        resultColTypes.push_back(colTypes.at(colName));
    }
    
    DataFrame dfResult(colNames, resultColTypes);
    dfResult.columns.resize(numCols);
    dfResult.numRecords = indexes.size();

    // Copia os dados
    for (int idx : indexes) {
        if (idx < 0 || idx >= numRecords) continue;
        for (int j = 0; j < numCols; j++) {
            dfResult.columns[j].push_back(columns[j][idx]);
        }
    }

    return dfResult;
}

string DataFrame::variantToString(const ElementType& val) const {
    return visit([](const auto& arg) -> string {
        if constexpr (is_same_v<decay_t<decltype(arg)>, string>)
            return arg;
        else
            return to_string(arg);
    }, val);
}

void DataFrame::printDF() const {
    lock_guard<mutex> lock(df_mutex);
    if (numRecords == 0) {
        cout << "DataFrame vazio.\n";
        return;
    }

    vector<int> colWidths(numCols, 0);
    for (size_t j = 0; j < numCols; j++) {
        colWidths[j] = colNames[j].size();
        for (size_t i = 0; i < numRecords; i++) {
            int width = variantToString(columns[j][i]).size();
            colWidths[j] = max(colWidths[j], width);
        }
    }

    auto printSeparator = [&]() {
        cout << "+";
        for (size_t j = 0; j < numCols; j++) {
            cout << string(colWidths[j] + 2, '-') << "+";
        }
        cout << endl;
    };

    printSeparator();
    cout << "|";
    for (size_t j = 0; j < numCols; j++) {
        cout << " " << setw(colWidths[j]) << left << colNames[j] << " |";
    }
    cout << endl;
    printSeparator();

    for (size_t i = 0; i < numRecords; i++) {
        cout << "|";
        for (size_t j = 0; j < numCols; j++) {
            cout << " " << setw(colWidths[j]) << left << variantToString(columns[j][i]) << " |";
        }
        cout << endl;
    }

    printSeparator();
}

vector<ElementType> DataFrame::getRecord(int i) const {
    lock_guard<mutex> lock(df_mutex);
    vector<ElementType> record;
    for (int j = 0; j < numCols; ++j) {
        record.push_back(columns[j][i]);
    }
    return record;
}

vector<string> DataFrame::getColumnNames() const {
    lock_guard<mutex> lock(df_mutex);
    return colNames;
}

int DataFrame::getNumCols() const {
    lock_guard<mutex> lock(df_mutex);
    return numCols;
}

vector<ElementType> DataFrame::getColumn(int i) const {
    lock_guard<mutex> lock(df_mutex);
    return columns[i];
}

unordered_map<string, string> DataFrame::getColumnTypes() const {
    lock_guard<mutex> lock(df_mutex);
    return colTypes;
}

int DataFrame::getNumRecords() const {
    lock_guard<mutex> lock(df_mutex);
    return numRecords;
}