
#include <iostream>
#include <vector>
#include <string>
#include <variant>
#include <unordered_map>
#include <mutex>
#include <thread>
#include <iomanip>
#include <iostream>
#include "df.h"
#include <algorithm>
#include <functional>

using namespace std;

// Tipo possível das variáveis
using ElementType = variant<int, float, bool, string>; 

// TODO : adicionar mecanismos de concorrência básicos

// Mutex para sincronizar acesso ao DataFrame
mutex df_mutex;

// Construtor
DataFrame::DataFrame(const vector<string>& colNamesRef, const vector<string>& colTypesRef)
{
    /*
    Esse construtor faz a base do DataFrame com base em um vetor de nome das colunas
    e o um vetor com seus respectivos tipos.
    */

    if (colNamesRef.size() != colTypesRef.size()){
        cerr << "Número de elementos entre os vetores incompatível" << endl; 
    }

    colNames = colNamesRef;
    numCols = colNamesRef.size();
    numRecords = 0;
    
    // Atualização dos Hashes colName-> idx & colName -> colType 
    for (int i = 0; i < colNamesRef.size(); i++){
        idxColumns[colNamesRef[i]] = i;
        colTypes[colNamesRef[i]] = colTypesRef[i];
    }

    // Adição de colunas vazias 
    columns.resize(numCols);
}


// Destrutor
DataFrame::~DataFrame()
{
    // lock()
    columns.clear();
    colNames.clear();
    idxColumns.clear();
    colTypes.clear();

    numCols = 0;
    numRecords = 0;
    // unlock()
}


void DataFrame::addColumn(const vector<ElementType>& col, string colName, string colType)
{
    if (numRecords != col.size()){
        cerr << "Número de registros imcompatível" << endl;
    }

    // Se a coluna já existe, atualizamos ela
    if (colTypes.find(colName) != colTypes.end() && colTypes[colName] == colType){
        // Dar lock no df
        // Atualização da coluna existente
        columns[idxColumns[colName]] = col;
    }
    
    // Atualização dos metadados
    numCols++;
    colNames.push_back(colName);
    idxColumns[colName] = numCols-1;  
    colTypes[colName] = colType;
    columns.push_back(col);
    // Dar unlock no DF
}


void DataFrame::addRecord(const vector<string>& record) {
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
}


DataFrame DataFrame::getRecords(const vector<int>& indexes)
{
    int qtdIndices = indexes.size();

    // Novo DataFrame 
    DataFrame dfResult(colNames, vector<string>(colNames.size()));
    for (int i = 0; i < colNames.size(); i++) {
        dfResult.colTypes[colNames[i]] = colTypes[colNames[i]];
        dfResult.idxColumns[colNames[i]] = i;
    }

    dfResult.numCols = numCols;
    dfResult.numRecords = qtdIndices;
    dfResult.columns.resize(numCols); 

    // Aqui entraria o lock, se estivermos usando concorrência
    // mutex.lock();

    // Copia os dados das linhas selecionadas
    for (int idx : indexes) {
        if (idx < 0 || idx >= numRecords) {
            cerr << "Índice fora dos limites: " << idx << endl;
            continue;
        }

        for (int j = 0; j < numCols; j++) {
            dfResult.columns[j].push_back(columns[j][idx]);
        }
    }

    // mutex.unlock();

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

    if (numRecords == 0) {
        cout << "DataFrame vazio.\n";
        return;
    }

    // lock()
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

int DataFrame::getNumCols() const {
    return numCols;
}

vector<ElementType> DataFrame::getColumn(int i) const {
    return columns[i];
}

unordered_map<string, string> DataFrame::getColumnTypes() const {
    return colTypes;
}

int DataFrame::getNumRecords() const {
    return numRecords;
}

// Função auxiliar para filtrar um bloco de registros
vector<int> filter_block_records(DataFrame& df, function<bool(const vector<ElementType>&)> condition, int idx_min, int idx_max) {
    vector<int> idxes_list;
    
    for (int i = idx_min; i < idx_max; i++) {
        if (condition(df.getRecord(i))) {
            idxes_list.push_back(i);
        }
    }
    
    return idxes_list;
}

// Função auxiliar para criar um novo DataFrame com registros filtrados
DataFrame filter_records_by_idxes(DataFrame& df, const vector<int>& idxes) {
    return df.getRecords(idxes);
}

// Função principal para filtrar registros
DataFrame filter_records(DataFrame& df, function<bool(const vector<ElementType>&)> condition, int num_threads) {
    //const int NUM_THREADS = thread::hardware_concurrency();
    int block_size = df.getNumRecords() / num_threads;
    
    vector<vector<int>> thread_results(num_threads);
    vector<thread> threads;
    
    df_mutex.lock();
    
    for (int i = 0; i < num_threads; i++) {
        int start = i * block_size;
        int end = (i == num_threads - 1) ? df.getNumRecords() : min(start + block_size, df.getNumRecords());
        
        threads.emplace_back([&, start, end, i]() {
            thread_results[i] = filter_block_records(df, condition, start, end);
        });
    }
    
    for (auto& t : threads) {
        t.join();
    }
    
    df_mutex.unlock();
    
    // Combina os resultados
    vector<int> idx_validos;
    for (auto it = thread_results.begin(); it != thread_results.end(); ++it) {
        const auto& result = *it;
        idx_validos.insert(idx_validos.end(), result.begin(), result.end());
    }
    
    sort(idx_validos.begin(), idx_validos.end());
    
    return filter_records_by_idxes(df, idx_validos);
}

int DataFrame::getColumnIndex(const string& colName) const {
    auto it = idxColumns.find(colName);
    return it->second;
}

template <typename T>
T accumulate(const vector<T>& vec, T init) {
    for (int i = 0; i < vec.size(); i++) {
        T val = vec[i];
        init += val;
    }
    return init;
}

DataFrame groupby_mean(DataFrame& df, const string& group_col, const string& target_col, int num_threads) {
    int group_idx = df.getColumnIndex(group_col);
    int target_idx = df.getColumnIndex(target_col);

    unordered_map<string, vector<float>> groups;

    for (int i = 0; i < df.getNumRecords(); ++i) {
        string key = variantToString(df.getColumn(group_idx)[i]);
        float value = get<float>(df.getColumn(target_idx)[i]);
        groups[key].push_back(value);
    }

    // Cria o novo DataFrame com as médias
    vector<string> colNames = {group_col, "mean_" + target_col};
    vector<string> colTypes = {"string", "float"};
    DataFrame result_df(colNames, colTypes);

    // Mutex para sincronizar o acesso ao DataFrame final
    mutex mtx;

    // Container de threads
    vector<thread> threads;

    // Converter unordered_map para vetor de pares para permitir fatiamento
    vector<pair<string, vector<float>>> group_entries(groups.begin(), groups.end());
    size_t total_groups = group_entries.size();
    size_t block_size = (total_groups + num_threads - 1) / num_threads;

    for (size_t t = 0; t < num_threads; ++t) {
        size_t start = t * block_size;
        size_t end = min(start + block_size, total_groups);

        threads.emplace_back([start, end, &group_entries, &result_df, &mtx, &group_col, &target_col]() {
            for (size_t i = start; i < end; ++i) {
                const string& key = group_entries[i].first;
                const vector<float>& values = group_entries[i].second;
                float mean = accumulate(values, 0.0f) / values.size();

                mtx.lock();
                result_df.addRecord({key, to_string(mean)});
                mtx.unlock();
            }
        });
    }

    for (size_t i = 0; i < threads.size(); ++i) {
        threads[i].join();
    }

    return result_df;
}



// Driver Code Test

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

    return 0;
}