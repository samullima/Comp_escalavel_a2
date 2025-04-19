#include <iostream>
#include <vector>
#include <string>
#include <variant>
#include <unordered_map>
#include <map>
#include <mutex>
#include <thread>
#include <iomanip>
#include <iostream>
#include <algorithm>
#include <functional>
#include <future>

#include "../include/df.h"
#include "../include/threads.h"
#include "../include/tratadores.h"

using namespace std;

// Tipo possível das variáveis
using ElementType = variant<int, float, bool, string>; 

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


DataFrame filter_records(DataFrame& df, function<bool(const vector<ElementType>&)> condition, int num_threads, ThreadPool& pool) {
    // Define o tamanho do bloco
    int block_size = (df.getNumRecords() + num_threads - 1) / num_threads;

    // Define vetor de promessas e de futuros
    vector<promise<vector<int>>> promises(num_threads);
    vector<future<vector<int>>> futures;

    // Conectamos promessas e futuros (future resolvido qnd chamarmos p.set_value())
    for (auto& p : promises) {
        futures.push_back(p.get_future()); // Armazenamos todos os leitores para uso posterior
    }

    // Para cada thread
    for (int i = 0; i < num_threads; ++i) {
        // Define os limites do bloco
        int start = i * block_size;
        int end = min(start + block_size, df.getNumRecords());

        // Cada thread processa uma lambda, que processa um bloco
        pool.enqueue(
            [&, start, end, i]() mutable {
                // Recebemos o vetor filtrado e comunicamos às promessas
                vector<int> result = filter_block_records(df, condition, start, end);
                promises[i].set_value(move(result));
            }
        );
    }

    // Espera todos os resultados e junta os índices válidos
    vector<int> idx_validos;
    for (auto& f : futures) {
        vector<int> res = f.get();
        idx_validos.insert(idx_validos.end(), res.begin(), res.end());
    }

    sort(idx_validos.begin(), idx_validos.end());
    return filter_records_by_idxes(df, idx_validos);
}

DataFrame groupby_mean(DataFrame& df, const string& group_col, const string& target_col, ThreadPool& pool) {
    // Toma os índices das colunas relevantes
    int group_idx = df.getColumnIndex(group_col);
    int target_idx = df.getColumnIndex(target_col);

    // Acessa as colunas de agrupamento e a coluna cujos valores vamos operar
    const auto& group_vec = df.columns[group_idx];
    const auto& target_vec = df.columns[target_idx];

    int total_records = df.getNumRecords();
    int num_threads = pool.size();
    int block_size = (total_records + num_threads - 1) / num_threads;

    // Cria promessas e futuros
    vector<promise<unordered_map<string, pair<float, int>>>> promises(num_threads);
    vector<future<unordered_map<string, pair<float, int>>>> futures;

    // Define o relacionamento entre promessas e futuros
    for (auto& p : promises) {
        futures.push_back(p.get_future());
    }

    // Para cada thread
    for (int t = 0; t < num_threads; ++t) {
        int start = t * block_size;
        int end = min(start + block_size, total_records);
        
        pool.enqueue([&, start, end, t]() mutable {
            unordered_map<string, pair<float, int>> local_map;

            for (int i = start; i < end; ++i) {
                string key = variantToString(group_vec[i]); // converte o valor da coluna de agrupamento para string
                float value = get<float>(target_vec[i]);    // extrai o valor numérico da coluna de interesse
                auto& acc = local_map[key]; // pega ref do acumulador da chave
                acc.first += value;
                acc.second += 1;
            }

            // Entrega o resultado parcial da thread via promise
            promises[t].set_value(move(local_map));
        });
    }

    // Fusão dos resultados: chave -> (soma total, contagem total)
    unordered_map<string, pair<float, int>> global_map;

    // Para cada futuro
    for (auto& f : futures) {
        auto local = f.get(); // obtém o resultado parcial da thread (bloqueia até o valor estar disponível)
        // Adiona o resultado local no mapa global
        for (const auto& [key, val] : local) {
            global_map[key].first += val.first;
            global_map[key].second += val.second;
        }
    }

    // Montagem do DataFrame de saída
    vector<string> colNames = {group_col, "mean_" + target_col};
    vector<string> colTypes = {"int", "float"};
    DataFrame result_df(colNames, colTypes);

    for (const auto& [key, pair] : global_map) {
        float mean = pair.first / pair.second;
        result_df.addRecord({key, to_string(mean)});    // adiciona ao DataFrame como registro
    }
    return result_df;
}

DataFrame join_by_key(const DataFrame& df1, const DataFrame& df2, const string& key_col, ThreadPool& pool) {
    // Pega os índices das colunas relevantes
    size_t key_idx1 = df1.getColumnIndex(key_col);
    size_t key_idx2 = df2.getColumnIndex(key_col);

    // Acessa as colunas de interesse
    const auto& key_col1 = df1.columns[key_idx1];
    const auto& key_col2 = df2.columns[key_idx2];

    // Pré-processamento: índice para busca em df2 baseado na chave
    unordered_map<int, vector<size_t>> df2_lookup;
    // Mapa que associa cada valor da chave em df2 a uma lista de posições onde ele aparece

    for (size_t i = 0; i < key_col2.size(); ++i) {
        // Verifica se o valor da chave naquela linha é do tipo int
        if (holds_alternative<int>(key_col2[i])) {
            int key = get<int>(key_col2[i]);         // Extrai o valor inteiro da chave
            df2_lookup[key].push_back(i);            // Adiciona o índice i à lista de posições dessa chave
        }
    }

    // Construir metadados do DataFrame resultante
    vector<string> result_col_names;
    vector<string> result_col_types;

    // Mudamos os nomes das colunas para podermos entender sua origem quando virmos o dataframe depois do join
    for (const auto& name : df1.colNames) {
        result_col_names.push_back("A_" + name);
        result_col_types.push_back(df1.colTypes.at(name));
    }
    for (const auto& name : df2.colNames) {
        if (name != key_col) {
            result_col_names.push_back("B_" + name);
            result_col_types.push_back(df2.colTypes.at(name));
        }
    }

    DataFrame result(result_col_names, result_col_types);

    // Paralelismo usando ThreadPool
    size_t numRecords = df1.getNumRecords();
    int num_threads = pool.size();
    size_t block_size = (numRecords + num_threads - 1) / num_threads;

    vector<future<vector<vector<string>>>> futures;

    for (int i = 0; i < num_threads; ++i) {
        size_t start = i * block_size;
        size_t end = min(start + block_size, numRecords);

        promise<vector<vector<string>>> p;
        futures.push_back(p.get_future());

        pool.enqueue([&, start, end, p = move(p)]() mutable {
            vector<vector<string>> local_records;

            for (size_t i = start; i < end && i < numRecords; ++i) {
                if (!holds_alternative<int>(key_col1[i])) continue;

                int key = get<int>(key_col1[i]);
                auto it = df2_lookup.find(key);
                if (it == df2_lookup.end()) continue;

                for (size_t match_idx : it->second) {
                    vector<ElementType> joined_row;

                    // Dados do df1
                    for (size_t j = 0; j < df1.getNumCols(); ++j) {
                        joined_row.push_back(df1.columns[j][i]);
                    }

                    // Dados do df2 (sem a chave)
                    for (size_t j = 0; j < df2.getNumCols(); ++j) {
                        if (j == key_idx2) continue;
                        joined_row.push_back(df2.columns[j][match_idx]);
                    }

                    // Conversão para string
                    vector<string> record_str;
                    for (const auto& el : joined_row) {
                        if (holds_alternative<int>(el)) {
                            record_str.push_back(to_string(get<int>(el)));
                        } else if (holds_alternative<float>(el)) {
                            record_str.push_back(to_string(get<float>(el)));
                        } else {
                            record_str.push_back(get<string>(el));
                        }
                    }

                    local_records.push_back(record_str);
                }
            }

            p.set_value(move(local_records));
        });
    }

    // Fusão dos resultados
    for (auto& f : futures) {
        auto local = f.get();
        for (const auto& record : local) {
            result.addRecord(record);
        }
    }

    return result;
}

DataFrame count_values(const DataFrame& df, const string& colName, ThreadPool& pool)
{
    int colIdx = df.getColumnIndex(colName);
    vector<ElementType> column = df.getColumn(colIdx);
    size_t dataSize = column.size();

    int numThreads = pool.size();
    size_t blockSize = (dataSize + numThreads - 1) / numThreads;

    vector<future<unordered_map<string, int>>> futures;

    // Cálculo de contagens para cada thread
    for (int t=0; t<numThreads; t++)
    {
        size_t start = t * blockSize;
        size_t end = min(start + blockSize, dataSize);
        if (start >= end) break;

        // Criando uma promise para um future
        auto promise = make_shared<std::promise<unordered_map<string, int>>>();
        futures.push_back(promise->get_future());

        // Enfileira tarefa no threadpool
        pool.enqueue([start, end, &column, promise]() {
            // Contagem
            unordered_map<string, int> localCounts;
            for (size_t i = start; i < end; i++) {
                const ElementType& val = column[i];
                string valStr = variantToString(val);
                localCounts[valStr]++;
            }
            // Desbloqueia o future e envia o valor a ele
            promise->set_value(move(localCounts));
        });
    }

     // Junta os resultados
     unordered_map<string, int> counts;
     for (auto& f : futures) {
        // f.get() é bloqueado até que a thred mande o valor da promise ao future
        unordered_map<string, int> localMap = f.get();
        for (const auto& pair : localMap) {
            counts[pair.first] += pair.second;
         }
     }

    // Novas colunas
    vector<ElementType> values;
    vector<ElementType> frequencies;

    // Adicionando valores às colunas
    for (const auto& pair: counts)
    {
        values.push_back(pair.first);
        frequencies.push_back(pair.second);
    }

    // Pegando tipo da coluna original
    int idxColumn = df.getColumnIndex(colName);
    string typeColumn = df.getColumnType(idxColumn);

    // Novo DataFrame
    vector<string> colNames = {"value", "count"};
    vector<string> colTypes = {typeColumn, "int"};

    DataFrame result(colNames, colTypes);
    result.addColumn(values, "value", typeColumn);
    result.addColumn(frequencies, "count", "int");

    return result;
}

DataFrame get_hour_by_time(const DataFrame& df, const string& colName, ThreadPool& pool)
{
    int idxColumn = df.getColumnIndex(colName);
    vector<ElementType> timeColumn = df.getColumn(idxColumn);

    size_t dataSize = timeColumn.size();
    int numThreads = pool.size();
    size_t blockSize = (dataSize + numThreads - 1) / numThreads;
    vector<future<vector<string>>> futures;

    for (int t = 0; t < numThreads; ++t)
    {
        size_t start = t * blockSize;
        size_t end = min(start + blockSize, dataSize);
        if (start >= end) break;

        auto promise = make_shared<std::promise<vector<string>>>();
        futures.push_back(promise->get_future());

        // Enfileira a tarefa
        pool.enqueue([start, end, &timeColumn, promise]() {
            vector<string> partialResult;
            //partialResult.reserve(end - start); // pode ser útil para o desempenho

            for (size_t i = start; i < end; i++)
            {
                const auto& elem = timeColumn[i];
                if (holds_alternative<string>(elem))
                {
                    string timeStr = get<string>(elem);
                    partialResult.push_back(timeStr.substr(0, 2));
                }
                else
                {
                    partialResult.push_back("");
                }
            }

            promise->set_value(move(partialResult));
        });
    }

    // Junta os resultados
    vector<string> hoursColumn;
    //hoursColumn.reserve(dataSize); // pode ser útil para o desempenho
    for (auto& f : futures)
    {
        vector<string> partial = f.get();
        hoursColumn.insert(hoursColumn.end(), partial.begin(), partial.end());
    }

    // Pegando tipo/nome da coluna original
    string typeColumn = df.getColumnType(idxColumn);
    string nameColumn = df.getColumnName(idxColumn);

    // Novo DataFrame com a coluna hour
    vector<string> colNames = {nameColumn};
    vector<string> colTypes = {typeColumn};

    vector<ElementType> colHour;
    for (size_t i=0; i<hoursColumn.size(); i++)
    {
        const auto& hour = hoursColumn[i];
        colHour.push_back(hour);
    }

    DataFrame dfHours(colNames, colTypes);
    dfHours.addColumn(colHour, nameColumn, typeColumn);

    return dfHours;
}