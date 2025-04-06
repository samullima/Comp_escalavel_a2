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

// Mutex para sincronizar acesso ao DataFrame
mutex df_mutex;

string variantToString(const ElementType& val) {
    /*Função auxiliar para alterar o tipo variant para string.*/
    return visit([](const auto& arg) -> string {
        if constexpr (is_same_v<decay_t<decltype(arg)>, string>)
            return arg;
        else
            return to_string(arg);
    }, val);
}

template <typename T>
T accumulate(const vector<T>& vec, T init) {
    for (int i = 0; i < vec.size(); i++) {
        T val = vec[i];
        init += val;
    }
    return init;
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

DataFrame join_by_key(const DataFrame& df1, const DataFrame& df2, const string& key_col, int num_threads) {
    size_t key_idx1 = df1.getColumnIndex(key_col);
    size_t key_idx2 = df2.getColumnIndex(key_col);

    const auto& key_col1 = df1.columns[key_idx1];
    const auto& key_col2 = df2.columns[key_idx2];

    // Pré-processamento: índice para busca em df2 baseado na chave
    unordered_map<int, vector<size_t>> df2_lookup;
    for (size_t i = 0; i < key_col2.size(); ++i) {
        if (holds_alternative<int>(key_col2[i])) {
            int key = get<int>(key_col2[i]);
            df2_lookup[key].push_back(i);
        }
    }

    // Construir metadados do DataFrame resultante
    vector<string> result_col_names;
    vector<string> result_col_types;

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

    // Paralelismo
    size_t numRecords = df1.getNumRecords();
    size_t block_size = (numRecords + num_threads - 1) / num_threads;

    mutex mtx;
    vector<thread> threads;

    auto join_task = [&](size_t start, size_t end) {
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

        // Inserção protegida no DataFrame resultante
        mtx.lock();
        for (const auto& record : local_records) {
            result.addRecord(record);
        }
        mtx.unlock();
    };

    // Lançar threads
    for (size_t i = 0; i < num_threads; ++i) {
        size_t start = i * block_size;
        size_t end = min(start + block_size, numRecords);
        threads.emplace_back(join_task, start, end);
    }

    for (size_t i = 0; i < threads.size(); ++i) {
        threads[i].join();
    }

    return result;
}
