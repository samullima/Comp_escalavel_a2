#include <iostream>
#include <vector>
#include <string>
#include <variant>
#include <unordered_map>
#include <unordered_set>
#include <map>
#include <mutex>
#include <thread>
#include <iomanip>
#include <iostream>
#include <algorithm>
#include <functional>
#include <future>
#include <cmath>
#include <numeric>

#include "../include/df.h"
#include "../include/threads.h"
#include "../include/tratadores.h"

using namespace std;

// Tipo possível das variáveis
using ElementType = variant<int, float, bool, string>; 

// Função auxiliar para filtrar um bloco de registros
vector<int> filter_block_records(DataFrame& df, function<bool(const vector<ElementType>&)> condition, int idxMin, int idxMax) {
    vector<int> idxesList;
    
    for (int i = idxMin; i < idxMax; i++) {
        if (condition(df.getRecord(i))) {
            idxesList.push_back(i);
        }
    }
    
    return idxesList;
}

// Função auxiliar para criar um novo DataFrame com registros filtrados
DataFrame filter_records_by_idxes(DataFrame& df, const vector<int>& idxes) {
    return df.getRecords(idxes);
}


DataFrame filter_records(DataFrame& df, int id, int numThreads, function<bool(const vector<ElementType>&)> condition, ThreadPool& pool) {
    numThreads = min(numThreads, static_cast<int>(df.getNumRecords()));
    int blockSize = (df.getNumRecords() + numThreads - 1) / numThreads;

    vector<shared_ptr<promise<vector<int>>>> promises(numThreads);
    vector<future<vector<int>>> futures;

    // Conectamos promessas e futuros
    for (int i = 0; i < numThreads; ++i) {
        promises[i] = make_shared<promise<vector<int>>>();
        futures.push_back(promises[i]->get_future());
    }

    for (int i = 0; i < numThreads; ++i) {
        int start = i * blockSize;
        int end = min(start + blockSize, df.getNumRecords());

        pool.enqueue(-id, [&, start, end, p = promises[i]]() mutable {
            vector<int> result = filter_block_records(df, condition, start, end);
            p->set_value(move(result));
        });
    }

    pool.isReady(-id);

    vector<int> idxValidos;
    for (auto& f : futures) {
        vector<int> res = f.get();
        idxValidos.insert(idxValidos.end(), res.begin(), res.end());
    }

    sort(idxValidos.begin(), idxValidos.end());
    return filter_records_by_idxes(df, idxValidos);
}

DataFrame groupby_mean(DataFrame& df, int id, int numThreads, const string& groupCol, const string& targetCol, ThreadPool& pool) {
    int groupIdx = df.getColumnIndex(groupCol);
    int targetIdx = df.getColumnIndex(targetCol);

    const auto& groupVec = df.columns[groupIdx];
    const auto& targetVec = df.columns[targetIdx];

    int total_records = df.getNumRecords();
    numThreads = min(numThreads, static_cast<int>(total_records));
    int block_size = (total_records + numThreads - 1) / numThreads;

    vector<shared_ptr<promise<unordered_map<string, pair<float, int>>>>> promises(numThreads);
    vector<future<unordered_map<string, pair<float, int>>>> futures;

    // Define o relacionamento entre promessas e futuros
    for (int i = 0; i < numThreads; ++i) {
        promises[i] = make_shared<promise<unordered_map<string, pair<float, int>>>>();
        futures.push_back(promises[i]->get_future());
    }

    for (int t = 0; t < numThreads; ++t) {
        int start = t * block_size;
        int end = min(start + block_size, total_records);

        pool.enqueue(-id, [&, start, end, p = promises[t]]() mutable {
            unordered_map<string, pair<float, int>> local_map;
            for (int i = start; i < end; ++i) {
                string key = variantToString(groupVec[i]);
                float value = get<float>(targetVec[i]);
                auto& acc = local_map[key];
                acc.first += value;
                acc.second += 1;
            }
            p->set_value(move(local_map));
        });
    }

    pool.isReady(-id);

    // Fusão dos resultados
    unordered_map<string, pair<float, int>> globalMap;
    for (auto& f : futures) {
        auto local = f.get();
        for (const auto& [key, val] : local) {
            globalMap[key].first += val.first;
            globalMap[key].second += val.second;
        }
    }

    // Novo DataFrame
    vector<string> colNames = {groupCol, "mean_" + targetCol};
    vector<string> colTypes = {"string", "float"};
    DataFrame resultDf(colNames, colTypes);

    for (const auto& [key, pair] : globalMap) {
        float mean = pair.first / pair.second;
        resultDf.addRecord({key, to_string(mean)});
    }

    return resultDf;
}

DataFrame join_by_key(const DataFrame& df1, const DataFrame& df2, int id, int numThreads, const string& keyCol, ThreadPool& pool) {
    size_t keyIdx1 = df1.getColumnIndex(keyCol);
    size_t keyIdx2 = df2.getColumnIndex(keyCol);

    const auto& keyCol1 = df1.columns[keyIdx1];
    const auto& keyCol2 = df2.columns[keyIdx2];

    // Mapa que associa cada valor da chave em df2 a uma lista de posições onde ele aparece
    unordered_map<int, vector<size_t>> df2Lookup;
    for (size_t i = 0; i < keyCol2.size(); ++i) {
        // Verifica se o valor da chave naquela linha é do tipo int
        if (holds_alternative<int>(keyCol2[i])) {
            int key = get<int>(keyCol2[i]);
            df2Lookup[key].push_back(i);
        }
    }

    vector<string> resultColNames;
    vector<string> result_col_types;

    // Mudamos os nomes das colunas para podermos entender sua origem quando virmos o dataframe depois do join
    for (const auto& name : df1.colNames) {
        resultColNames.push_back("A_" + name);
        result_col_types.push_back(df1.colTypes.at(name));
    }
    for (const auto& name : df2.colNames) {
        if (name != keyCol) {
            resultColNames.push_back("B_" + name);
            result_col_types.push_back(df2.colTypes.at(name));
        }
    }

    DataFrame result(resultColNames, result_col_types);

    size_t numRecords = df1.getNumRecords();
    numThreads = min(numThreads, static_cast<int>(numRecords));
    size_t blockSize = (numRecords + numThreads - 1) / numThreads;

    vector<shared_ptr<promise<vector<vector<string>>>>> promises(numThreads);
    vector<future<vector<vector<string>>>> futures;

    for (int i = 0; i < numThreads; ++i) {
        promises[i] = make_shared<promise<vector<vector<string>>>>();
        futures.push_back(promises[i]->get_future());
    }

    for (int i = 0; i < numThreads; ++i) {
        size_t start = i * blockSize;
        size_t end = min(start + blockSize, numRecords);

        pool.enqueue(-id, [&, start, end, p = promises[i]]() mutable {
            vector<vector<string>> local_records;
            for (size_t idx = start; idx < end && idx < numRecords; ++idx) {
                if (!holds_alternative<int>(keyCol1[idx])) continue;

                int key = get<int>(keyCol1[idx]);
                auto it = df2Lookup.find(key);
                if (it == df2Lookup.end()) continue;

                for (size_t match_idx : it->second) {
                    vector<string> record;

                    // Dados do df1
                    for (size_t j = 0; j < df1.getNumCols(); ++j) {
                        record.push_back(variantToString(df1.columns[j][idx]));
                    }

                    // Dados do df2 (sem a chave)
                    for (size_t j = 0; j < df2.getNumCols(); ++j) {
                        if (j == keyIdx2) continue;
                        record.push_back(variantToString(df2.columns[j][match_idx]));
                    }
                    local_records.push_back(record);
                }
            }
            p->set_value(move(local_records));
        });
    }

    pool.isReady(-id);

    for (auto& f : futures) {
        auto local = f.get();
        for (const auto& record : local) {
            result.addRecord(record);
        }
    }

    return result;
}

DataFrame count_values(const DataFrame& df, int id, int numThreads, const string& colName, int numDays, ThreadPool& pool) {
    int colIdx = df.getColumnIndex(colName);
    const vector<ElementType>& column = df.columns[colIdx];
    size_t dataSize = column.size();
    numThreads = min(numThreads, static_cast<int>(dataSize));
    size_t blockSize = (dataSize + numThreads - 1) / numThreads;

    vector<shared_ptr<promise<unordered_map<string, int>>>> promises(numThreads);
    vector<future<unordered_map<string, int>>> futures;

    // Cálculo de contagens para cada thread
    for (int t = 0; t < numThreads; ++t) {
        // Criando uma promise para um future
        promises[t] = make_shared<promise<unordered_map<string, int>>>();
        futures.push_back(promises[t]->get_future());

        size_t start = t * blockSize;
        size_t end = min(start + blockSize, dataSize);

        if (start >= end) break;

        // Enfileira tarefa no threadpool
        pool.enqueue(-id, [&, start, end, p = promises[t]]() mutable {
            unordered_map<string, int> localCount;
            for (size_t i = start; i < end; ++i) {
                string key = variantToString(column[i]);
                localCount[key]++;
            }
            // Desbloqueia o future e envia o valor a ele
            p->set_value(move(localCount));
        });
    }

    pool.isReady(-id);

    // Junta os resultados
    unordered_map<string, int> global_count;
    for (auto& f : futures) {
        // f.get() é bloqueado até que a thred mande o valor da promise ao future
        auto local = f.get();
        for (const auto& [key, count] : local) {
            global_count[key] += count;
        }
    }

    // Pegando tipo da coluna original
    int idxColumn = df.getColumnIndex(colName);
    string typeColumn = df.getColumnType(idxColumn);

    vector<string> colNames = {colName, "count"};
    vector<string> colTypes = {typeColumn, "int"};
    DataFrame result(colNames, colTypes);

    // Adicionando registros
    for (const auto& [key, count] : global_count) {
        int finalCount = (numDays > 0) ? count / numDays : count;
        result.addRecord({key, to_string(finalCount)});
    }

    return result;
}

DataFrame get_hour_by_time(const DataFrame& df, int id, int numThreads, const string& colName, ThreadPool& pool)
{
    int idxColumn = df.getColumnIndex(colName);
    vector<ElementType> timeColumn = df.getColumn(idxColumn);
    size_t dataSize = timeColumn.size();
    numThreads = min(numThreads, static_cast<int>(dataSize));
    size_t blockSize = (dataSize + numThreads - 1) / numThreads;
    
    vector<shared_ptr<promise<vector<string>>>> promises(numThreads);
    vector<future<vector<string>>> futures;
    
    for (int t = 0; t < numThreads; ++t)
    {
        // Criando uma promise para um future
        promises[t] = make_shared<promise<vector<string>>>();
        futures.push_back(promises[t]->get_future());
        
        size_t start = min(t * blockSize, dataSize);
        size_t end = min(start + blockSize, dataSize);
        if (start >= end) break;
        
        // Enfileira a tarefa
        pool.enqueue(-id, [&, start, end, p = promises[t]]() mutable {
            vector<string> partialResult;

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
            // Desbloqueia o future e envia o valor a ele
            p->set_value(move(partialResult));
        });
    }

    pool.isReady(-id);
    
    
    // Junta os resultados
    vector<string> hoursColumn;

    for(int i = 0; i < numThreads; i++)
    {
        vector<string> partial = futures[i].get();
        hoursColumn.insert(hoursColumn.end(), partial.begin(), partial.end());
    }

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

DataFrame num_transac_by_hour(const DataFrame& df, int id, int numThreads, const string& hourCol, int numDays, ThreadPool& pool)
{
    DataFrame hours = get_hour_by_time(df, id, numThreads, hourCol, pool);
    DataFrame counts = count_values(hours, id, numThreads, hourCol, numDays, pool);
    
    return counts;
}

DataFrame classify_accounts_parallel(DataFrame& df, int id, int numThreads, const string& idCol, const string& classFirst, const string& classSec, ThreadPool& tp) {
    // Toma os índices das colunas relevantes
    int idIdx = df.getColumnIndex(idCol);
    int mediaIdx = df.getColumnIndex(classFirst);
    int saldoIdx = df.getColumnIndex(classSec);

    // Busca as colunas relevantes
    auto idCol_ = df.getColumn(idIdx);
    auto mediaCol = df.getColumn(mediaIdx);
    auto saldoCol = df.getColumn(saldoIdx);

    int numRecords = df.getNumRecords();
    numThreads = min(numThreads, static_cast<int>(numRecords));
    int block_size = (numRecords + numThreads - 1) / numThreads;

    // Promises comunicação de threads
    vector<future<vector<ElementType>>> futuresIds;
    vector<future<vector<ElementType>>> futuresCategorias;

    // Para cada thread
    for (int t = 0; t < numThreads; ++t) {
        // Define os limites do bloco
        int start = t * block_size;
        int end = min(start + block_size, numRecords);
        
        // Define vetor de promessas para clientes e suas categorias
        promise<vector<ElementType>> pIds;
        promise<vector<ElementType>> pCategorias;
        futuresIds.push_back(pIds.get_future());
        futuresCategorias.push_back(pCategorias.get_future());

        // Cada thread processa uma lambda, que processa um bloco
        tp.enqueue(-id, 
            [start, end, &idCol_, &mediaCol, &saldoCol, pIds = move(pIds), pCategorias = move(pCategorias)]() mutable {
                // Cria um vetor local pra armazenar resultados do bloco
                vector<ElementType> ids, categorias;
    
                // Para cada entrada do bloco
                for (int i = start; i < end; ++i) {
                    // Verifica se os valores estão no formato certo
                    if (!holds_alternative<int>(idCol_[i]) || 
                        !holds_alternative<float>(mediaCol[i]) || 
                        !holds_alternative<float>(saldoCol[i]))
                        continue;
                    
                    // Se está tudo certo, extrai os valores 
                    int account_id = get<int>(idCol_[i]);
                    float media = get<float>(mediaCol[i]);
                    float saldo = get<float>(saldoCol[i]);
    
                    // Classifica pessoas de acordo com as médias
                    string categoria;
                    if (media > 500) {
                        categoria = "A";
                    } else if (media >= 200 && media <= 500) {
                        categoria = (saldo > 10000) ? "B" : "C";
                    } else {
                        categoria = "D";
                    }
    
                    // E armazena os resultados
                    ids.push_back(account_id);
                    categorias.push_back(categoria);
                }
    
                pIds.set_value(move(ids));
                pCategorias.set_value(move(categorias));
            }
        );
    }
    tp.isReady(-id);

    // Espera todas as threads e junta os resultados
    vector<string> colNames = {"account_id", "categoria"};
    vector<string> colTypes = {"int", "string"};
    DataFrame result(colNames, colTypes);

    // Para cada thread
    for (int t = 0; t < numThreads; ++t) {
        // Pegue os resultados dos futuros
        auto ids = futuresIds[t].get();
        auto categorias = futuresCategorias[t].get();

        for (size_t i = 0; i < ids.size(); ++i) {
            result.columns[0].push_back(ids[i]);
            result.columns[1].push_back(categorias[i]);
            result.numRecords++;
        }
    }

    return result;
}

DataFrame sort_by_column_parallel(const DataFrame& df, int id, int numThreads, const string& keyCol, ThreadPool& pool, bool ascending) {
    size_t keyIdx = df.getColumnIndex(keyCol);
    const auto& keyColumn = df.columns[keyIdx];
    size_t n = df.getNumRecords();
    vector<size_t> indices(n);
    iota(indices.begin(), indices.end(), 0);
    
    numThreads = min(numThreads, static_cast<int>(n));
    size_t block_size = (n + numThreads - 1) / numThreads;
    
    vector<vector<size_t>> sortedBlocks(numThreads);
    vector<promise<void>> promises(numThreads);
    vector<future<void>> futures;
    
    // Relaciona promessas e futuros
    for (auto& p : promises) {
        futures.push_back(p.get_future());
    }

    // Lança as tarefas no pool
    for (int i = 0; i < numThreads; ++i) {
        size_t start = min(i * block_size, n);
        size_t end = min(start + block_size, n);

        pool.enqueue(-id, [&, start, end, i]() {
            vector<size_t> local(indices.begin() + start, indices.begin() + end);
            sort(local.begin(), local.end(), [&](size_t a, size_t b) {
                if (holds_alternative<int>(keyColumn[a]) && holds_alternative<int>(keyColumn[b])) {
                    return ascending ? get<int>(keyColumn[a]) < get<int>(keyColumn[b])
                                     : get<int>(keyColumn[a]) > get<int>(keyColumn[b]);
                }
                if (holds_alternative<float>(keyColumn[a]) && holds_alternative<float>(keyColumn[b])) {
                    return ascending ? get<float>(keyColumn[a]) < get<float>(keyColumn[b])
                                     : get<float>(keyColumn[a]) > get<float>(keyColumn[b]);
                }
                return false;
            });
            sortedBlocks[i] = move(local);
            promises[i].set_value();
        });
    }

    pool.isReady(-id);

    for (auto& f : futures) f.get();

    // Merge com heap
    auto cmp = [&](pair<size_t, int> a, pair<size_t, int> b) {
        float fvalA = holds_alternative<int>(keyColumn[a.first]) ? static_cast<float>(get<int>(keyColumn[a.first])) : get<float>(keyColumn[a.first]);
        float fvalB = holds_alternative<int>(keyColumn[b.first]) ? static_cast<float>(get<int>(keyColumn[b.first])) : get<float>(keyColumn[b.first]);
        return ascending ? fvalA > fvalB : fvalA < fvalB;
    };
    priority_queue<pair<size_t, int>, vector<pair<size_t, int>>, decltype(cmp)> pq(cmp);
    vector<size_t> pos(numThreads, 0);

    for (int i = 0; i < numThreads; ++i)
        if (!sortedBlocks[i].empty())
            pq.emplace(sortedBlocks[i][0], i);

    vector<size_t> finalIndices;
    while (!pq.empty()) {
        auto [idx, block] = pq.top();
        pq.pop();
        finalIndices.push_back(idx);
        if (++pos[block] < sortedBlocks[block].size())
            pq.emplace(sortedBlocks[block][pos[block]], block);
    }

    // Construir metadados do DataFrame resultante
    vector<string> resultColNames;
    vector<string> resultColTypes;

    for (const auto& name : df.colNames) {
        resultColNames.push_back(name);
        resultColTypes.push_back(df.colTypes.at(name));
    }

    DataFrame result(resultColNames, resultColTypes);
    for (size_t i : finalIndices) {
        vector<string> record;
        for (size_t j = 0; j < df.getNumCols(); ++j) {
            const auto& val = df.columns[j][i];
            if (holds_alternative<int>(val)) {
                record.push_back(to_string(get<int>(val)));
            } else if (holds_alternative<float>(val)) {
                record.push_back(to_string(get<float>(val)));
            } else if (holds_alternative<bool>(val)) {
                record.push_back(get<bool>(val) ? "true" : "false");
            } else if (holds_alternative<string>(val)) {
                record.push_back(get<string>(val));
            }
        }
        result.addRecord(record);
    }

    return result;
}

unordered_map<string, ElementType> getQuantiles(const DataFrame& df, int id, int numThreads, const string& colName, const vector<double>& quantiles, ThreadPool& pool) {
    unordered_map<string, ElementType> result;

    DataFrame sortedDf = sort_by_column_parallel(df, id, numThreads, colName, pool, true);
    const auto& sortedCol = sortedDf.getColumn(sortedDf.getColumnIndex(colName));
    int n = sortedDf.getNumRecords();

    // função para calcular os quantis
    auto getQuantileValue = [&](double quantile) -> ElementType {
        if (n == 0) return ElementType{};
        double pos = quantile * (n - 1);
        int lower = floor(pos);
        int upper = ceil(pos);
        if (lower == upper) {
            return sortedCol[lower];
        } 
        // se for um numero par de records calcula a media
        else {
            if (holds_alternative<int>(sortedCol[lower]) && holds_alternative<int>(sortedCol[upper])) {
                double lowerVal = get<int>(sortedCol[lower]);
                double upperVal = get<int>(sortedCol[upper]);
                return static_cast<int>((upper - pos) * lowerVal + (pos - lower) * upperVal);
            } else if (holds_alternative<float>(sortedCol[lower]) && holds_alternative<float>(sortedCol[upper])) {
                float lowerVal = get<float>(sortedCol[lower]);
                float upperVal = get<float>(sortedCol[upper]);
                return static_cast<float>((upper - pos) * lowerVal + (pos - lower) * upperVal);
            } else {
                return sortedCol[lower];
            }
        }
    };
    // adiciona os quantis no df
    for (double q : quantiles) {
        string label;
        if (q == 0.0) label = "min";
        else if (q == 1.0) label = "max";
        else if (q == 0.5) label = "median";
        else label = "Q" + to_string(static_cast<int>(q * 100));
        result[label] = getQuantileValue(q);
    }

    return result;
}

double calculateMeanParallel(const DataFrame& df, int id, int numThreads, const string& target_col, ThreadPool& pool) {
    // Obtém o índice da coluna de interesse
    int targetIdx = df.getColumnIndex(target_col);
    const auto& targetVec = df.columns[targetIdx];

    int totalRecords = df.getNumRecords();
    numThreads = min(numThreads, static_cast<int>(totalRecords));
    int blockSize = (totalRecords + numThreads - 1) / numThreads;

    // Cria promessas e futuros
    vector<promise<pair<double, int>>> promises(numThreads);
    vector<future<pair<double, int>>> futures;

    // Relacionamento entre promessas e futuros
    for (auto& p : promises) {
        futures.push_back(p.get_future());
    }

    // Divide o trabalho entre as threads
    for (int t = 0; t < numThreads; ++t) {
        int start = t * blockSize;
        int end = min(start + blockSize, totalRecords);

        pool.enqueue(-id, [&, start, end, t]() mutable {
            double localSum = 0.0;
            int count = 0;

            for (int i = start; i < end; ++i) {
                if (holds_alternative<int>(targetVec[i])) {
                    localSum += get<int>(targetVec[i]);
                    count++;
                } else if (holds_alternative<float>(targetVec[i])) {
                    localSum += get<float>(targetVec[i]);
                    count++;
                }
            }

            // Define o resultado local da thread
            promises[t].set_value(make_pair(localSum, count));
        });
    }

    pool.isReady(-id);

    // Fusão dos resultados
    double totalSum = 0.0;
    int totalCount = 0;

    for (auto& f : futures) {
        auto result = f.get();
        totalSum += result.first;
        totalCount += result.second;
    }

    // Calcula a média
    return totalCount > 0 ? totalSum / totalCount : 0.0;
}

DataFrame summaryStats(const DataFrame& df, int id, int numThreads, const string& colName, ThreadPool& pool) {
    // Quantis que queremos calcular
    vector<double> quantilesToCompute = {0.0, 0.25, 0.5, 0.75, 1.0};
    unordered_map<string, ElementType> quantileResults = getQuantiles(df, id, numThreads, colName, quantilesToCompute, pool);
    
    for (auto& [label, val] : quantileResults) {
        if (holds_alternative<int>(val)) {
            val = static_cast<float>(get<int>(val));
        }
    }

    // Calcula a média
    double mean = calculateMeanParallel(df, id, numThreads, colName, pool);

    // Monta os nomes e tipos do DataFrame de saída
    vector<string> colNames = {"statistic", "value"};
    vector<string> colTypes = {"string", "float"};
    DataFrame summaryDf(colNames, colTypes);

    // Adiciona os quantis no DataFrame
    summaryDf.addRecord({"min", to_string(get<float>(quantileResults["min"]))});
    summaryDf.addRecord({"Q1", to_string(get<float>(quantileResults["Q25"]))});
    summaryDf.addRecord({"median", to_string(get<float>(quantileResults["median"]))});
    summaryDf.addRecord({"Q3", to_string(get<float>(quantileResults["Q75"]))});
    summaryDf.addRecord({"max", to_string(get<float>(quantileResults["max"]))});

    // Adiciona a média
    summaryDf.addRecord({"mean", to_string(mean)});

    return summaryDf;
}

DataFrame top_10_cidades_transacoes(const DataFrame& df, int id, int numThreads, const string& colName, ThreadPool& pool) {
    // Conta o número de transações por cidade
    int numDays = 0;
    DataFrame contagem = count_values(df, id, numThreads, colName, numDays, pool);

    // Ordena de forma decrescente pelo número de transações
    DataFrame ordenado = sort_by_column_parallel(contagem, id, numThreads, contagem.getColumnName(1), pool, false);

    // Monta os nomes e tipos do DataFrame de saída
    vector<string> colNames = {"location", "num_trans"};
    vector<string> colTypes = {"string", "int"};
    DataFrame top10(colNames, colTypes);

    // Adiciona as 10 primeiras cidades no df
    size_t max_records = 10;
    for (size_t i = 0; i < max_records; ++i) {
        vector<ElementType> record = ordenado.getRecord(i);
        string cidade = std::get<string>(record[0]);
        int num_trans = std::get<int>(record[1]);

        top10.addRecord({cidade, to_string(num_trans)});
    }
    
    return top10;
}

DataFrame abnormal_transactions(const DataFrame& dfTransac, const DataFrame& dfAccount, int id, int numThreads, const string& transactionIDCol, const string& amountCol, const string& locationColTransac, const string& accountColTransac, const string& accountColAccount, const string& locationColAccount, ThreadPool& pool)
{
    unordered_map<string, ElementType> quantiles = getQuantiles(dfTransac, id, numThreads, amountCol, {0.25, 0.75}, pool);
    float q1 = get<float>(quantiles["Q25"]);
    float q3 = get<float>(quantiles["Q75"]);
    float iqr = q3 - q1;
    
    float lower = (q1 - 0.45f * iqr < 0) ? 1.0f : (q1 - 0.45f * iqr);
    float upper = q3 + 1.25f * iqr;

    size_t dataSize = dfTransac.getNumRecords();
    numThreads = min(numThreads, static_cast<int>(dataSize));
    size_t blockSize = (dataSize + numThreads - 1) / numThreads;

    int idxTrans = dfTransac.getColumnIndex(transactionIDCol);
    int idxAmount = dfTransac.getColumnIndex(amountCol);
    int idxLocation = dfTransac.getColumnIndex(locationColTransac);
    int idxAccountTransac = dfTransac.getColumnIndex(accountColTransac);
    int idxAccountAccount = dfAccount.getColumnIndex(accountColAccount);
    int idxLocationAccount = dfAccount.getColumnIndex(locationColAccount);
    
    const vector<ElementType>& colTrans = dfTransac.getColumn(idxTrans);
    const vector<ElementType>& colAmount = dfTransac.getColumn(idxAmount);
    const vector<ElementType>& colLocationTransac = dfTransac.getColumn(idxLocation);
    const vector<ElementType>& colAccountTransac = dfTransac.getColumn(idxAccountTransac);
    const vector<ElementType>& colAccountAccount = dfAccount.getColumn(idxAccountAccount);
    const vector<ElementType>& colLocationAccount = dfAccount.getColumn(idxLocationAccount);
    
    // Mapeando todos os ids de conta para as suas localizações
    unordered_map<int, string> accountLocationMap;
    for (int i = 0; i < dfAccount.getNumRecords(); i++) 
    {

        int id = get<int>(colAccountAccount[i]);
        string loc = get<string>(colLocationAccount[i]);
        accountLocationMap[id] = loc;
    }

    vector<shared_ptr<promise<tuple<vector<ElementType>, vector<ElementType>, vector<ElementType>>>>> promises(numThreads);
    vector<future<tuple<vector<ElementType>, vector<ElementType>, vector<ElementType>>>> futures;

    for (int t = 0; t < numThreads; ++t)
    {
        size_t start = t * blockSize;
        size_t end = std::min(start + blockSize, dataSize);
        if (start >= end) break;

        promises[t] = make_shared<promise<tuple<vector<ElementType>, vector<ElementType>, vector<ElementType>>>>();
        futures.push_back(promises[t]->get_future());
        
        pool.enqueue(-id, [start, end, lower, upper,
                    &colTrans, &colAmount, &colLocationTransac, &colAccountTransac, 
                    &colAccountAccount, &colLocationAccount, &accountLocationMap, p = promises[t]]() {
            vector<ElementType> ids;
            vector<ElementType> suspiciousLocation;
            vector<ElementType> suspiciousAmount;

            for (size_t i = start; i < end; i++) 
            {
                float amount = get<float>(colAmount[i]);
                string locationTransac = get<string>(colLocationTransac[i]);
                int id = get<int>(colTrans[i]);
                int accountIDTransac = get<int>(colAccountTransac[i]);
                
                // Pegando a localização da conta em account
                auto it = accountLocationMap.find(accountIDTransac);
                string locationAccount = it->second;

                bool isAmountSus = (amount < lower || amount > upper);
                bool isLocationSus = (locationTransac != locationAccount);
                if (isAmountSus || isLocationSus) {
                    ids.push_back(id);
                    suspiciousLocation.push_back(isLocationSus);
                    suspiciousAmount.push_back(isAmountSus);
                }
            }

            p->set_value(make_tuple(move(ids), move(suspiciousLocation), move(suspiciousAmount)));
        });
    }

    pool.isReady(-id);

    // Juntando resultados das threads
    vector<ElementType> ids, suspiciousLocation, suspiciousAmount;
    for (auto& fut : futures)
    {
        auto [localIds, localLoc, localAmt] = fut.get();
        ids.insert(ids.end(), localIds.begin(), localIds.end());
        suspiciousLocation.insert(suspiciousLocation.end(), localLoc.begin(), localLoc.end());
        suspiciousAmount.insert(suspiciousAmount.end(), localAmt.begin(), localAmt.end());
    }

    // Monta o DataFrame
    string typeColumn = dfTransac.getColumnType(idxTrans);
    vector<string> colNames = {transactionIDCol, "is_location_suspicious", "is_amount_suspicious"};
    vector<string> colTypes = {typeColumn, "bool", "bool"};

    DataFrame result(colNames, colTypes);
    result.addColumn(ids, transactionIDCol, typeColumn);
    result.addColumn(suspiciousLocation, "is_location_suspicious", "bool");
    result.addColumn(suspiciousAmount, "is_amount_suspicious", "bool");

    return result;
}
















