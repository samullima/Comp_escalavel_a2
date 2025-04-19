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
#include <queue>
#include <numeric>
#include <condition_variable>
#include <cmath>

#include "../include/df.h"
#include "../include/threads.h"
#include "../include/tratadores.h"

using namespace std;

mutex cout_mutex;

// Tipo possível das variáveis
using ElementType = variant<int, float, bool, string>; 

DataFrame sort_by_column_parallel(const DataFrame& df, const string& key_col, ThreadPool& pool, bool ascending) {
	size_t key_idx = df.getColumnIndex(key_col);
	const auto& key_column = df.columns[key_idx];
	size_t n = df.getNumRecords();
	vector<size_t> indices(n);
	iota(indices.begin(), indices.end(), 0);

	int num_threads = pool.size();
	size_t block_size = (n + num_threads - 1) / num_threads;
	vector<vector<size_t>> sorted_blocks(num_threads);
	vector<future<void>> futures;

	for (int i = 0; i < num_threads; ++i) {
		size_t start = i * block_size;
		size_t end = min(start + block_size, n);
		vector<future<void>> futures;

		for (int i = 0; i < num_threads; ++i) {
			size_t start = i * block_size;
			size_t end = min(start + block_size, n);

			futures.push_back(async(std::launch::async, [&, start, end, i]() {
				vector<size_t> local(indices.begin() + start, indices.begin() + end);
				sort(local.begin(), local.end(), [&](size_t a, size_t b) {
                    if (holds_alternative<int>(key_column[a]) && holds_alternative<int>(key_column[b])) {
                        return ascending ? get<int>(key_column[a]) < get<int>(key_column[b]) 
                                         : get<int>(key_column[a]) > get<int>(key_column[b]);
                    }
                    if (holds_alternative<float>(key_column[a]) && holds_alternative<float>(key_column[b])) {
                        return ascending ? get<float>(key_column[a]) < get<float>(key_column[b]) 
                                         : get<float>(key_column[a]) > get<float>(key_column[b]);
                    }
                    return false; // Caso onde os tipos não coincidem
                });
				sorted_blocks[i] = move(local);
			}));
		}

	}

	for (auto& f : futures) f.get();

	// merge com heap
	auto cmp = [&](pair<size_t, int> a, pair<size_t, int> b) {
        float fvalA = holds_alternative<int>(key_column[a.first]) ? static_cast<float>(get<int>(key_column[a.first])) : get<float>(key_column[a.first]);
        float fvalB = holds_alternative<int>(key_column[b.first]) ? static_cast<float>(get<int>(key_column[b.first])) : get<float>(key_column[b.first]);
        return ascending ? fvalA > fvalB : fvalA < fvalB;
    };
	priority_queue<pair<size_t, int>, vector<pair<size_t, int>>, decltype(cmp)> pq(cmp);
	vector<size_t> pos(num_threads, 0);

	for (int i = 0; i < num_threads; ++i)
		if (!sorted_blocks[i].empty())
			pq.emplace(sorted_blocks[i][0], i);

	vector<size_t> final_indices;
	while (!pq.empty()) {
		auto [idx, block] = pq.top();
		pq.pop();
		final_indices.push_back(idx);
		if (++pos[block] < sorted_blocks[block].size())
			pq.emplace(sorted_blocks[block][pos[block]], block);
	}
	// Construir metadados do DataFrame resultante
    vector<string> result_col_names;
    vector<string> result_col_types;

	// Mudamos os nomes das colunas para podermos entender sua origem quando virmos o dataframe depois do join
    for (const auto& name : df.colNames) {
        result_col_names.push_back(name);
        result_col_types.push_back(df.colTypes.at(name));
    }
	DataFrame result(result_col_names, result_col_types);
	for (size_t i : final_indices) {
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

unordered_map<string, ElementType> getQuantiles(const DataFrame& df, const string& colName, const vector<double>& quantiles, ThreadPool& pool) {
    unordered_map<string, ElementType> result;

    DataFrame sorted_df = sort_by_column_parallel(df, colName, pool, true);
    const auto& sorted_col = sorted_df.getColumn(sorted_df.getColumnIndex(colName));
    int n = sorted_df.getNumRecords();

    auto get_quantile_value = [&](double quantile) -> ElementType {
        if (n == 0) return ElementType{};
        double pos = quantile * (n - 1);
        int lower = floor(pos);
        int upper = ceil(pos);
        if (lower == upper) {
            return sorted_col[lower];
        } else {
            if (holds_alternative<int>(sorted_col[lower]) && holds_alternative<int>(sorted_col[upper])) {
                double lower_val = get<int>(sorted_col[lower]);
                double upper_val = get<int>(sorted_col[upper]);
                return static_cast<int>((upper - pos) * lower_val + (pos - lower) * upper_val);
            } else if (holds_alternative<float>(sorted_col[lower]) && holds_alternative<float>(sorted_col[upper])) {
                float lower_val = get<float>(sorted_col[lower]);
                float upper_val = get<float>(sorted_col[upper]);
                return static_cast<float>((upper - pos) * lower_val + (pos - lower) * upper_val);
            } else {
                return sorted_col[lower]; // fallback
            }
        }
    };

    for (double q : quantiles) {
        string label;
        if (q == 0.0) label = "min";
        else if (q == 1.0) label = "max";
        else if (q == 0.5) label = "median";
        else label = "Q" + to_string(static_cast<int>(q * 100));
        result[label] = get_quantile_value(q);
    }

    return result;
}

double calculateMeanParallel(const DataFrame& df, const string& target_col, ThreadPool& pool) {
    // Obtém o índice da coluna de interesse
    int target_idx = df.getColumnIndex(target_col);
    const auto& target_vec = df.columns[target_idx];

    int total_records = df.getNumRecords();
    int num_threads = pool.size();
    int block_size = (total_records + num_threads - 1) / num_threads;

    // Cria promessas e futuros
    vector<promise<pair<double, int>>> promises(num_threads);
    vector<future<pair<double, int>>> futures;

    // Relacionamento entre promessas e futuros
    for (auto& p : promises) {
        futures.push_back(p.get_future());
    }

    // Divide o trabalho entre as threads
    for (int t = 0; t < num_threads; ++t) {
        int start = t * block_size;
        int end = min(start + block_size, total_records);

        pool.enqueue([&, start, end, t]() mutable {
            double local_sum = 0.0;
            int count = 0;

            for (int i = start; i < end; ++i) {
                if (holds_alternative<int>(target_vec[i])) {
                    local_sum += get<int>(target_vec[i]);
                    count++;
                } else if (holds_alternative<float>(target_vec[i])) {
                    local_sum += get<float>(target_vec[i]);
                    count++;
                }
            }

            // Define o resultado local da thread
            promises[t].set_value(make_pair(local_sum, count));
        });
    }

    // Fusão dos resultados: soma total e contagem total
    double total_sum = 0.0;
    int total_count = 0;

    for (auto& f : futures) {
        auto result = f.get(); // Obtém o resultado da thread (bloqueia até estar disponível)
        total_sum += result.first;
        total_count += result.second;
    }

    // Calcula a média
    return total_count > 0 ? total_sum / total_count : 0.0;
}

DataFrame summaryStats(const DataFrame& df, const string& colName, ThreadPool& pool) {
    // Quantis que queremos calcular
    vector<double> quantilesToCompute = {0.0, 0.25, 0.5, 0.75, 1.0};
    unordered_map<string, ElementType> quantile_results = getQuantiles(df, colName, quantilesToCompute, pool);
    
    for (auto& [label, val] : quantile_results) {
        if (holds_alternative<int>(val)) {
            val = static_cast<float>(get<int>(val));  // converte int para float
        }
    }

    // Calcula a média paralelamente
    double mean = calculateMeanParallel(df, colName, pool);

    // Monta os nomes e tipos do DataFrame de saída
    vector<string> colNames = {"statistic", "value"};
    vector<string> colTypes = {"string", "float"};
    DataFrame summary_df(colNames, colTypes);

    // Adiciona os quantis no DataFrame
    summary_df.addRecord({"min", to_string(get<float>(quantile_results["min"]))});
    summary_df.addRecord({"Q1", to_string(get<float>(quantile_results["Q25"]))});
    summary_df.addRecord({"median", to_string(get<float>(quantile_results["median"]))});
    summary_df.addRecord({"Q3", to_string(get<float>(quantile_results["Q75"]))});
    summary_df.addRecord({"max", to_string(get<float>(quantile_results["max"]))});

    // Adiciona a média
    summary_df.addRecord({"mean", to_string(mean)});

    return summary_df;
}

// Teste simples
int main() {
    // DataFrame com coluna float "salario"
    DataFrame df({"id", "nome", "salario"}, {"int", "string", "float"});
    df.addRecord({"1", "Ana", "3500.50"});
    df.addRecord({"2", "Bruno", "2700.75"});
    df.addRecord({"3", "Clara", "4100.00"});
    df.addRecord({"4", "Diego", "1800.25"});
    df.addRecord({"5", "Eva", "5000.80"});
    df.addRecord({"6", "Felipe", "3300.00"});
    df.addRecord({"7", "Gi", "2900.30"});
    df.addRecord({"8", "Hugo", "3100.10"});

    cout << "Original:\n";
    df.printDF();

    ThreadPool pool(4);

    // Ordenar por salário
    DataFrame sorted = sort_by_column_parallel(df, "salario", pool, true);
    cout << "\nOrdenado por 'salario':\n";
    sorted.printDF();

    // Quantis
    vector<double> quantis = {0.0, 0.25, 0.5, 0.75, 1.0};
    auto resultado = getQuantiles(df, "salario", quantis, pool);

    cout << "\nQuantis para 'salario':\n";
    for (const auto& [q, val] : resultado) {
        cout << q << ": ";
        visit([](auto&& v) { cout << v << '\n'; }, val);
    }

    // Média
    double mean = calculateMeanParallel(df, "salario", pool);
    cout << "Média da coluna 'salario': " << mean << endl;

    // Resumo estatístico
    DataFrame resumo = summaryStats(df, "salario", pool);
    cout << "\nResumo estatístico para 'salario':\n";
    resumo.printDF();

    return 0;
}