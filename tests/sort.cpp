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

#include "../include/df.h"
#include "../include/threads.h"
#include "../include/tratadores.h"

using namespace std;

mutex cout_mutex;

// Tipo possível das variáveis
using ElementType = variant<int, float, bool, string>; 

// Funçao paralela de ordenaçao
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
        return ascending ? get<int>(key_column[a.first]) > get<int>(key_column[b.first])
                         : get<int>(key_column[a.first]) < get<int>(key_column[b.first]);
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

// Teste simples
int main() {
	DataFrame df({"id", "nome", "idade"}, {"int", "string", "int"});
	df.addRecord({"3", "Alice", "30"});
	df.addRecord({"1", "Bob", "25"});
	df.addRecord({"2", "Carol", "28"});
	df.addRecord({"5", "Dan", "22"});
	df.addRecord({"4", "Eve", "35"});

	cout << "Original:\n";
	df.printDF();

	ThreadPool pool(4);
	DataFrame sorted = sort_by_column_parallel(df, "id", pool, true);

	cout << "\nOrdenado por 'id':\n";
	sorted.printDF();

	return 0;
}