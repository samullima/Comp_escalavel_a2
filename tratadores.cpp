#include <iostream>
#include <vector>
#include <unordered_map>
#include <variant>
#include <mutex>
#include <memory>
#include <thread>
#include <numeric>

using VariantType = std::variant<int, double, std::string>;

struct DF {
    std::vector<std::vector<VariantType>> columns;
    std::vector<std::string> colNames;
    std::unordered_map<std::string, size_t> idxColumns;
    std::vector<std::unique_ptr<std::mutex>> column_mutexes;
    size_t num_threads;

    DF(size_t threads = 1) : num_threads(threads) {}

    void add_column(const std::string& name) {
        colNames.push_back(name);
        idxColumns[name] = columns.size();
        columns.emplace_back();
        column_mutexes.emplace_back(std::make_unique<std::mutex>());
    }
};

DF sum_by_user(DF& df, const std::string& id_col, const std::string& value_col) {
    size_t id_idx = df.idxColumns[id_col];
    size_t val_idx = df.idxColumns[value_col];

    std::lock_guard<std::mutex> lock_id(*df.column_mutexes[id_idx]);
    std::lock_guard<std::mutex> lock_val(*df.column_mutexes[val_idx]);

    auto& id_column = df.columns[id_idx];
    auto& val_column = df.columns[val_idx];
    size_t numRecords = id_column.size();

    std::unordered_map<int, double> result;
    std::mutex result_mutex;

    size_t block_size = (numRecords + df.num_threads - 1) / df.num_threads;
    std::vector<std::thread> threads;

    auto sum_task = [&](size_t start, size_t end) {
        std::unordered_map<int, double> local_result;
        for (size_t i = start; i < end && i < numRecords; ++i) {
            if (std::holds_alternative<int>(id_column[i]) &&
                (std::holds_alternative<int>(val_column[i]) || std::holds_alternative<double>(val_column[i]))) {

                int user_id = std::get<int>(id_column[i]);
                double value = std::holds_alternative<int>(val_column[i]) ? std::get<int>(val_column[i])
                                                                           : std::get<double>(val_column[i]);
                local_result[user_id] += value;
            }
        }

        std::lock_guard<std::mutex> res_lock(result_mutex);
        for (const auto& [user_id, sum] : local_result) {
            result[user_id] += sum;
        }
    };

    for (size_t i = 0; i < df.num_threads; ++i) {
        size_t start = i * block_size;
        size_t end = start + block_size;
        threads.emplace_back(sum_task, start, end);
    }

    for (auto& t : threads) {
        t.join();
    }

    DF df_result;
    df_result.add_column("user_id");
    df_result.add_column("soma");

    for (const auto& [user_id, soma] : result) {
        df_result.columns[0].push_back(user_id);
        df_result.columns[1].push_back(soma);
    }

    return df_result;
}


DF mean_by_user(DF& df, const std::string& id_col, const std::string& value_col) {
    size_t id_idx = df.idxColumns[id_col];
    size_t val_idx = df.idxColumns[value_col];

    std::lock_guard<std::mutex> lock_id(*df.column_mutexes[id_idx]);
    std::lock_guard<std::mutex> lock_val(*df.column_mutexes[val_idx]);

    auto& id_column = df.columns[id_idx];
    auto& val_column = df.columns[val_idx];
    size_t numRecords = id_column.size();

    std::unordered_map<int, double> sum_result;
    std::unordered_map<int, int> count_result;
    std::mutex result_mutex;

    size_t block_size = (numRecords + df.num_threads - 1) / df.num_threads;
    std::vector<std::thread> threads;

    auto mean_task = [&](size_t start, size_t end) {
        std::unordered_map<int, double> local_sum;
        std::unordered_map<int, int> local_count;

        for (size_t i = start; i < end && i < numRecords; ++i) {
            if (std::holds_alternative<int>(id_column[i]) &&
                (std::holds_alternative<int>(val_column[i]) || std::holds_alternative<double>(val_column[i]))) {

                int user_id = std::get<int>(id_column[i]);
                double value = std::holds_alternative<int>(val_column[i]) ? std::get<int>(val_column[i])
                                                                           : std::get<double>(val_column[i]);

                local_sum[user_id] += value;
                local_count[user_id]++;
            }
        }

        std::lock_guard<std::mutex> lock(result_mutex);
        for (const auto& [user_id, s] : local_sum) {
            sum_result[user_id] += s;
        }
        for (const auto& [user_id, c] : local_count) {
            count_result[user_id] += c;
        }
    };

    for (size_t i = 0; i < df.num_threads; ++i) {
        size_t start = i * block_size;
        size_t end = start + block_size;
        threads.emplace_back(mean_task, start, end);
    }

    for (auto& t : threads) {
        t.join();
    }

    DF df_result;
    df_result.add_column("user_id");
    df_result.add_column("media");

    for (const auto& [user_id, total] : sum_result) {
        double mean_value = total / count_result[user_id];
        df_result.columns[0].push_back(user_id);
        df_result.columns[1].push_back(mean_value);
    }

    return df_result;
}


void select_columns(const DF& df, const std::string& col1, const std::string& col2) {
    if (df.idxColumns.find(col1) == df.idxColumns.end() || df.idxColumns.find(col2) == df.idxColumns.end()) {
        std::cerr << "Erro: Coluna(s) não encontrada(s) no DataFrame.\n";
        return;
    }

    size_t idx1 = df.idxColumns.at(col1);
    size_t idx2 = df.idxColumns.at(col2);

    const auto& c1 = df.columns[idx1];
    const auto& c2 = df.columns[idx2];

    if (c1.empty() || c2.empty()) {
        std::cerr << "Erro: Uma ou ambas as colunas estão vazias.\n";
        return;
    }

    size_t n = std::min(c1.size(), c2.size());

    std::cout << "\nSelecionando colunas: " << col1 << " e " << col2 << "\n";
    for (size_t i = 0; i < n; ++i) {
        std::visit([](auto&& val1) { std::cout << val1 << "\t"; }, c1[i]);
        std::visit([](auto&& val2) { std::cout << val2 << "\n"; }, c2[i]);
    }
}


DF join_by_key(const DF& df1, const DF& df2, const std::string& key_col) {
    DF result(df1.num_threads);

    size_t key_idx1 = df1.idxColumns.at(key_col);
    size_t key_idx2 = df2.idxColumns.at(key_col);

    const auto& key_col1 = df1.columns[key_idx1];
    const auto& key_col2 = df2.columns[key_idx2];

    std::unordered_map<int, std::vector<size_t>> df2_lookup;

    for (size_t i = 0; i < key_col2.size(); ++i) {
        if (std::holds_alternative<int>(key_col2[i])) {
            int key = std::get<int>(key_col2[i]);
            df2_lookup[key].push_back(i);
        }
    }

    for (const auto& name : df1.colNames) {
        result.add_column("A_" + name);
    }
    for (const auto& name : df2.colNames) {
        if (name != key_col) {
            result.add_column("B_" + name);
        }
    }

    std::mutex result_mutex;

    size_t numRecords = key_col1.size();
    size_t block_size = (numRecords + df1.num_threads - 1) / df1.num_threads;
    std::vector<std::thread> threads;

    auto join_task = [&](size_t start, size_t end) {
        std::vector<std::vector<VariantType>> local_rows(result.columns.size());

        for (size_t i = start; i < end && i < numRecords; ++i) {
            if (!std::holds_alternative<int>(key_col1[i])) continue;

            int key = std::get<int>(key_col1[i]);

            auto it = df2_lookup.find(key);
            if (it == df2_lookup.end()) continue;

            for (size_t match_idx : it->second) {
                for (size_t j = 0; j < df1.columns.size(); ++j) {
                    local_rows[j].push_back(df1.columns[j][i]);
                }

                size_t offset = df1.columns.size();
                for (size_t j = 0; j < df2.columns.size(); ++j) {
                    if (j == key_idx2) continue;
                    local_rows[offset++].push_back(df2.columns[j][match_idx]);
                }
            }
        }

        std::lock_guard<std::mutex> lock(result_mutex);
        for (size_t j = 0; j < local_rows.size(); ++j) {
            result.columns[j].insert(result.columns[j].end(), local_rows[j].begin(), local_rows[j].end());
        }
    };

    for (size_t i = 0; i < df1.num_threads; ++i) {
        size_t start = i * block_size;
        size_t end = start + block_size;
        threads.emplace_back(join_task, start, end);
    }

    for (auto& t : threads) t.join();

    return result;
}

int main() {
    DF df(4);
    df.add_column("user_id");
    df.add_column("saldo");

    df.columns[df.idxColumns["user_id"]] = {1, 2, 1, 3, 2, 1, 3};
    df.columns[df.idxColumns["saldo"]] = {100.5, 200, 50, 300.75, 150, 25, 99.25};

    DF df_sum = sum_by_user(df, "user_id", "saldo");
    DF df_mean = mean_by_user(df, "user_id", "saldo");

    DF df_joined = join_by_key(df_sum, df_mean, "user_id");

    std::cout << "\n>> DataFrame - Join (Soma + Média):\n";
    size_t idx_id = df_joined.idxColumns["A_user_id"];
    size_t idx_soma = df_joined.idxColumns["A_soma"];
    size_t idx_media = df_joined.idxColumns["B_media"];
    
    size_t n = df_joined.columns[idx_id].size();
    for (size_t i = 0; i < n; ++i) {
        std::visit([](auto&& v) { std::cout << v << "\t"; }, df_joined.columns[idx_id][i]);
        std::visit([](auto&& v) { std::cout << v << "\t"; }, df_joined.columns[idx_soma][i]);
        std::visit([](auto&& v) { std::cout << v << "\n"; }, df_joined.columns[idx_media][i]);
    }

    return 0;
}
