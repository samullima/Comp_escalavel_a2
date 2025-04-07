#include <iostream>
#include <vector>
#include <string>
#include <unordered_map>
#include <variant>
#include <mutex>
#include <thread>
#include <memory>

using VariantType = std::variant<int, double, std::string>;

struct DF {
    std::vector<std::vector<VariantType>> columns;
    std::vector<std::string> colNames;
    std::unordered_map<std::string, size_t> idxColumns;
    std::vector<std::unique_ptr<std::mutex>> column_mutexes;
    size_t num_threads;

    DF(size_t threads = 1) : num_threads(threads) {}

    // Adiciona uma nova coluna ao DataFrame
    void add_column(const std::string& name) {
        colNames.push_back(name);
        idxColumns[name] = columns.size();
        columns.emplace_back();
        column_mutexes.emplace_back(std::make_unique<std::mutex>());
    }
};


// Função principal que retorna um novo DF com account_id e categoria
DF classify_accounts_parallel(DF& df) {
    size_t id_idx    = df.idxColumns["account_id"];
    size_t media_idx = df.idxColumns["media"];
    size_t saldo_idx = df.idxColumns["current_balance"];

    auto& id_col    = df.columns[id_idx];
    auto& media_col = df.columns[media_idx];
    auto& saldo_col = df.columns[saldo_idx];

    // Divide o processamento entre as threads
    size_t numRecords = id_col.size();
    size_t block_size = (numRecords + df.num_threads - 1) / df.num_threads;

    std::vector<std::thread> threads;

    // Vetores locais para cada thread armazenarem os resultados parciais
    std::vector<std::vector<VariantType>> partial_ids(df.num_threads);
    std::vector<std::vector<VariantType>> partial_categorias(df.num_threads);

    // Funcao que classifica os registros
    auto classify_task = [&](size_t thread_id, size_t start, size_t end) {
        auto& ids = partial_ids[thread_id];
        auto& categorias = partial_categorias[thread_id];

        for (size_t i = start; i < end && i < numRecords; ++i) {
            if (!std::holds_alternative<int>(id_col[i]) || !std::holds_alternative<double>(media_col[i]) || !std::holds_alternative<double>(saldo_col[i]))
                continue;

            int account_id = std::get<int>(id_col[i]);
            double media = std::get<double>(media_col[i]);
            double saldo = std::get<double>(saldo_col[i]);
            std::string categoria;

            if (media > 5000) {
                categoria = "A";
            } else if (media >= 1000 && media <= 5000) {
                categoria = (saldo > 10000) ? "B" : "C";
            } else {
                categoria = "D";
            }

            ids.push_back(account_id);
            categorias.push_back(categoria);
        }
    };

    // Criação das threads
    for (size_t t = 0; t < df.num_threads; ++t) {
        size_t start = t * block_size;
        size_t end = start + block_size;
        threads.emplace_back(classify_task, t, start, end);
    }

    for (auto& th : threads) {
        th.join();
    }

    // Junta os resultados das threads em um novo df
    DF result(df.num_threads);
    result.add_column("account_id");
    result.add_column("categoria");

    for (size_t t = 0; t < df.num_threads; ++t) {
        for (size_t i = 0; i < partial_ids[t].size(); ++i) {
            result.columns[0].push_back(partial_ids[t][i]);
            result.columns[1].push_back(partial_categorias[t][i]);
        }
    }

    return result;
}

// Exemplo de uso
int main() {
    DF df(2); // duas threads

    df.add_column("account_id");
    df.add_column("media");
    df.add_column("current_balance");

    df.columns[df.idxColumns["account_id"]] = {1, 2, 3, 4, 20, 5, 15, 30};
    df.columns[df.idxColumns["media"]] = {6000.0, 7000.0, 3000.0, 500.0, 1000.0, 5000.0, 15000.0, 700.0};
    df.columns[df.idxColumns["current_balance"]] = {5000.0, 15000.0, 8000.0, 2000.0, 1000.0, 2000.0, 500.0, 3000.0};

    DF resultado = classify_accounts_parallel(df);
    size_t idx_id = resultado.idxColumns["account_id"];
    size_t n = resultado.columns[idx_id].size();
    for (size_t i = 0; i < n; ++i) {
        std::cout << "ID: " << std::get<int>(resultado.columns[resultado.idxColumns["account_id"]][i])
                  << " | Categoria: " << std::get<std::string>(resultado.columns[resultado.idxColumns["categoria"]][i])
                  << '\n';
    }

    return 0;
}
