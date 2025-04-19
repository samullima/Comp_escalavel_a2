#include <iostream>
#include <vector>
#include <string>
#include <unordered_map>
#include <variant>
#include <mutex>
#include <thread>
#include <memory>
#include <future>

#include "../include/df.h"
#include "../include/threads.h"

/*
using VariantType = std::variant<int, double, std::string>;
using namespace std;

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
DF classify_accounts_parallel(DF& df, const string& id, const string& class_first, const string& class_sec, int num_threads) {
    size_t id_idx    = df.getColumnIndex(id);
    size_t media_idx = df.getColumnIndex(class_first);
    size_t saldo_idx = df.getColumnIndex(class_sec);

    auto& id_col    = df.columns[id_idx];
    auto& media_col = df.columns[media_idx];
    auto& saldo_col = df.columns[saldo_idx];

    // Divide o processamento entre as threads
    size_t numRecords = id_col.getNumRecords();
    size_t block_size = (numRecords + num_threads - 1) / num_threads;

    std::vector<std::thread> threads;

    // Vetores locais para cada thread armazenarem os resultados parciais
    std::vector<std::vector<VariantType>> partial_ids(num_threads);
    std::vector<std::vector<VariantType>> partial_categorias(num_threads);

    // Funcao que classifica os registros
    auto classify_task = [&](size_t thread_id, size_t start, size_t end) {
        auto& ids = partial_ids[thread_id];
        auto& categorias = partial_categorias[thread_id];

        for (size_t i = start; i < end && i < numRecords; ++i) {
            if (!std::holds_alternative<int>(id_col[i]) || !std::holds_alternative<float>(media_col[i]) || !std::holds_alternative<float>(saldo_col[i]))
                continue;

            int account_id = std::get<int>(id_col[i]);
            float media = std::get<float>(media_col[i]);
            float saldo = std::get<float>(saldo_col[i]);
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
    for (size_t t = 0; t < num_threads; ++t) {
        size_t start = t * block_size;
        size_t end = start + block_size;
        threads.emplace_back(classify_task, t, start, end);
    }

    for (auto& th : threads) {
        th.join();
    }

    // Junta os resultados das threads em um novo df
    DF result(num_threads);
    result.add_column("account_id");
    result.add_column("categoria");

    for (size_t t = 0; t < df.num_threads; ++t) {
        for (size_t i = 0; i < partial_ids[t].size(); ++i) {
            result.columns[0].push_back(partial_ids[t][i]);
            result.columns[1].push_back(partial_categorias[t][i]);
        }
    }

    return result;
}*/

// // Função principal que retorna um novo DF com account_id e categoria
// DF classify_accounts_parallel(DF& df) {
//     size_t id_idx    = df.idxColumns["account_id"];
//     size_t media_idx = df.idxColumns["media"];
//     size_t saldo_idx = df.idxColumns["current_balance"];

//     auto& id_col    = df.columns[id_idx];
//     auto& media_col = df.columns[media_idx];
//     auto& saldo_col = df.columns[saldo_idx];

//     // Divide o processamento entre as threads
//     size_t numRecords = id_col.size();
//     size_t block_size = (numRecords + df.num_threads - 1) / df.num_threads;

//     std::vector<std::thread> threads;

//     // Vetores locais para cada thread armazenarem os resultados parciais
//     std::vector<std::vector<VariantType>> partial_ids(df.num_threads);
//     std::vector<std::vector<VariantType>> partial_categorias(df.num_threads);

//     // Funcao que classifica os registros
//     auto classify_task = [&](size_t thread_id, size_t start, size_t end) {
//         auto& ids = partial_ids[thread_id];
//         auto& categorias = partial_categorias[thread_id];

//         for (size_t i = start; i < end && i < numRecords; ++i) {
//             if (!std::holds_alternative<int>(id_col[i]) || !std::holds_alternative<double>(media_col[i]) || !std::holds_alternative<double>(saldo_col[i]))
//                 continue;

//             int account_id = std::get<int>(id_col[i]);
//             double media = std::get<double>(media_col[i]);
//             double saldo = std::get<double>(saldo_col[i]);
//             std::string categoria;

//             if (media > 5000) {
//                 categoria = "A";
//             } else if (media >= 1000 && media <= 5000) {
//                 categoria = (saldo > 10000) ? "B" : "C";
//             } else {
//                 categoria = "D";
//             }

//             ids.push_back(account_id);
//             categorias.push_back(categoria);
//         }
//     };

//     // Criação das threads
//     for (size_t t = 0; t < df.num_threads; ++t) {
//         size_t start = t * block_size;
//         size_t end = start + block_size;
//         threads.emplace_back(classify_task, t, start, end);
//     }

//     for (auto& th : threads) {
//         th.join();
//     }

//     // Junta os resultados das threads em um novo df
//     DF result(df.num_threads);
//     result.add_column("account_id");
//     result.add_column("categoria");

//     for (size_t t = 0; t < df.num_threads; ++t) {
//         for (size_t i = 0; i < partial_ids[t].size(); ++i) {
//             result.columns[0].push_back(partial_ids[t][i]);
//             result.columns[1].push_back(partial_categorias[t][i]);
//         }
//     }

//     return result;
// }

DataFrame classify_accounts_parallel(DataFrame& df, const string& id, const string& class_first, const string& class_sec, ThreadPool& tp) {
    // Toma os índices das colunas relevantes
    int id_idx    = df.getColumnIndex(id);
    int media_idx = df.getColumnIndex(class_first);
    int saldo_idx = df.getColumnIndex(class_sec);

    // Busca as colunas relevantes
    auto id_col    = df.getColumn(id_idx);
    auto media_col = df.getColumn(media_idx);
    auto saldo_col = df.getColumn(saldo_idx);

    int numRecords = df.getNumRecords();
    int num_threads = tp.size();
    int block_size = (numRecords + num_threads - 1) / num_threads;

    // Promises comunicação de threads
    vector<future<vector<ElementType>>> futures_ids;
    vector<future<vector<ElementType>>> futures_categorias;

    // Para cada thread
    for (int t = 0; t < num_threads; ++t) {
        // Define os limites do bloco
        int start = t * block_size;
        int end = min(start + block_size, numRecords);
        
        // Define vetor de promessas para clientes e suas categorias
        promise<vector<ElementType>> p_ids;
        promise<vector<ElementType>> p_categorias;
        futures_ids.push_back(p_ids.get_future());
        futures_categorias.push_back(p_categorias.get_future());

        // Cada thread processa uma lambda, que processa um bloco
        tp.enqueue(
            [start, end, &id_col, &media_col, &saldo_col, p_ids = move(p_ids), p_categorias = move(p_categorias)]() mutable {
                // Cria um vetor local pra armazenar resultados do bloco
                vector<ElementType> ids, categorias;
    
                // Para cada entrada do blobo
                for (int i = start; i < end; ++i) {
                    // Verifica se os valores estão no formato certo
                    if (!holds_alternative<int>(id_col[i]) || 
                        !holds_alternative<float>(media_col[i]) || 
                        !holds_alternative<float>(saldo_col[i]))
                        continue;
                    
                    // Se está tudo certo, extrai os valores 
                    int account_id = get<int>(id_col[i]);
                    float media = get<float>(media_col[i]);
                    float saldo = get<float>(saldo_col[i]);
    
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
    
                p_ids.set_value(move(ids));
                p_categorias.set_value(move(categorias));
            }
        );
    }

    // Espera todas as threads e junta os resultados
    vector<string> colNames = {"account_id", "categoria"};
    vector<string> colTypes = {"int", "string"};
    DataFrame result(colNames, colTypes);

    // Para cada thread
    for (int t = 0; t < num_threads; ++t) {
        // Pegue os resultados dos futuros
        auto ids = futures_ids[t].get();
        auto categorias = futures_categorias[t].get();

        for (size_t i = 0; i < ids.size(); ++i) {
            result.columns[0].push_back(ids[i]);
            result.columns[1].push_back(categorias[i]);
            result.numRecords++;
        }
    }

    return result;
}  

// Exemplo de uso
int main() {
    int num_threads = 2;
    vector<string> colNames = {"account_id", "media", "current_balance"};
    vector<string> colTypes = {"int", "float", "float"};

    DataFrame df(colNames, colTypes);

    // Adiciona dados manualmente
    vector<ElementType> ids = {1, 2, 3, 4, 20, 5, 15, 30};
    vector<ElementType> medias = {6000.0f, 7000.0f, 3000.0f, 500.0f, 1000.0f, 5000.0f, 15000.0f, 700.0f};
    vector<ElementType> saldos = {5000.0f, 15000.0f, 8000.0f, 2000.0f, 1000.0f, 2000.0f, 500.0f, 3000.0f};

    df.columns[0] = ids;
    df.columns[1] = medias;
    df.columns[2] = saldos;
    df.numRecords = ids.size(); // todos têm o mesmo tamanho

    ThreadPool tp(4); // 4 threads
    DataFrame resultado = classify_accounts_parallel(df, "account_id", "media", "current_balance", tp);
    resultado.printDF();

    return 0;
}
