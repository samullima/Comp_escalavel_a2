#include <iostream>
#include <vector>
#include <string>
#include <unordered_map>
#include <variant>
#include <mutex>
#include <thread>
#include <memory>
#include <future>
#include "threads.h"
#include "df.h"
#include "tratadores_S.h"

std::mutex cout_mutex;

namespace multiprocessing {

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
                        if (media > 5000) {
                            categoria = "A";
                        } else if (media >= 1000 && media <= 5000) {
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
    
}    

// Exemplo de uso
int main() {
    vector<string> colNames1 = {"account_id", "amount"};
    vector<string> colTypes1 = {"int", "float"};

    DataFrame df1(colNames1, colTypes1);

    df1.addRecord({"1", "6000.0"});
    df1.addRecord({"2", "7000.0"});
    df1.addRecord({"3", "3000.0"});
    df1.addRecord({"4", "500.0"});
    df1.addRecord({"2", "1000.0"});
    df1.addRecord({"3", "5000.0"});
    df1.addRecord({"1", "15000.0"});
    df1.addRecord({"1", "700.0"});

    vector<string> colNames2 = {"account_id", "current_balance"};
    vector<string> colTypes2 = {"int", "float"};

    DataFrame df2(colNames2, colTypes2);

    df2.addRecord({"1", "5000.0"});
    df2.addRecord({"2", "15000.0"});
    df2.addRecord({"3", "8000.0"});
    df2.addRecord({"4", "2000.0"});

    ThreadPool tp(4); // Cria a pool de threads

    DataFrame media = multiprocessing::groupby_mean(df1, "account_id", "amount", tp);
    DataFrame join = multiprocessing::join_by_key(media, df2, "account_id", tp);
    DataFrame resultado = multiprocessing::classify_accounts_parallel(join, "A_account_id", "A_mean_amount", "B_current_balance", tp);

    // Espera pequena para garantir execução completa (ou substitua com método join da ThreadPool se tiver)
    this_thread::sleep_for(std::chrono::milliseconds(200));

    cout << "[MAIN] Média por conta:\n";
    media.printDF();
    std::cout << std::flush;

    std::cout << "[MAIN] Join:\n";
    join.printDF();
    std::cout << std::flush;

    std::cout << "[MAIN] Resultado final:\n";
    resultado.printDF();
    std::cout << std::flush;

    return 0;
}
