#include <iostream>
#include <vector>
#include <string>
#include <variant>
#include <unordered_map>
#include <mutex>
#include <thread>
#include <functional>
#include "df.h"
#include <algorithm>

using namespace std;

// Mutex para sincronizar acesso ao DataFrame
mutex df_mutex;

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
    DataFrame new_df(df.getColumnNames(), vector<string>(df.getNumCols()));
    
    for (size_t i = 0; i < df.getNumCols(); i++) {
        vector<ElementType> new_col;
        for (int idx : idxes) {
            new_col.push_back(df.getColumn(i)[idx]);
        }
        new_df.addColumn(new_col, df.getColumnNames()[i], df.getColumnTypes()[df.getColumnNames()[i]]);
    }
    
    return new_df;
}

// Função principal para filtrar registros
DataFrame filter_records(DataFrame& df, function<bool(const vector<ElementType>&)> condition) {
    const int NUM_THREADS = thread::hardware_concurrency();
    int block_size = df.getNumRecords() / NUM_THREADS;
    
    vector<vector<int>> thread_results(NUM_THREADS);
    vector<thread> threads;
    
    df_mutex.lock();
    
    for (int i = 0; i < NUM_THREADS; i++) {
        int start = i * block_size;
        int end = (i == NUM_THREADS - 1) ? df.getNumRecords() : start + block_size;
        
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
    for (const auto& result : thread_results) {
        idx_validos.insert(idx_validos.end(), result.begin(), result.end());
    }
    
    sort(idx_validos.begin(), idx_validos.end());
    
    return filter_records_by_idxes(df, idx_validos);
}

int main() {
    vector<string> nomes = {"id", "nome", "idade", "ativo"};
    vector<string> tipos = {"int", "string", "int", "bool"};

    DataFrame df(nomes, tipos);

    df.addRecord({"1", "Alice", "30", "true"});
    df.addRecord({"2", "Bob", "25", "false"});
    df.addRecord({"3", "Carol", "22", "true"});
    df.addRecord({"4", "Daniel", "40", "true"});
    df.addRecord({"5", "Eva", "19", "false"});

    cout << "DataFrame original:\n";
    df.printDF();

    // Define condição para filtrar: idade > 25
    auto cond = [&](const vector<ElementType>& row) -> bool {
        return get<int>(row[2]) > 25;
    };

    DataFrame filtrado = filter_records(df, cond);

    cout << "\nDataFrame filtrado (idade > 25):\n";
    filtrado.printDF();

    return 0;
}