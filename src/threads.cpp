#include "../include/df.h"
#include "../include/threads.h"    
#include <thread>
#include <chrono>
#include <iostream>
#include <sstream>
#include <iomanip>
#include <mutex>
#include <optional>
#include <future>

using namespace std;
using namespace chrono;

// Mutex global para sincronizar as saídas de log
std::mutex cout_mutex;


void logThread(const string& label, const string& state, optional<steady_clock::time_point> start = {}) {
    /**
     * Imprime uma mensagem de log formatada.
     * 
     * label: Rótulo que identifica a operação.
     * state: Estado da operação ("START" ou "END").
     * start: (Opcional) Tempo de início para calcular a duração.
     */

    lock_guard<mutex> lock(cout_mutex);  // Garante acesso exclusivo à saída
    stringstream ss;
    
    // Converte o ID da thread para string
    stringstream tidStream;
    tidStream << this_thread::get_id();
    string tidStr = tidStream.str();
    
    // Monta a mensagem com campos alinhados
    ss << "[" << setw(5) << left << state << "]"
       << " | Thread ID: " << setw(10) << left << tidStr
       << " | " << label;
       
    // Se for log de finalização, calcula e imprime a duração
    if (state == "END" && start.has_value()) {
        auto now = steady_clock::now();
        auto duration = duration_cast<milliseconds>(now - start.value()).count();
        ss << " | Duration: " << duration << " ms";
    }
    ss << "\n";
    cout << ss.str();
}

void threadAddRecords(DataFrame& df, int threadIndex, int numRecords) {
    /*
     * Tarefa para adicionar registros ao DataFrame.
     * 
     * Cada tarefa adiciona 'numRecords' registros identificados pelo 'threadIndex'.
     * 
     * df: Referência para o DataFrame.
     * threadIndex: Índice identificador da tarefa.
     * numRecords: Número de registros a serem adicionados.
     */

    string label = "AddRecords T" + to_string(threadIndex);
    auto start = steady_clock::now();
    logThread(label, "START");
    
    for (int i = 0; i < numRecords; ++i) {
        vector<string> record = {
            to_string(threadIndex * 100 + i),  // id
            to_string(3.14 + i),                 // float
            (i % 2 == 0 ? "true" : "false"),     // bool
            "str_" + to_string(i)                // string
        };
        df.addRecord(record);
        this_thread::sleep_for(milliseconds(5)); // Simula algum trabalho
    }
    
    logThread(label, "END", start);
}


void threadAddColumn(DataFrame& df, const string& colName, const string& colType, const vector<ElementType>& colData) {
    /**
     * Tarefa para adicionar uma nova coluna ao DataFrame.
     * 
     * df: Referência para o DataFrame.
     * colName: Nome da nova coluna.
     * colType: Tipo da nova coluna.
     * colData: Dados a serem adicionados na nova coluna.
     */

    string label = "AddColumn " + colName;
    auto start = steady_clock::now();
    logThread(label, "START");
    
    df.addColumn(colData, colName, colType);
    
    logThread(label, "END", start);
}

// int main() {
//     // Marca o início do tempo total de execução
//     auto total_start = steady_clock::now();
    
//     // Exibe número de núcleos disponíveis
//     cout << "Número de threads concorrentes disponíveis (número de núcleos): " 
//          << thread::hardware_concurrency() << endl;
    
//     ThreadPool pool(10);
//     cout << "\nNúmero de threads do thread pool: " << pool.size() << "\n" << endl;

//     // Cria o DataFrame com colunas iniciais
//     vector<string> colNames = { "id", "value", "flag", "text" };
//     vector<string> colTypes = { "int", "float", "bool", "string" };
//     DataFrame df(colNames, colTypes);

//     const int numTasks = 4;         // Número de tarefas para adicionar registros
//     const int recordsPerTask = 10;    // Número de registros por tarefa
//     vector<future<void>> futures;

//     // Enfileira tarefas para adicionar registros e armazena os futuros para sincronização
//     for (int i = 0; i < numTasks; ++i) {
//         promise<void> p;
//         futures.push_back(p.get_future());
//         pool.enqueue(i, [&, i, p = move(p)]() mutable {
//             threadAddRecords(df, i, recordsPerTask);
//             p.set_value();
//         });
//     }    
    
//     // Aguarda todas as tarefas de adição de registros finalizarem
//     for (int i = 0; i < numTasks; ++i)
//         futures[i].wait();

//     // Prepara os dados para a nova coluna; o tamanho deve ser igual ao número total de registros
//     vector<ElementType> newCol;
//     for (int i = 0; i < numTasks * recordsPerTask; ++i)
//         newCol.push_back(i % 2 == 0 ? "extraA" : "extraB");

//     futures.clear();
//     {
//         promise<void> p;
//         futures.push_back(p.get_future());
//         pool.enqueue(100, [&df, newCol, p = move(p)]() mutable {
//             threadAddColumn(df, "extra_col", "string", newCol);
//             p.set_value();
//         });
//     }
        
//     // Aguarda a tarefa de adicionar a nova coluna finalizar
//     for (auto& f : futures)
//         f.wait();

//     // Calcula o tempo total de execução
//     auto total_end = steady_clock::now();
//     auto total_duration = duration_cast<milliseconds>(total_end - total_start).count();

//     cout << "\n[RESULTADO FINAL]\n";
//     df.printDF();
//     cout << "\nTempo total de execução: " << total_duration << " ms\n";

//     return 0;
// }
