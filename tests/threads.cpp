#include "../include/threads.h"
#include "../include/df.h"
#include "../include/csv_extractor.h"
#include "../include/tratadores.h"
#include <chrono>
#include <thread>
#include <mutex>
#include <fstream>
#include <ctime>
#include <iomanip>
#include <random>
#include <vector>

using namespace std;
std::mutex cout_mutex; 
std::mutex file_mutex;

void func() {
    std::lock_guard<std::mutex> lock(cout_mutex);
    std::cout << "\nInício da task em thread " << std::this_thread::get_id() << "\n";
    std::this_thread::sleep_for(std::chrono::seconds(1)); 
    std::cout << "Fim da task em thread " << std::this_thread::get_id() << "\n\n";
}

void update_transactions() {
    lock_guard<mutex> file_lock(file_mutex);
    
    // Verificar se o arquivo existe e obter o último ID
    int last_id = 0;
    bool file_exists = false;
    
    ifstream in_file("./data/transactions/transactions_test.csv");
    if (in_file.good()) {
        file_exists = true;
        string line;
        
        // Pular cabeçalho se existir
        if (getline(in_file, line)) {
            while (getline(in_file, line)) {
                if (!line.empty()) {
                    size_t pos = line.find(',');
                    if (pos != string::npos) {
                        last_id = stoi(line.substr(0, pos)) + 1;
                    }
                }
            }
        }
        in_file.close();
    }

    // Gerar dados aleatórios
    random_device rd;
    mt19937 gen(rd());
    
    vector<int> accounts = {3294, 8503, 6980, 8027, 6096};
    vector<int> recipients = {5890, 9155, 7222, 2497, 1768};
    vector<string> types = {"depósito", "pagamento"};
    vector<string> locations = {"Florianópolis", "João Pessoa", "Porto Alegre", "Macapá", "Boa Vista"};
    
    uniform_int_distribution<> acc_dist(0, accounts.size()-1);
    uniform_int_distribution<> rec_dist(0, recipients.size()-1);
    uniform_int_distribution<> type_dist(0, types.size()-1);
    uniform_int_distribution<> loc_dist(0, locations.size()-1);
    uniform_real_distribution<> amount_dist(100.0, 1000.0);
    
    // Gerar tempos aleatórios com time_end > time_start
    uniform_int_distribution<> hour_dist(0, 23);
    uniform_int_distribution<> min_sec_dist(0, 59);
    
    int start_h = hour_dist(gen);
    int start_m = min_sec_dist(gen);
    int start_s = min_sec_dist(gen);
    
    int end_h = start_h;
    int end_m = start_m + min_sec_dist(gen); // Garante que end >= start
    int end_s = start_s + min_sec_dist(gen);
    
    // Ajustar overflow
    if (end_s >= 60) { end_s -= 60; end_m++; }
    if (end_m >= 60) { end_m -= 60; end_h++; }
    if (end_h >= 24) end_h = 23;

    auto now = chrono::system_clock::now();
    time_t now_time = chrono::system_clock::to_time_t(now);
    tm* local_time = localtime(&now_time);
    
    // Abre arquivo
    ofstream file("./data/transactions/transactions_test.csv", ios::app);
    if (!file.is_open()) {
        cerr << "Erro ao abrir o arquivo transactions_test.csv" << endl;
        return;
    }
    
    // Se arquivo não existe, escreve cabeçalho
    if (!file_exists) {
        file << "transation_id,account_id,recipient_id,amount,type,location,time_start,time_end,date\n";
    }
    
    // Gera nova transação
    int acc_idx = acc_dist(gen);
    
    file << (file_exists ? "\n" : "") // Só adiciona nova linha se arquivo já existia
         << last_id << ","
         << accounts[acc_idx] << ","
         << recipients[rec_dist(gen)] << "," // Destinatário aleatório independente
         << fixed << setprecision(2) << amount_dist(gen) << ","
         << types[type_dist(gen)] << ","
         << locations[loc_dist(gen)] << ","
         << setw(2) << setfill('0') << start_h << ":"
         << setw(2) << setfill('0') << start_m << ":"
         << setw(2) << setfill('0') << start_s << ","
         << setw(2) << setfill('0') << end_h << ":"
         << setw(2) << setfill('0') << end_m << ":"
         << setw(2) << setfill('0') << end_s << ","
         << put_time(local_time, "%Y-%m-%d");
    
    file.close();
    
    lock_guard<mutex> cout_lock(cout_mutex);
    cout << "Nova transação adicionada com ID: " << last_id << endl;
}

int main() {
    const int NUM_THREADS = thread::hardware_concurrency();

    ThreadPool pool(4); 

    pool.periodic_task(666, func, 1); 
    pool.periodic_task(1, update_transactions, 5);

    std::this_thread::sleep_for(std::chrono::seconds(30));

    {
        std::lock_guard<std::mutex> lock(cout_mutex);
        std::cout << "Encerrando...\n";
    }

    // Mostra conteúdo do arquivo gerado
    {
        std::lock_guard<std::mutex> file_lock(file_mutex);
        ifstream in_file("./data/transactions/transactions_test.csv");
        std::cout << "\nConteúdo do arquivo:\n";
        std::string line;
        while (getline(in_file, line)) {
            std::cout << line << "\n";
        }
    }

    // Utilizando o CSV atualizado para fazer uma análise: Valor Médio de cada Conta
    vector<string> transactionsColTypes = {"int", "int", "int", "float", "string", "string", "string", "string"};

    DataFrame* dfNewTransactions = readCSV(1, "./data/transactions/transactions_test.csv", NUM_THREADS, transactionsColTypes, pool);
    DataFrame grouped = groupby_mean(*dfNewTransactions, 1, NUM_THREADS, "account_id", "amount", pool);
    grouped.printDF();

    return 0;
}