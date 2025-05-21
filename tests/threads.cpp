#include "../include/threads.h"
#include <chrono>
#include <thread>
#include <mutex>

using namespace std;
std::mutex cout_mutex; 

void func() {
    std::lock_guard<std::mutex> lock(cout_mutex);
    std::cout << "\nInício da task em thread " << std::this_thread::get_id() << "\n";
    std::this_thread::sleep_for(std::chrono::seconds(1)); 
    std::cout << "Fim da task em thread " << std::this_thread::get_id() << "\n\n";
}

int main() {
    ThreadPool pool(4); 

    pool.periodic_task(666, func, 1); 

    std::this_thread::sleep_for(std::chrono::seconds(10));

    {
        std::lock_guard<std::mutex> lock(cout_mutex);
        std::cout << "Encerrando...\n";
    }

    return 0;
}