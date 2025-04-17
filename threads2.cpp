#include <iostream>
#include <thread>
#include <chrono>
#include <set>
#include <atomic>
#include "threads2.h"

mutex cout_mutex;

void task_1() {
    this_thread::sleep_for(chrono::seconds(3));  
    lock_guard<mutex> lock(cout_mutex);
    cout << "Tarefa pesada 1 concluída" << endl;
}

void task_2() {
    this_thread::sleep_for(chrono::seconds(3));  
    lock_guard<mutex> lock(cout_mutex);
    cout << "Tarefa pesada 2 concluída" << endl;
}

void task_3() {
    lock_guard<mutex> lock(cout_mutex);
    cout << "Tarefa leve concluída (depende das tarefas pesadas)" << endl;
}

int main() {
    /*
    Se tudo der certo, as duas primeiras vão executar primeiro que a terceira :)
    */

    ThreadPool pool(5);

    pool.enqueue(1, task_1); 
    pool.enqueue(2, task_2); 

    // Task dependente das outras 
    set<int> dependencies = {1, 2};  
    pool.enqueue(3, task_3, dependencies); 

    return 0;
}