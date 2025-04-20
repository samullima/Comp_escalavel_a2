#ifndef THREADS_H
#define THREADS_H

#include <queue>
#include <vector>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <functional>
#include <memory>
#include <utility>
#include <sstream>
#include <iostream>
#include <unordered_map>
#include <set>

using namespace std;
extern mutex cout_mutex; // Mutex para logs

// Classe que encapsula funções que podem ser movidas (sem cópias)
template<typename T>
class MoveOnlyFunction; 

template<typename R, typename... Args>
class MoveOnlyFunction<R(Args...)> {
    struct CallableBase {
        virtual R invoke(Args&&... args) = 0;
        virtual ~CallableBase() = default;
    };
    template<typename F>
    struct Callable : CallableBase {
        F f;
        explicit Callable(F&& func) : f(move(func)) {}
        R invoke(Args&&... args) override {
            return f(forward<Args>(args)...);
        }
    };
    unique_ptr<CallableBase> callable;
public:
    MoveOnlyFunction() = default;
    template<typename F>
    MoveOnlyFunction(F&& f) : callable(make_unique<Callable<F>>(forward<F>(f))) {}
    MoveOnlyFunction(MoveOnlyFunction&&) = default;
    MoveOnlyFunction& operator=(MoveOnlyFunction&&) = default;
    MoveOnlyFunction(const MoveOnlyFunction&) = delete;
    MoveOnlyFunction& operator=(const MoveOnlyFunction&) = delete;
    R operator()(Args... args) {
        return callable->invoke(forward<Args>(args)...);
    }
};


class ThreadPool {
    /*
    Gerencia um conjunto de threads e permite enfileirar tasks com dependências.
    */
public:
    ThreadPool(size_t numThreads);
    ~ThreadPool();

    // Função para enfileirar tasks, associando uma ID, uma função e dependências (opcionais)
    template <typename F>
    void enqueue(int id, F&& task, const set<int>& dependencies = {});

    size_t size() const;

    bool getActiveThreads() const {
        return activeThreads;
    }    

private:
    struct Task {
        MoveOnlyFunction<void()> func;  // Função a ser executada
        set<int> dependencies;          // Dependências das outras tasks (IDs)
    };

    vector<thread> workers;                  // Threads trabalhadoras
    unordered_map<int, Task> tasks;          // Hash de tasks indexadas por ID
    unordered_map<int, set<int>> dependents; // Mapeia cada task para quem depende dela
    queue<int> readyTasks;                   // IDs prontos para execução
    mutable mutex queueMutex;                // Mutex para controlar o acesso à fila de tasks
    condition_variable condition;            // Variável de condição para a sincronização
    atomic<bool> stop{false};                // Flag para parar o pool de threads  
    atomic<size_t> activeThreads{0};         // Num. Threads Ativas

    void task_finished(int finishedID);
};

// Construtor
inline ThreadPool::ThreadPool(size_t numThreads) {
    for (size_t i = 0; i < numThreads; ++i) {
        workers.emplace_back([this] {
            {
                // Criação das Threads
                lock_guard<mutex> lock(cout_mutex);
                // stringstream ss;
                // ss << "[POOL INIT] Thread criada: ID = " << this_thread::get_id() << endl;
                // cout << ss.str() << endl;
            }

            while (true) {
                int taskID;
                {
                    unique_lock<mutex> lock(queueMutex);

                    // Espera até que haja uma task ou o pool seja parado
                    condition.wait(lock, [this] {
                        return stop || !readyTasks.empty();
                    });

                    // Se o pool for parado e não houver tasks, sai
                    if (stop && readyTasks.empty())
                        return;

                    taskID = readyTasks.front();    // Pega a ID da task pronta
                    readyTasks.pop();               // Remove da fila
                    activeThreads++;                // Aumenta o num. de threads ativas
                }

                tasks[taskID].func();   // Executa a função associada
                task_finished(taskID);  // Marca como terminada
                activeThreads--;        // Temos uma thread livre agora
            }
        });
    }
}

template <typename F>
void ThreadPool::enqueue(int id, F&& task_func, const set<int>& dependencies) {
    /*
    Aciona na fila uma task com ID e dependências (se existir).
    */
    {
        lock_guard<mutex> lock(queueMutex);

        // Adiciona a task
        tasks[id] = Task{MoveOnlyFunction<void()>(forward<F>(task_func)), dependencies};

        // Atualiza dependents: para cada dependência da task, anota que ela depende
        for (int dep : dependencies) {
            dependents[dep].insert(id);
        }

        // Se não há dependências, a task pode ser executada imediatamente
        if (dependencies.empty()) {
            readyTasks.push(id); 
            condition.notify_one();
        }
    }
}

inline void ThreadPool::task_finished(int finishedID) {
    /*
    Função chamada quando uma task termina. Ela remove a task concluída das dependências das outras.
    */
    lock_guard<mutex> lock(queueMutex);

    auto it = dependents.find(finishedID);

    if (it != dependents.end()) {
        for (int dependentID : it->second) {
            auto& dependentTask = tasks[dependentID];
            dependentTask.dependencies.erase(finishedID);

            if (dependentTask.dependencies.empty()) {
                readyTasks.push(dependentID);
                condition.notify_one();
            }
        }
        // Limpa dependentes dessa task terminada
        dependents.erase(it); 
    }

    // Remove a task concluída da memória
    tasks.erase(finishedID);
}

// Destrutor
inline ThreadPool::~ThreadPool() {
    {
        lock_guard<mutex> lock(queueMutex);
        stop = true;
    }
    // Notifica todas as threads para saírem
    condition.notify_all();
    for (thread &worker : workers) {
        if (worker.joinable())
            worker.join();
    }
}

inline size_t ThreadPool::size() const {
    // Retorna o número de threads trabalhando.
    return workers.size();
}

#endif // THREADS_H
