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
#include <future>
#include <utility>
#include <sstream>
#include <iostream>

extern std::mutex cout_mutex; // Mutex para logs

using namespace std;

// Encapsula funções que podem ser movidas (sem cópias)
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
    public:
        ThreadPool(size_t numThreads);

        // Enfileira uma task com um ID, retorna um future para sincronização
        template <typename F>
        auto enqueue(int id, F&& task) -> std::future<decltype(task())>;

        // Informa ao pool que a task com ID está liberada para execução
        void isReady(int id);

        ~ThreadPool();
        
        size_t size() const;

    private:
        struct Task {
            MoveOnlyFunction<void()> func;      // Função para executar
            int id;                             // ID associado da Task
            Task(MoveOnlyFunction<void()> f, int i) : func(move(f)), id(i) {}
        };

        vector<thread> workers;                 // Threads do pool
        queue<MoveOnlyFunction<void()>> tasks;  // Tasks liberadas para execução
        vector<Task> waitingTasks;              // Tasks esperando para serem liberadas
        mutable mutex queue_mutex;              // Mutex de acesso às filas/vetores
        condition_variable condition;           // Sincronização da produção/consumo de tasks
        atomic<bool> stop;                      // Flag para parada do pool
    };

// Construtor
inline ThreadPool::ThreadPool(size_t numThreads) : stop(false) {
    for (size_t i = 0; i < numThreads; ++i) {
        workers.emplace_back([this] {
            while (true) {
                MoveOnlyFunction<void()> task;
                {
                    unique_lock<mutex> lock(queue_mutex);
                    
                    // Espera até haver uma task pronta ou o pool ser destruído
                    condition.wait(lock, [this] {
                        return stop || !tasks.empty();
                    });

                    if (stop && tasks.empty())
                        return;

                    task = move(tasks.front());
                    tasks.pop();
                }
                try{
                    task();
                }catch (std::exception& e){
                    cout<<"running task, with exception..."<<e.what()<<endl;
                    // return;
                }
            }
        });
    }
}

template <typename F>
auto ThreadPool::enqueue(int id, F&& task) -> std::future<decltype(task())> {
    /*
    Enfileira uma função para execução futura, associando a um ID.
    Inicialmente vai para a fila de espera até ser liberada via isReady.
    */
    using RetType = decltype(task());

    auto promise = std::make_shared<std::promise<RetType>>();
    auto future = promise->get_future();

    {
        lock_guard<mutex> lock(queue_mutex);
        waitingTasks.emplace_back(
            MoveOnlyFunction<void()>([task = std::move(task), promise]() mutable {
                if constexpr (std::is_void_v<RetType>) {
                    task();
                    promise->set_value();
                } else {
                    promise->set_value(task());
                }
            }),
            id
        );
    }

    return future;
}



inline void ThreadPool::isReady(int id) {
    /*
    Libera a execução das tasks que possuem o ID informado.
    Move elas da fila de espera para a fila de tasks prontas.
    */
    lock_guard<mutex> lock(queue_mutex);
    auto it = waitingTasks.begin();
    while (it != waitingTasks.end()) {
        if (it->id == id) {
            tasks.emplace(move(it->func)); // Move para a fila de execução
            it = waitingTasks.erase(it);   // Remove da fila de espera
        } else {
            ++it;
        }
    }
    // Acorda todas as threads para pegar novas tasks
    condition.notify_all();         
}

// Destrutor
inline ThreadPool::~ThreadPool() {
    {
        lock_guard<mutex> lock(queue_mutex);
        stop = true; 
    }
    // Acorda todas as threads para terminarem
    condition.notify_all(); 
    for (thread &worker : workers) {
        if (worker.joinable())
            worker.join(); 
    }
}

inline size_t ThreadPool::size() const {
    return workers.size();
}

#endif // THREADS_H