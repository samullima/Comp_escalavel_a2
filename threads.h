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

extern std::mutex cout_mutex; // Mutex para logs

using namespace std;

// Armazena callables que não são copiáveis (lambdas que movem um std::promise).
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
    // Desabilita cópia
    MoveOnlyFunction(const MoveOnlyFunction&) = delete;
    MoveOnlyFunction& operator=(const MoveOnlyFunction&) = delete;
    R operator()(Args... args) {
        return callable->invoke(forward<Args>(args)...);
    }
};

// Conjunto fixo de threads que ficam aguardando tarefas na fila
class ThreadPool {
public:
    ThreadPool(size_t numThreads);
    
    // Método enqueue aceita qualquer callable movível
    template <typename F>
    void enqueue(F&& task);
    
    ~ThreadPool();
    
    size_t size() const;
    
private:
    vector<thread> workers;
    queue<MoveOnlyFunction<void()>> tasks;
    mutable mutex queue_mutex;
    condition_variable condition;
    atomic<bool> stop;
};

inline ThreadPool::ThreadPool(size_t numThreads) : stop(false) {
    for (size_t i = 0; i < numThreads; ++i) {
        workers.emplace_back([this] {
            {
                // Exibe log quando a thread do pool é criada
                lock_guard<mutex> lock(cout_mutex);
                stringstream ss;
                ss << "[POOL INIT] Thread criada: ID = " << this_thread::get_id() << endl;
                cout << ss.str();
            }
            while (true) {
                MoveOnlyFunction<void()> task;
                {
                    unique_lock<mutex> lock(queue_mutex);
                    condition.wait(lock, [this] {
                        return stop || !tasks.empty();
                    });
                    if (stop && tasks.empty())
                        return;
                    task = move(tasks.front());
                    tasks.pop();
                }
                task();
            }
        });
    }
}

template <typename F>
void ThreadPool::enqueue(F&& task) {
    {
        lock_guard<mutex> lock(queue_mutex);
        tasks.emplace(MoveOnlyFunction<void()>(forward<F>(task)));
    }
    condition.notify_one();
}

inline ThreadPool::~ThreadPool() {
    {
        lock_guard<mutex> lock(queue_mutex);
        stop = true;
    }
    condition.notify_all();
    for (thread &worker : workers) {
        if (worker.joinable())
            worker.join();
    }
}

inline size_t ThreadPool::size() const {
    return workers.size();
}

#endif 
