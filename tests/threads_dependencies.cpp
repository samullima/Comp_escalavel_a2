#include <iostream>
#include <thread>
#include <chrono>
#include "..\include\threads.h"

mutex cout_mutex;

int tratador1(int& num1) {
    for(int i = 0; i < 100000; i++) {
        //if (i < 10) cout << i << endl;
        num1++;
    }
    {
        lock_guard<mutex> lock(cout_mutex);
        cout << "Tratador1: " << num1 << endl;
    }
    return num1;
}

int tratador2(int& num2) {
    for(int i = 0; i < 100000; i++) {
        //if (i < 10) cout << i << endl;
        num2++;
    }
    {
        lock_guard<mutex> lock(cout_mutex);
        cout << "Tratador2: " << num2 << endl;
    }
    return num2;
}

int tratador3(int& num1, int& num2) {
    {
        lock_guard<mutex> lock(cout_mutex);
        cout << "Tratador3: " << num1 + num2 << endl;
    }
    return num1 + num2;
}

int main() {
    ThreadPool pool(5);

    int a = 0;
    int b = 0;

    auto fut1 = pool.enqueue(1, [&a]() { return tratador1(a); });
    auto fut2 = pool.enqueue(2, [&b]() { return tratador2(b); });
    pool.isReady(1);
    pool.isReady(2);
    int result1 = fut1.get();
    int result2 = fut2.get();

    auto fut3 = pool.enqueue(3, [&a, &b]() { return tratador3(a, b); });
    pool.isReady(3);
    int result3 = fut3.get();

    cout << "Result1 = " << result1 << endl;
    cout << "Result2 = " << result2 << endl;
    cout << "Result3 = " << result3 << endl;

    return 0;
}