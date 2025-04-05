#include <mutex>
#include <iostream>

using namespace std;

class MinhaClasse {
private:
    int x;
    int y;

    mutable mutex mutex_x;
    mutable mutex mutex_y;

public:
    MinhaClasse(int x_inicial = 0, int y_inicial = 0)
        : x(x_inicial), y(y_inicial) {}

    void setX(int valor) {
        lock_guard<mutex> lock(mutex_x);
        x = valor;
    }

    int getX() const {
        lock_guard<mutex> lock(mutex_x);
        return x;
    }

    void setY(int valor) {
        lock_guard<mutex> lock(mutex_y);
        y = valor;
    }

    int getY() const {
        lock_guard<mutex> lock(mutex_y);
        return y;
    }

    // Método que retorna uma nova instância com x+1 e y+1
    MinhaClasse novaClasseIncrementada() const {
        int novo_x, novo_y;

        {
            lock_guard<mutex> lock_x(mutex_x);
            novo_x = x + 1;
        }
        {
            lock_guard<mutex> lock_y(mutex_y);
            novo_y = y + 1;
        }

        return MinhaClasse(novo_x, novo_y);
    }

    void print() const {
        cout << "x = " << getX() << ", y = " << getY() << endl;
    }
};

int main() {
    MinhaClasse a(10, 20);
    a.print();  // x = 10, y = 20

    MinhaClasse b = a.novaClasseIncrementada();
    b.print();  // x = 11, y = 21

    return 0;
}