#include <iostream>
#include <string>
#include "df.h"
#include "csv_extractor.h"

int NUM_THREADS = 2;

int main(int argc, char* argv[]) {
    // Verifica se o número de threads foi passado como argumento
    if (argc > 1) {
        NUM_THREADS = atoi(argv[1]);
        if (NUM_THREADS < 2) {
            cout << "Número de threads deve ser maior ou igual a 2. Usando 2 threads." << endl;
            NUM_THREADS = 2;
        }
    }
    cout << "Número de threads: " << NUM_THREADS << endl;

    // Nome do arquivo CSV
    string filename = "data.csv";    

    DataFrame * df = readCSV(filename, NUM_THREADS);
    // df->printDF();
    cout << "Deu certo" << endl;
    cout << "Número de colunas: " << df->numCols << endl;
    cout << "Número de registros: " << df->numRecords << endl;

    return 0;
}