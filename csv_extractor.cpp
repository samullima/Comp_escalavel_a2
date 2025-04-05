#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <variant>
#include <unordered_map>
#include <mutex>
#include <thread>
#include <iomanip>
#include <iostream>
#include <chrono>
#include "df.h"

using namespace std;

int NUM_THREADS = 4;

void readCSVLines(ifstream& file, vector<string>& linesRead, bool& fileAlreadyRead) {
    /*
    Esse método lê o arquivo CSV e preenche o vetor linesRead com as linhas lidas.
    */
    
    string line;
    bool eof = false;
    int lastIndex = 0;
    // while(!eof)
    // {
    //     vector<string> blockRead;
    //     for(int i = lastIndex; i < lastIndex + 500; i++ && getline(file, line)) {
    //         blockRead.push_back(line);
    //     }
    //     linesRead.insert(
    //         linesRead.end(),
    //         std::make_move_iterator(blockRead.begin()),
    //         std::make_move_iterator(blockRead.end())
    //       );
    // }
    while (getline(file, line)) {
        linesRead.push_back(line);
    }
    fileAlreadyRead = true;
    file.close();
    // Notifica que o arquivo foi lido
    cout << "Arquivo lido com sucesso!" << endl;
    cout << "Número de linhas lidas: " << linesRead.size() << endl;
}

void processCSVLines(const vector<string>& linesRead, DataFrame* df, int& recordsCount, bool& fileAlreadyRead, mutex& mtx) {
    /*
    Esse método processa as linhas lidas do CSV e preenche o DataFrame.
    */
   
    while(!fileAlreadyRead || recordsCount < linesRead.size()){
        bool canProceed = false;
        mtx.lock();
        canProceed = (recordsCount < linesRead.size());
        int currentLine = recordsCount;
        recordsCount = recordsCount + canProceed;
        mtx.unlock();
        if(!canProceed) {
            continue;
        }
        string line = linesRead[currentLine];
        stringstream ss(line);
        string value;
        vector<string> record;
        while (getline(ss, value, ',')) {
            record.push_back(value);
        }
        // Adiciona o registro ao DataFrame
        if(record.size() != df->numCols) {
            cerr << "Número de valores no registro " << currentLine << " não é igual ao número de colunas." << endl;
            continue;
        }
        df->addRecord(record);
    }
}

DataFrame* readCSV(const string& filename) {
    /*
    Esse método lê um arquivo CSV e preenche o DataFrame com os dados.
    O arquivo CSV deve ter o seguinte formato:
    ID, Nome, Salario
    1, "João", 3000.50
    2, "Maria", 4000.75
    3, "José", 2500.00
    */
    
    ifstream file(filename);
    if(!file.is_open()) {
        cerr << "Erro ao abrir o arquivo: " << filename << endl;
        throw runtime_error("Erro ao abrir o arquivo");
    }

    vector<string> headers;
    vector<string> colTypes;
    
    // Lê o cabeçalho
    string headerLine;
    getline(file, headerLine);
    stringstream ss(headerLine);
    string header;
    while (getline(ss, header, ',')) {
        headers.push_back(header);
        colTypes.push_back("string"); // Supondo que todos os tipos sejam string inicialmente
    }
    cout << "Colunas lidas: " << headers.size() << endl;
    DataFrame * df = new DataFrame(headers, colTypes);

    // Lê os dados
    vector<string> linesRead;
    vector<thread> threads;
    bool fileAlreadyRead = false;

    threads.push_back(thread(readCSVLines, ref(file), ref(linesRead), ref(fileAlreadyRead)));
    
    int recordsCount = 0;
    mutex mtx;
    for(int i = 1; i < NUM_THREADS; i++) {
        threads.push_back(thread(processCSVLines, ref(linesRead), df, ref(recordsCount), ref(fileAlreadyRead), ref(mtx)));
    }

    threads[0].join();
    for(int i = 1; i < NUM_THREADS; i++) {
        
        threads[i].join();
    }
    
    return df;
}

int main()
{
    // Nome do arquivo CSV
    string filename = "data.csv";
    // ifstream file(filename);
    // vector<string> linesRead;
    // bool fileAlreadyRead = false;
    // cout << fileAlreadyRead << endl;
    // // Lê o arquivo CSV
    // readCSVLines(file, linesRead, fileAlreadyRead);
    // cout << fileAlreadyRead << endl;
    

    DataFrame * df = readCSV(filename);
    // df->printDF();
    cout << "Deu certo" << endl;
    cout << "Número de colunas: " << df->numCols << endl;
    cout << "Número de registros: " << df->numRecords << endl;


    
    return 0;
}