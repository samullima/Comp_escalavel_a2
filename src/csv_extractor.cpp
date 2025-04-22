#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <variant>
#include <unordered_map>
#include <mutex>
#include <thread>
#include <iomanip>
#include <chrono>
#include "include/df.h"
#include "include/csv_extractor.h"

using namespace std;

int STORAGE_BLOCKSIZE = 30000;
int PROCESS_BLOCKSIZE = 1000;

void readCSVLines(ifstream& file, vector<string>& linesRead, bool& fileAlreadyRead, mutex& mtxFile, int& needLines, int blocksize) {
    /*
    Esse método lê o arquivo CSV e preenche o vetor linesRead com as linhas lidas.
    */
    
    string line;
    while(!file.eof())
    {
        vector<string> blockRead;
        // for(int i = 0; i < blocksize && getline(file, line); i++) {
        //     blockRead.push_back(line);
        // }
        while(needLines < 1 || blockRead.size() < blocksize) {
            if(getline(file, line))
                blockRead.push_back(line);
            else
                break;
        }
        mtxFile.lock();
        linesRead.insert(
            linesRead.end(),
            std::make_move_iterator(blockRead.begin()),
            std::make_move_iterator(blockRead.end())
          );
        needLines = -1;
        mtxFile.unlock();
    }
    
    fileAlreadyRead = true;
    file.close();
    // Notifica que o arquivo foi lido
    cout << "Arquivo lido com sucesso!" << endl;
    cout << "Número de linhas lidas: " << linesRead.size() << endl;
}

// Método linha por linha
void processCSVLines(const vector<string>& linesRead, DataFrame* df, int& recordsCount, bool& fileAlreadyRead, mutex& mtxFile, mutex& mtxCounter, int& needLines) {
    /*
    Esse método processa as linhas lidas do CSV e preenche o DataFrame.
    */
   
    while(!fileAlreadyRead || recordsCount < linesRead.size()){
        bool canProceed = false;
        mtxCounter.lock();
        canProceed = (recordsCount < linesRead.size());
        int currentLine = recordsCount;
        recordsCount = recordsCount + canProceed;
        mtxCounter.unlock();
        
        if(!canProceed) {
            continue;
        }

        if(!fileAlreadyRead)
            mtxFile.lock();
        string line = linesRead[currentLine];
        mtxFile.unlock();
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

// Método em bloco
void processCSVBlocks(const vector<string>& linesRead, DataFrame* df, int& recordsCount, bool& fileAlreadyRead, mutex& mtxFile, mutex& mtxCounter, int& needLines, int blocksize) {
    /*
    Esse método processa as linhas lidas do CSV e preenche o DataFrame.
    */
   
    while(!fileAlreadyRead || recordsCount < linesRead.size()){
        int beginning = 0;
        int lastLine = 0;
        mtxCounter.lock();
        if(recordsCount < linesRead.size()) {
            beginning = recordsCount;
            if(recordsCount + blocksize < linesRead.size())
                lastLine = recordsCount + blocksize;
            else
                lastLine = linesRead.size();
            recordsCount = lastLine;
        }
        else{
            if(needLines < 1 && !fileAlreadyRead)
            {
                needLines++;
            }
            mtxCounter.unlock();
            continue;
        }
        mtxCounter.unlock();

        if(!fileAlreadyRead)
            mtxFile.lock();
        vector<string>::const_iterator first = linesRead.begin() + beginning;
        vector<string>::const_iterator last = linesRead.begin() + lastLine;
        vector<string> blockRead(first, last);
        mtxFile.unlock();
        vector<vector<string>> filteredBlockRead(blockRead.size());

        for(int i = 0; i < blockRead.size(); i++) {
            string line = blockRead[i];
            stringstream ss(line);
            string value;
            vector<string> record;
            while (getline(ss, value, ',')) {
                record.push_back(value);
            }
            filteredBlockRead[i] = record;
        }
        df->addMultipleRecords(filteredBlockRead);
    }
}


DataFrame* readCSV(const string& filename, int numThreads, vector<string> colTypes) {
    /*
    Esse método lê um arquivo CSV e preenche o DataFrame com os dados.
    O arquivo CSV deve ter o seguinte formato:
    ID, Nome, Salario
    1, "João", 3000.50
    2, "Maria", 4000.75
    3, "José", 2500.00
    */
    if(numThreads < 2) numThreads = 2;
    ifstream file(filename);
    if(!file.is_open()) {
        cerr << "Erro ao abrir o arquivo: " << filename << endl;
        throw runtime_error("Erro ao abrir o arquivo");
    }

    vector<string> headers;
    // vector<string> colTypes = {"int", "int", "int", "float", "string", "string", "string", "string"};
    
    // Lê o cabeçalho
    string headerLine;
    getline(file, headerLine);
    stringstream ss(headerLine);
    string header;
    while (getline(ss, header, ',')) {
        headers.push_back(header);
        if(colTypes.size() < headers.size()) colTypes.push_back("string"); // Supondo que todos os tipos sejam string inicialmente
    }
    cout << "Colunas lidas: " << headers.size() << endl;
    DataFrame * df = new DataFrame(headers, colTypes);

    // Lê os dados
    vector<string> linesRead;
    vector<thread> threads;
    bool fileAlreadyRead = false;
    mutex mtxFile;
    mutex mtxCounter;
    int needLines = 0; // 0 = False, 1 = True, -1 = False e recentemente lido

    chrono::high_resolution_clock::time_point startRead = chrono::high_resolution_clock::now();
    threads.push_back(thread(readCSVLines, ref(file), ref(linesRead), ref(fileAlreadyRead), ref(mtxFile), ref(needLines), PROCESS_BLOCKSIZE*numThreads));
    
    chrono::high_resolution_clock::time_point startProcess = chrono::high_resolution_clock::now();
    int recordsCount = 0;
    for(int i = 1; i < numThreads; i++) {
        // threads.push_back(thread(processCSVLines, ref(linesRead), df, ref(recordsCount), ref(fileAlreadyRead), ref(mtxFile),ref(mtxCounter), ref(needLines)));
        threads.push_back(thread(processCSVBlocks, ref(linesRead), df, ref(recordsCount), ref(fileAlreadyRead), ref(mtxFile),ref(mtxCounter), ref(needLines), PROCESS_BLOCKSIZE));
    }

    threads[0].join();
    chrono::high_resolution_clock::time_point endRead = chrono::high_resolution_clock::now();
    for(int i = 1; i < numThreads; i++) {
        threads[i].join();
    }
    chrono::high_resolution_clock::time_point endProcess = chrono::high_resolution_clock::now();
    auto durationRead = chrono::duration_cast<chrono::milliseconds>(endRead - startRead);
    auto durationProcess = chrono::duration_cast<chrono::milliseconds>(endProcess - startProcess);
    cout << "Tempo de leitura: " << durationRead.count() << " ms" << endl;
    cout << "Tempo de processamento: " << durationProcess.count() << " ms" << endl;
    
    return df;
}