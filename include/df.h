#ifndef DF_H
#define DF_H

#include <vector>
#include <string>
#include <variant>
#include <unordered_map>
#include <mutex>
#include <thread>
#include <deque>
#include <fstream>
#include <sstream>
#include <memory>

using namespace std;
using ElementType = variant<int, float, bool, string>; // Tipo genérico para os dados 


class DataFrame {
    /*
    Essa classe representa um DataFrame base.
    */
    public:
        // Construtor
        DataFrame(const vector<string>& colNames, const vector<string>& colTypes);
        DataFrame(const DataFrame& other);

        // Destrutor
        ~DataFrame();

        vector<vector<ElementType>> columns; 

        // Metadados
        int numRecords = 0; 
        int numCols = 0; 
        vector<string> colNames;
        unordered_map<string, int> idxColumns;
        unordered_map<string, string> colTypes;

        // Métodos
        void addColumn(const vector<ElementType>& col, string colName, string colType);
        void addRecord(const vector<string>& record);
        DataFrame getRecords(const vector<int>& indexes) const;
        void printDF();
        void DFtoCSV(string csvName);
        
        // Retorna o registro (linha) i como vetor de ElementType
        vector<ElementType> getRecord(int i) const;

        // Retorna os nomes das colunas
        vector<string> getColumnNames() const;

        // Retorna o número de colunas
        int getNumCols() const;

        // Retorna a coluna i como vetor de ElementType
        vector<ElementType> getColumn(int i) const;

        // Retorna o mapa de tipos das colunas
        unordered_map<string, string> getColumnTypes() const;

        // Retorna o número de registros (linhas)
        int getNumRecords() const;

        // Retorna o índice da coluna
        int getColumnIndex(const string&) const;

    private:
        // Concorrência 
        mutable mutex mutexDF;
        deque<mutex> columnMutexes;
        deque<mutex> rowMutexes;
};

inline string variantToString(const ElementType& val) {
    /*Função auxiliar para alterar o tipo variant para string.*/
    return visit([](const auto& arg) -> string {
        if constexpr (is_same_v<decay_t<decltype(arg)>, string>)
            return arg;
        else
            return to_string(arg);
    }, val);
}

string variantToString(const ElementType& val);
#endif