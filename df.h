#include <vector>
#include <string>
#include <variant>
#include <unordered_map>
#include <mutex>
#include <thread>

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

    private:
        // Concorrência 
        mutable mutex mutexDF;
        mutable vector<mutex> columnMutexes;
        mutable vector<mutex> rowMutexes;
};
