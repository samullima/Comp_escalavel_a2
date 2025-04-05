#ifndef DF_H
#define DF_H

#include <vector>
#include <string>
#include <variant>
#include <unordered_map>
#include <mutex>
#include <thread>

using namespace std;
using ElementType = variant<int, float, bool, string>;

class DataFrame {
public:
    // Construtor e destrutor
    DataFrame(const vector<string>& colNames, const vector<string>& colTypes);
    ~DataFrame();

    // Delete operações de cópia
    DataFrame(const DataFrame&) = delete;
    DataFrame& operator=(const DataFrame&) = delete;

    // Move constructor e move assignment operator definidos manualmente
    DataFrame(DataFrame&& other) noexcept;
    DataFrame& operator=(DataFrame&& other) noexcept;

    // Métodos principais
    void addColumn(const vector<ElementType>& col, string colName, string colType);
    void addRecord(const vector<string>& record);
    DataFrame getRecords(const vector<int>& indexes);
    void printDF() const;

    // Métodos de acesso
    vector<ElementType> getRecord(int i) const;
    vector<string> getColumnNames() const;
    int getNumCols() const;
    vector<ElementType> getColumn(int i) const;
    unordered_map<string, string> getColumnTypes() const;
    int getNumRecords() const;

private:
    mutable mutex df_mutex;
    vector<vector<ElementType>> columns;
    vector<string> colNames;
    unordered_map<string, int> idxColumns;
    unordered_map<string, string> colTypes;
    int numRecords = 0;
    int numCols = 0;

    string variantToString(const ElementType& val) const;
};

#endif
