#ifndef SQL_EXTRACTOR_H
#define SQL_EXTRACTOR_H

#include "threads.h"
DataFrame * readDB(const string& filename, string tableName, int numThreads, vector<string> colTypes);
DataFrame * readDB(int id, const string& filename, string tableName, int numThreads, vector<string> colTypes, ThreadPool& pool);

#endif