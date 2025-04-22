#ifndef CSV_EXTRACTOR_H
#define CSV_EXTRACTOR_H

#include "threads.h"
void readCSVLines(ifstream& file, vector<string>& linesRead, bool& fileAlreadyRead, mutex& mtxFile, int& needLines, int blocksize);
void processCSVLines(const vector<string>& linesRead, DataFrame* df, int& recordsCount, bool& fileAlreadyRead, mutex& mtxFile, mutex& mtxCounter, int& needLines);
void processCSVBlocks(const vector<string>& linesRead, DataFrame* df, int& recordsCount, bool& fileAlreadyRead, mutex& mtxFile, mutex& mtxCounter, int& needLines, int blocksize);
DataFrame* readCSV(const string& filename, int numThreads, vector<string> colTypes);
DataFrame* readCSV(int id, const string& filename, int numThreads, vector<string> colTypes, ThreadPool& pool);
#endif