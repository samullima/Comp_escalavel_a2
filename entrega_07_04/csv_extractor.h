#ifndef CSV_EXTRACTOR_H
#define CSV_EXTRACTOR_H
void readCSVLines(ifstream& file, vector<string>& linesRead, bool& fileAlreadyRead, mutex& mtxFile);
void processCSVLines(const vector<string>& linesRead, DataFrame* df, int& recordsCount, bool& fileAlreadyRead, mutex& mtxFile, mutex& mtxCounter);
void processCSVBlocks(const vector<string>& linesRead, DataFrame* df, int& recordsCount, bool& fileAlreadyRead, mutex& mtxFile, mutex& mtxCounter, int blocksize);
DataFrame* readCSV(const string& filename, int numThreads, vector<string> colTypes);
#endif