
void readCSVLines(ifstream& file, vector<string>& linesRead, bool& fileAlreadyRead, mutex& mtxFile);
void processCSVLines(const vector<string>& linesRead, DataFrame* df, int& recordsCount, bool& fileAlreadyRead, mutex& mtxFile, mutex& mtxCounter);
DataFrame* readCSV(const string& filename, int numThreads);