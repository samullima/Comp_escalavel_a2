#include <iostream>
#include <string>
#include <thread>
#include <grpcpp/grpcpp.h>
#include <chrono>

#include "proto/dataframe.grpc.pb.h"
#include "proto/dataframe.pb.h"
#include "df.h"
#include "csv_extractor.h"
#include "tratadores.h"

ThreadPool* mainPool;
DataFrame* TransactionsDF;
vector<float> amounts;
int NUM_RECORDS = 0;
float SUM_AMOUNTS = 0.0;
DataFrame* Classificador;
DataFrame* CountClasses;


/*
    TransactionsDF has the following columns:
    transation_id,account_id,recipient_id,amount,type,location,time_start,time_end,date
*/

std::string getCurrentDate(){
    auto now = std::chrono::system_clock::now();
    auto in_time_t = std::chrono::system_clock::to_time_t(now);

    std::stringstream ss;
    ss << std::put_time(std::localtime(&in_time_t), "%Y-%m-%d");
    return ss.str();
}
std::string getCurrentTime(){
    auto now = std::chrono::system_clock::now();
    auto time = std::chrono::system_clock::to_time_t(now);
    auto localTime = std::localtime(&time);

    auto result = std::put_time(localTime, "%H:%M:%S");
    
    std::ostringstream oss;
    oss << result;
    return oss.str();

    return 0;
}


class ProcessingImpl : public ProcessingServices::Service {
    // Implement your service methods here
    ::grpc::Status addTransaction(::grpc::ServerContext* context, const ::Transaction* request, ::GenericResponse* response) {
        auto pos = std::lower_bound(amounts.begin(), amounts.end(), request->amount());
        amounts.insert(pos, request->amount());

        SUM_AMOUNTS += request->amount();
        NUM_RECORDS++;

        // Handle the addTransaction request
        int newID = TransactionsDF->getNumRecords()+1;
        int senderID = request->idsender();
        int receiverID = request->idreceiver();
        float amount = request->amount();
        string type = "pagamento";
        string location = request->location();
        // Now
        string time_start = getCurrentTime();
        string time_end = getCurrentTime();
        // Today date YY MM DD
        string date = getCurrentDate();
        vector<string> record = {std::to_string(newID), std::to_string(senderID), std::to_string(receiverID), std::to_string(amount), type, location, time_start, time_end, date};
        TransactionsDF->addRecord(record);

        std::cout << "Transaction added: " << newID << std::endl;
        std::cout << "location: " << location << std::endl;

        return ::grpc::Status::OK;
    }
    ::grpc::Status transactionsInfo(::grpc::ServerContext* context, const GenericInput* request, Summary* response) override {
        // Handle the transactionsInfo request

        float medianValue;
        int q1Index, q3Index;

        if (amounts.size() % 2 == 0) {
            // Even number of elements
            medianValue = (amounts[amounts.size() / 2 - 1] + amounts[amounts.size() / 2]) / 2;
        } else {
            // Odd number of elements
            medianValue = amounts[amounts.size() / 2];
        }

        q1Index = static_cast<int>(0.25 * amounts.size());
        q3Index = static_cast<int>(0.75 * amounts.size());

        float min = amounts[0];
        float q1 = amounts[q1Index];
        float median = medianValue;
        float q3 = amounts[q3Index];
        float max = amounts[amounts.size() - 1];
        float mean = SUM_AMOUNTS / NUM_RECORDS;

        cout << "Transactions Info:" << std::endl;
        cout << "------------------------" << endl;
        cout << "Min: " << min << std::endl;
        cout << "Q1: " << q1 << std::endl;
        cout << "Median: " << median << std::endl;
        cout << "Q3: " << q3 << std::endl;
        cout << "Max: " << max << std::endl;
        cout << "Mean: " << mean << std::endl;

        response->set_min(min);
        response->set_q1(q1);
        response->set_median(median);
        response->set_q3(q3);
        response->set_max(max);
        response->set_mean(mean);
        return ::grpc::Status::OK;
    }
    ::grpc::Status accountClass(::grpc::ServerContext* context, const ::AccountId* request, ::Class* response) {
        int ID = request->idaccount();
        int n = Classificador.getNumRecords();
        int accountIdx = Classificador.getColumnIndex("account_id");
        int classIdx = Classificador.getColumnIndex("categoria");
        auto idCol = Classificador.getColumn(accountIdx);
        auto classCol = Classificador.getColumn(classIdx);
        string classe;
        for (i=0; i<n; i++){
            if (idCol[i] == ID) {
                classe = classCol[i];
            }
        }
        response->set_class(classe);
        
        return ::grpc::Status::OK;
    }
    ::grpc::Status accountByClass(::grpc::ServerContext* context, const ::Class* request, ::NumberOfAccounts* response) {
        string classe = request->class();
        int n = CountClasses.getNumRecords();
        int contadorIdx = CountClasses.getColumnIndex("count");
        int classIdx = CountClasses.getColumnIndex("categoria");
        auto contadorCol = CountClasses.getColumn(contadorIdx);
        auto classCol = CountClasses.getColumn(classIdx);
        int numClasses;
        for (i=0; i<n; i++){
            if (classCol[i] == classe) {
                numClasses = contadorCol[i];
            }
        }
        response->set_class(numClasses);
        
        return ::grpc::Status::OK;
    }
};

int main(int argc, char** argv) {
    int MAX_THREADS = thread::hardware_concurrency();
    vector<string> transactionsColTypes = {"int", "int", "int", "float", "string", "string", "string", "string"};
    vector<string> accountsColTypes = {"int", "int", "float", "string", "string", "string", "string"};
    vector<string> customersColTypes = {"int", "string", "string", "string", "string"};

    mainPool = new ThreadPool(MAX_THREADS);
    TransactionsDF = readCSV("../../data/transactions/transactions.csv", MAX_THREADS, transactionsColTypes);
    AccountsDF = readCSV("../../data/accounts/accounts.csv", MAX_THREADS, accountsColTypes);
    MediasTrans = groupby_mean(*TransactionsDF, 5, MAX_THREADS, "account_id", "amount", mainPool);
    joined = join_by_key(MediasTrans, *AccountsDF, 6, MAX_THREADS, "account_id", mainPool);
    Classificador = classify_accounts_parallel(joined, 7, MAX_THREADS, "B_customer_id", "A_mean_amount", "B_current_balance", mainPool);
    CountClasses = count_values(Classificador, 10, MAX_THREADS, "categoria", 0, mainPool);

    auto column = TransactionsDF->getColumn(3); 
    amounts.clear();
    amounts.reserve(column.size());
    for (const auto& value : column) {
        amounts.push_back(std::get<float>(value));
    }

    sort(amounts.begin(),amounts.end());
    NUM_RECORDS = TransactionsDF->getNumRecords();
    for (const auto& amount : amounts) {
        SUM_AMOUNTS += amount;
    }

    ProcessingImpl service;
    grpc::ServerBuilder builder;
    builder.AddListeningPort("0.0.0.0:9999", grpc::InsecureServerCredentials());
    builder.RegisterService(&service);

    std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
    std::cout << "Server listening on port 9999" << std::endl;

    server->Wait();

    return 0;
}
