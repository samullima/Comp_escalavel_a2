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
#include <iomanip>
#include <vector>
#include <variant>
#include <string>
#include <type_traits>

using ElementType = std::variant<int, float, bool, std::string>;

ThreadPool* mainPool;
DataFrame* TransactionsDF;
DataFrame* AccountsDF;

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
// Versão mais limpa usando ElementType
int convertToInt(const std::variant<int, float, bool, std::string>& elem) {
    return std::visit([](const auto& value) -> int {
        using T = std::decay_t<decltype(value)>;
        
        if constexpr (std::is_same_v<T, int>) {
            return static_cast<int>(value);
        }
        else if constexpr (std::is_same_v<T, float>) {
            return static_cast<int>(value); // Converte float para int
        }
        else if constexpr (std::is_same_v<T, bool>) {
            return value ? 1 : 0; // Converte bool para 0 ou 1
        }
        else if constexpr (std::is_same_v<T, std::string>) {
            try {
                return std::stoi(value); // Tenta converter string para int
            } catch (...) {
                return 0; // Valor padrão se a conversão falhar
            }
        }
    }, elem);
}


class ProcessingImpl : public ProcessingServices::Service {
    // Implement your service methods here
    ::grpc::Status addTransaction(::grpc::ServerContext* context, const ::Transaction* request, ::GenericResponse* response) {
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
        int numThreads = std::thread::hardware_concurrency();
        DataFrame summary = summaryStats(*TransactionsDF, 1, numThreads, "amount", *mainPool);
        std::cout << "Summary: " << std::endl;
        float min = std::get<float>(summary.getRecord(0)[1]);
        float q1 = std::get<float>(summary.getRecord(1)[1]);
        float median = std::get<float>(summary.getRecord(2)[1]);
        float q3 = std::get<float>(summary.getRecord(3)[1]);
        float max = std::get<float>(summary.getRecord(4)[1]);
        float mean = std::get<float>(summary.getRecord(5)[1]);
        response->set_min(min);
        response->set_q1(q1);
        response->set_median(median);
        response->set_q3(q3);
        response->set_max(max);
        response->set_mean(mean);
        return ::grpc::Status::OK;
    }
    ::grpc::Status abnormalTransactions(::grpc::ServerContext* context, const GenericInput* request, Abnormal* response) override {
        int numThreads = std::thread::hardware_concurrency();
        DataFrame abnormal = abnormal_transactions(*TransactionsDF, *AccountsDF, 2, numThreads, "transation_id", "amount", "location", "account_id", "account_id", "account_location", *mainPool);
        std::cout << "Abnormal: " << std::endl;
        std::vector<ElementType> vectorAbnormal = abnormal.getColumn(0);

        // Converter e adicionar ao response
        for (const auto& elem : vectorAbnormal) {
            response->add_vectorabnormal(convertToInt(elem));
        }

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

    ProcessingImpl service;
    grpc::ServerBuilder builder;
    builder.AddListeningPort("0.0.0.0:9999", grpc::InsecureServerCredentials());
    builder.RegisterService(&service);

    std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
    std::cout << "Server listening on port 9999" << std::endl;

    server->Wait();

    return 0;
}