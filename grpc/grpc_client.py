import grpc
from typing import Any
from dataframe_pb2 import (
    Element, Column, DataFrame,
    ClassifyAccountsRequest, SummaryStatsRequest,
    Top10CidadesRequest, AbnormalTransactionsRequest,
    AddAccountRequest, AddTransactionRequest, OperationResponse
)
from dataframe_pb2_grpc import DataFrameServiceStub


def create_element(value: Any) -> Element:
    if isinstance(value, int):
        return Element(int_val=value)
    if isinstance(value, float):
        return Element(float_val=value)
    if isinstance(value, bool):
        return Element(bool_val=value)
    if isinstance(value, str):
        return Element(string_val=value)
    raise ValueError(f"Unsupported type: {type(value)}")


def create_column(name: str, dtype: str, values: list[Any]) -> Column:
    return Column(name=name, type=dtype, values=[create_element(v) for v in values])


def create_dataframe(columns: list[Column]) -> DataFrame:
    num_records = len(columns[0].values) if columns else 0
    return DataFrame(numRecords=num_records, numCols=len(columns), columns=columns)


class GRPCClient:
    def __init__(self, address: str = "localhost:50051") -> None:
        self.channel = grpc.insecure_channel(address)
        self.stub = DataFrameServiceStub(self.channel)

    def classify_accounts(self, df: DataFrame, id: int, num_threads: int, id_col: str,
                          c1: str, c2: str) -> str:
        req = ClassifyAccountsRequest(
            df=df, id=id, numThreads=num_threads,
            idCol=id_col, classFirst=c1, classSec=c2
        )
        # Aqui precisamos criar uma função que tome o df retornado e dê uma string
        response: OperationResponse = self.stub.classify_accounts_parallel(req)
        return response.result

    def summary_stats(self, df: DataFrame, id: int, num_threads: int, col: str) -> str:
        req = SummaryStatsRequest(df=df, id=id, numThreads=num_threads, colName=col)
        # Aqui precisamos criar uma função que tome o df retornado e dê uma string
        response: OperationResponse = self.stub.summaryStats(req)
        return response.result

    def top10_cidades(self, df: DataFrame, id: int, num_threads: int, col: str) -> str:
        req = Top10CidadesRequest(df=df, id=id, numThreads=num_threads, colName=col)
        # Aqui precisamos criar uma função que tome o df retornado e dê uma string
        response: OperationResponse = self.stub.top_10_cidades_transacoes(req)
        return response.result

    def abnormal_transactions(self, df_transac: DataFrame, df_account: DataFrame, id: int, num_threads: int,
        transaction_id_col: str, amount_col: str, location_col_transac: str, account_col_transac: str,
        account_col_account: str, location_col_account: str) -> str:
        req = AbnormalTransactionsRequest(
            dfTransac=df_transac, dfAccount=df_account,
            id=id, numThreads=num_threads,
            transactionIDCol=transaction_id_col,
            amountCol=amount_col,
            locationColTransac=location_col_transac,
            accountColTransac=account_col_transac,
            accountColAccount=account_col_account,
            locationColAccount=location_col_account
        )
        # Aqui precisamos criar uma função que tome o df retornado e dê uma string
        response: OperationResponse = self.stub.abnormal_transactions(req)
        return response.result
     
    def add_account(self, df: DataFrame, id: int, num_threads: int, id_col: str,
                c1: str, c2: str) -> DataFrame:
        req = AddAccountRequest(
            df=df, id=id, numThreads=num_threads,
            idCol=id_col, classFirst=c1, classSec=c2
        )
        # Precisa criar essa função
        return self.stub.addAccount(req)
    
    def add_transaction(self, df: DataFrame, id: int, num_threads: int, id_col: str,
                c1: str, c2: str) -> DataFrame:
        req = AddTransactionRequest(
            df=df, id=id, numThreads=num_threads,
            idCol=id_col, classFirst=c1, classSec=c2
        )
        # Precisa criar essa função
        return self.stub.addTransaction(req)
