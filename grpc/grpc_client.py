import grpc
from typing import Any
from dataframe_pb2 import (
    DataFrameServiceStub, Element, Column, DataFrame,
    ClassifyAccountsRequest, SummaryStatsRequest, Top10CidadesRequest, AbnormalTransactionsRequest
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
                          c1: str, c2: str) -> DataFrame:
        req = ClassifyAccountsRequest(
            df=df, id=id, numThreads=num_threads,
            idCol=id_col, classFirst=c1, classSec=c2
        )
        return self.stub.ClassifyAccountsParallel(req).df

    def summary_stats(self, df: DataFrame, id: int, num_threads: int, col: str) -> DataFrame:
        req = SummaryStatsRequest(df=df, id=id, numThreads=num_threads, colName=col)
        return self.stub.SummaryStats(req).df

    def top10_cidades(self, df: DataFrame, id: int, num_threads: int, col: str) -> DataFrame:
        req = Top10CidadesRequest(df=df, id=id, numThreads=num_threads, colName=col)
        return self.stub.Top10CidadesTransacoes(req).df

    def abnormal_transactions(self, df_transac: DataFrame, df_acc: DataFrame, id: int, num_threads: int,
                               transac_id: str, amount: str, loc_t: str, acc_t: str, acc_a: str, loc_a: str):
        return self.stub.AbnormalTransactions(AbnormalTransactionsRequest(
            dfTransac=df_transac, dfAccount=df_acc, id=id, numThreads=num_threads,
            transactionIDCol=transac_id, amountCol=amount, locationColTransac=loc_t,
            accountColTransac=acc_t, accountColAccount=acc_a, locationColAccount=loc_a
        ))
