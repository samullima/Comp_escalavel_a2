import grpc
from typing import Any
from google.protobuf.empty_pb2 import Empty
from processing_pb2 import (
    Transaction, GenericInput, Summary, Abnormal,
    AccountId, Class, NumberOfAccounts
)
from processing_pb2_grpc import ProcessingServicesStub


class GRPCClient:
    def __init__(self, address: str = "localhost:9999") -> None:
        self.channel = grpc.insecure_channel(address)
        self.stub = ProcessingServicesStub(self.channel)

    def add_transaction(self, id_sender: int, id_receiver: int, amount: float, location: str) -> None:
        req = Transaction(
            idSender=id_sender,
            idReceiver=id_receiver,
            amount=amount,
            location=location
        )
        self.stub.AddTransaction(req)

    def transactions_info(self) -> Summary:
        req = GenericInput()
        return self.stub.TransactionsInfo(req)

    def abnormal_transactions(self) -> list[int]:
        req = GenericInput()
        response: Abnormal = self.stub.AbnormalTransactions(req)
        return list(response.vectorAbnormal)

    def account_class(self, id_account: int) -> str:
        req = AccountId(idAccount=id_account)
        response: Class = self.stub.AccountClass(req)
        return response.className

    def account_by_class(self, class_name: str) -> int:
        req = Class(className=class_name)
        response: NumberOfAccounts = self.stub.AccountByClass(req)
        return response.sum
