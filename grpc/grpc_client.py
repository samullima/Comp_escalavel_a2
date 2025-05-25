import grpc
from dataframe_pb2 import (
    Transaction, GenericInput, Summary, Abnormal,
    AccountId, Class, NumberOfAccounts
)
from dataframe_pb2_grpc import ProcessingServicesStub


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
        self.stub.addTransaction(req)

    def transactions_info(self) -> Summary:
        req = GenericInput()
        return self.stub.transactionsInfo(req)

    def abnormal_transactions(self) -> list[int]:
        req = GenericInput()
        response: Abnormal = self.stub.abnormalTransactions(req)
        return list(response.vectorAbnormal)

    def account_class(self, id_account: int) -> str:
        req = AccountId(idAccount=id_account)
        response: Class = self.stub.accountClass(req)
        return response.className

    def account_by_class(self, class_name: str) -> int:
        req = Class(className=class_name)
        response: NumberOfAccounts = self.stub.accountByClass(req)
        return response.sum
    

if __name__ == "__main__":
    client = GRPCClient()
    # Example usage
    client.add_transaction(1, 2, 100.0, "New York")
    summary = client.transactions_info()
    print(f"Summary: {summary}")
    abnormal_ids = client.abnormal_transactions()
    print(f"Abnormal Transaction IDs: {abnormal_ids}")
    account_class = client.account_class(1)
    print(f"Account Class: {account_class}")
