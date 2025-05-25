from grpc_client import GRPCClient
from random import randint

client = GRPCClient()
def test_account_by_class():
    account_classes = ["A", "B", "C"]
    account_class = account_classes[randint(0, len(account_classes) - 1)]
    count = client.account_by_class(account_class)


if __name__ == "__main__":
    while True:
        test_account_by_class()