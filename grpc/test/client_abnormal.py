from grpc_client import GRPCClient

client = GRPCClient()
def test_abnormal_transactions():
    abnormal_transactions = client.abnormal_transactions()

if __name__ == "__main__":
    while True:
        test_abnormal_transactions()