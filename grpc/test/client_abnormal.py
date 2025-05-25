import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from grpc_client import GRPCClient

client = GRPCClient("10.22.171.206:9999")
def test_abnormal_transactions():
    abnormal_transactions = client.abnormal_transactions()

if __name__ == "__main__":
    while True:
        test_abnormal_transactions()