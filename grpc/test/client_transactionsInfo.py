import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from grpc_client import GRPCClient

client = GRPCClient()
def test_transaction_info():
    summary = client.transactions_info()
    
    # Check if the summary contains the expected fields
    assert hasattr(summary, 'totalTransactions')
    assert hasattr(summary, 'totalAmount')
    assert hasattr(summary, 'averageAmount')

if __name__ == "__main__":
    while True:
        test_transaction_info()