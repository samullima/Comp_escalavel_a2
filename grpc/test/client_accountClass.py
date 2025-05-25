import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from grpc_client import GRPCClient

client = GRPCClient()
def test_account_class():
    id_account = 12345  # Example account ID
    account_class = client.account_class(id_account)
    
    # Check if the account class is a string
    assert isinstance(account_class, str)
    # Check if the account class is not empty
    assert len(account_class) > 0

if __name__ == "__main__":
    while True:
        test_account_class()