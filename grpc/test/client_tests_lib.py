import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from grpc_client import GRPCClient
import dotenv
from random import randint, random

dotenv.load_dotenv()
client = GRPCClient(dotenv.get_key('.env', 'GRPC_SERVER'))

def test_abnormal_transactions():
    abnormal_transactions = client.abnormal_transactions()

def test_account_by_class():
    account_classes = ["A", "B", "C"]
    account_class = account_classes[randint(0, len(account_classes) - 1)]
    count = client.account_by_class(account_class)

def test_account_class():
    id_account = 12345  # Example account ID
    account_class = client.account_class(id_account)
    
    # Check if the account class is a string
    assert isinstance(account_class, str)

def test_transaction_info():
    summary = client.transactions_info()

def test_add_transaction():
    id_sender = randint(1, 1000)  # Random sender ID
    id_receiver = randint(1, 1000)  # Random receiver ID
    amount = randint(1, 10000) + random()  # Random amount
    location = "New York"
    
    # Add a transaction
    client.add_transaction(id_sender, id_receiver, amount, location)