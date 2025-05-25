from grpc_client import GRPCClient
from random import randint, random

client = GRPCClient()
def test_add_transaction():
    id_sender = randint(1, 1000)  # Random sender ID
    id_receiver = randint(1, 1000)  # Random receiver ID
    amount = randint(1, 10000) + random()  # Random amount
    location = "New York"
    
    # Add a transaction
    client.add_transaction(id_sender, id_receiver, amount, location)

if __name__ == "__main__":
    while True:
        test_add_transaction()