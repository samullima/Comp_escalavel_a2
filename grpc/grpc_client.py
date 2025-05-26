import grpc
from dataframe_pb2 import (
    Transaction, GenericInput, Summary, Abnormal,
    AccountId, Class, NumberOfAccounts
)
from dataframe_pb2_grpc import ProcessingServicesStub


class GRPCClient:
    """
    Cliente gRPC para interagir com o serviço ProcessingServices.

    Métodos:
        add_transaction: Adiciona uma transação no servidor.
        transactions_info: Retorna estatísticas resumidas das transações.
        abnormal_transactions: Retorna a lista de IDs de transações anormais.
        account_class: Retorna a classe de uma conta específica.
        account_by_class: Retorna o número de contas de uma determinada classe.
    """
    
    def __init__(self, address: str = "localhost:9999") -> None:
        """
        Inicializa o cliente gRPC.

        Args:
            address (str): Endereço do servidor no formato "host:porta".
        """
        # Cria um canal gRPC inseguro para o endereço especificado
        self.channel = grpc.insecure_channel(address)
        self.stub = ProcessingServicesStub(self.channel)
        

    def add_transaction(self, id_sender: int, id_receiver: int, amount: float, location: str) -> None:
        """
        Adiciona uma transação no servidor.

        Args:
            id_sender (int): ID da conta remetente.
            id_receiver (int): ID da conta destinatária.
            amount (float): Valor da transação.
            location (str): Localização da transação.
        """
        req = Transaction(
            idSender=id_sender,
            idReceiver=id_receiver,
            amount=amount,
            location=location
        )
        self.stub.addTransaction(req)


    def transactions_info(self) -> Summary:
        """
        Obtém estatísticas descritivas das transações.

        Returns:
            Summary: Quartis e média.
        """
        req = GenericInput()
        return self.stub.transactionsInfo(req)


    def abnormal_transactions(self) -> list[int]:
        """
        Retorna a lista de IDs de transações consideradas anormais.

        Returns:
            list[int]: Lista de IDs das transações anormais.
        """
        req = GenericInput()
        response: Abnormal = self.stub.abnormalTransactions(req)
        return list(response.vectorAbnormal)


    def account_class(self, id_account: int) -> str:
        """
        Retorna a classe associada a um ID de conta.

        Args:
            id_account (int): ID da conta.

        Returns:
            str: Classe da conta.
        """
        req = AccountId(idAccount=id_account)
        response: Class = self.stub.accountClass(req)
        return response.className


    def account_by_class(self, class_name: str) -> int:
        """
        Retorna o número de contas que pertencem a uma determinada classe.

        Args:
            class_name (str): Nome da classe.

        Returns:
            int: Quantidade de contas pertencentes à classe determinada.
        """
        
        req = Class(className=class_name)
        response: NumberOfAccounts = self.stub.accountByClass(req)
        return response.sum
    

if __name__ == "__main__":
    client = GRPCClient()
    
    # Exemplo de uso
    client.add_transaction(1, 2, 100.0, "New York")
    summary = client.transactions_info()
    print(f"Summary: {summary}")
    abnormal_ids = client.abnormal_transactions()
    print(f"Abnormal Transaction IDs: {abnormal_ids}")
    account_class = client.account_class(1)
    print(f"Account Class: {account_class}")
