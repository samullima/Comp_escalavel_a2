import pandas as pd
import random
from faker import Faker
from datetime import timedelta
import sqlite3

fake = Faker()

def gerar_contas(n_accounts, n_clients):
    status_conta = ["pendente", "bloqueada", "desbloqueada"]
    account_type_choices = ["corrente", "salário", "poupança"]
    
    # Lista de possíveis locais
    locais = [
        "Rio Branco", "Maceió", "Macapá", "Manaus", "Salvador", "Fortaleza", "Brasília",
        "Vitória", "Goiânia", "São Luís", "Cuiabá", "Campo Grande", "Belo Horizonte",
        "Belém", "João Pessoa", "Curitiba", "Recife", "Teresina", "Rio de Janeiro",
        "Natal", "Porto Alegre", "Porto Velho", "Boa Vista", "Florianópolis",
        "São Paulo", "Aracaju", "Palmas"
    ]
    
    dados = []
    account_id_counter = 0  

    for i in range(n_clients): 
        num_contas_cliente = random.randint(1, 3)  
        cliente_local = random.choice(locais) 
        
        for j in range(num_contas_cliente):
            if account_id_counter >= n_accounts:
                break  
 
            account_id = account_id_counter
            account_id_counter += 1  

            current_balance = round(random.uniform(0.0, 15000.0), 2)
            account_type = random.choice(account_type_choices)
            opening_date = fake.date_time_this_year()
            account_status = random.choice(status_conta)

            dados.append({
                "customer_id": i, 
                "account_id": account_id,
                "current_balance": current_balance,     
                "account_type": account_type,       
                "opening_date": opening_date.date(),
                "account_status": account_status,
                "account_location": cliente_local
            })

    return pd.DataFrame(dados)


def gerar_clientes(n_clients):
    dados = []

    for i in range(n_clients+1):
        name = fake.name()
        email = fake.email()
        address = fake.address().replace("\n", " - ")
        phone = int(fake.msisdn()[0:11])  

        dados.append({
            "customer_id": i,
            "name": name,
            "email": email,
            "address": address,
            "phone": phone
        })

    return pd.DataFrame(dados)


def gerar_transacoes(df_contas, n_transactions):
    tipos_transacoes = ["depósito", "retirada", "transferência", "pagamento"]
    dados = []
    
    locais = [
        "Rio Branco", "Maceió", "Macapá", "Manaus", "Salvador", "Fortaleza", "Brasília",
        "Vitória", "Goiânia", "São Luís", "Cuiabá", "Campo Grande", "Belo Horizonte",
        "Belém", "João Pessoa", "Curitiba", "Recife", "Teresina", "Rio de Janeiro",
        "Natal", "Porto Alegre", "Porto Velho", "Boa Vista", "Florianópolis",
        "São Paulo", "Aracaju", "Palmas"
    ]

    col_cidade = df_contas["account_location"]

    n_accounts = len(df_contas) - 1 

    # Geração das colunas de tempo
    lista_time_starts = []
    for i in range(n_transactions):
        lista_time_starts.append(fake.date_time_this_year())
    lista_time_starts.sort()

    for i in range(n_transactions):
        account_id = random.randint(0, n_accounts) 
        recipient_id = random.randint(0, n_accounts)
        amount = round(random.uniform(10.0, 700.0), 2)
        tipo = random.choice(tipos_transacoes)

        time_start = lista_time_starts[i]
        time_end = time_start + timedelta(seconds=random.randint(5, 60))

        transaction_date = time_start.date()

        if random.random() < 0.98:
            # Cidade padrão
            location = col_cidade.iloc[account_id]
        else:
            # Suspeita
            location = random.choice(locais)

        dados.append({
            "transation_id": i,
            "account_id": account_id,
            "recipient_id": recipient_id,
            "amount": amount,
            "type": tipo,
            "location": location,
            "time_start": time_start.time(),
            "time_end": time_end.time(),
            "date": transaction_date
        })

    return pd.DataFrame(dados)


# ALTERAR ESSES CAMPOS:
n_transacoes = 1000000   
n_contas = 10000         
n_max_clientes = 5000      
connection_obj = sqlite3.connect('../data/data.db')

df_accounts = gerar_contas(n_contas, n_max_clientes)
df_customers = gerar_clientes(df_accounts["customer_id"].iloc[-1])
df_transactions = gerar_transacoes(df_accounts, n_transacoes)

df_accounts.to_sql('accounts', connection_obj, if_exists='replace', index=False)
df_customers.to_sql('customers', connection_obj, if_exists='replace', index=False)
df_transactions.to_sql('transactions', connection_obj, if_exists='replace', index=False)

df_accounts.to_csv("../data/accounts/accounts.csv", index=False)
df_customers.to_csv("../data/customers/customers.csv", index=False)
df_transactions.to_csv("../data/transactions/transactions.csv", index=False)