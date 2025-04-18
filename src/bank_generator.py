import pandas as pd
import random
from faker import Faker
from datetime import timedelta
import sqlite3

fake = Faker()

def gerar_contas(n_accounts=1000):
    status_conta = ["pendente", "bloqueada", "desbloqueada"]
    account_type_choices = ["corrente", "salário", "poupança"]
    dados = []

    for i in range(n_accounts): 
        customer_id = i  
        account_id = i  
        current_balance = round(random.uniform(0.0, 15000.0), 2)
        account_type = random.choice(account_type_choices)
        opening_date = fake.date_time_this_year()
        account_status = random.choice(status_conta)

        dados.append({
            "customer_id": customer_id,
            "account_id": account_id,
            "current_balance": current_balance,
            "account_type": account_type,
            "opening_date": opening_date,
            "account_status": account_status
        })

    return pd.DataFrame(dados)


def gerar_transacoes(n_transactions=100000, n_accounts=1000):
    tipos_transacoes = ["depósito", "retirada", "transferência", "pagamento"]
    dados = []

    for i in range(n_transactions):
        transation_id = i 
        account_id = random.randint(0, n_accounts)
        recipient_id = random.randint(0, n_accounts)
        amount = round(random.uniform(10.0, 700.0), 2)
        tipo = random.choice(tipos_transacoes)

        # Geração de horários
        time_start = fake.date_time_this_year()
        processed_at = time_start + timedelta(seconds=random.randint(5, 60))
        
        location = fake.city()

        dados.append({
            "transation_id": transation_id,
            "account_id": account_id,
            "recipient_id": recipient_id,
            "amount": amount,
            "type": tipo,
            "time_start": time_start.time(),
            "location": location,
            "processed_at": processed_at.time()
        })

    return pd.DataFrame(dados)

n_transacoes = 100000
n_contas = 1000
connection_obj = sqlite3.connect('data/data.db')

df_contas = gerar_contas(n_contas)
df_contas.to_csv("accounts.csv", index=False)
df_contas.to_sql('accounts', connection_obj, if_exists='replace', index=False)

df_transacoes = gerar_transacoes(n_transacoes, n_contas)
df_transacoes.to_csv("transactions.csv", index=False)
df_transacoes.to_sql('transactions', connection_obj, if_exists='replace', index=False)