import pandas as pd
import random
from faker import Faker

fake = Faker()

def gerar_contas(n_accounts=1000):
    status_conta = ["pendente", "bloqueada", "desbloqueada"]
    account_type = ["corrente", "salário", "poupança"]
    dados = []

    for i in range(n_accounts): 
        customer_id = i  
        account_id = i  
        current_balance = random.uniform(0.0, 15000.0)
        account_type = random.choice(account_type)
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


df_transacoes = gerar_contas(1000000)
df_transacoes.to_csv("accounts.csv", index=False)