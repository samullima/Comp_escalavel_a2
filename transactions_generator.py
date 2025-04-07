import pandas as pd
import random
from faker import Faker
from datetime import timedelta

fake = Faker()

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


df_transacoes = gerar_transacoes(100000, 1000)
df_transacoes.to_csv("transactions.csv", index=False)