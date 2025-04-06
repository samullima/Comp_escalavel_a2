import pandas as pd
import random
from faker import Faker
from datetime import timedelta

fake = Faker()

def gerar_transacoes(n=10000):
    tipos_transacoes = ["depósito", "retirada", "transferência", "pagamento"]
    dados = []

    for i in range(n):
        transation_id = i 
        account_id = random.randint(100, 9999)
        recipient_id = random.randint(1000, 9999)
        amount = round(random.uniform(10.0, 5000.0), 2)
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


df_transacoes = gerar_transacoes(10000)
df_transacoes.to_csv("transactions.csv", index=False)