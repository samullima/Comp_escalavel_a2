import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

# Define os caminhos de entrada e saída
folder = Path('grpc/data')
output_folder = Path('grpc/plots')
output_folder.mkdir(parents=True, exist_ok=True)

files = [
    "client_abnormal.csv",
    "client_accountByClass.csv",
    "client_accountClass.csv",
    "client_addTransaction.csv",
    "client_transactionsInfo.csv"
]

# Gera gráficos a partir dos arquivos CSV
for file in files:
    path = folder / file
    df = pd.read_csv(path)
    
    # Nome da imagem vai ser o que está entre underline e ponto
    name = file.split('_')[1].split('.')[0]

    # Cria o gráfico de linha
    plt.figure()
    plt.plot(df['execução'], df['tempo'])
    plt.title(f"{name}: Execução x Tempo")
    plt.ylabel('Tempo (ms)')
    plt.xlabel('Execução')
    plt.grid(True)

    # Salva o gráfico como JPEG
    output_path = output_folder / (file.replace('.csv', '.jpeg'))
    plt.savefig(output_path, format='jpeg')
    plt.close()
