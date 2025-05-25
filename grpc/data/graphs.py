import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

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

for file in files:
    path = folder / file
    df = pd.read_csv(path)

    time_cols = [col for col in df.columns if 'time' in col.lower() or 'tempo' in col.lower()]
    exec_cols = [col for col in df.columns if 'exec' in col.lower() or 'execution' in col.lower() or 'run' in col.lower()]
    
    # nome entre underline e ponto
    name = file.split('_')[1].split('.')[0]

    if time_cols and exec_cols:
        plt.figure()
        for exec_col in exec_cols:
            plt.plot(df[exec_col], df[time_cols[0]])

        plt.title(f"{name}: Execução x Tempo")
        plt.ylabel('Tempo (ms)')
        plt.xlabel('Execução')
        plt.grid(True)

        output_path = output_folder / (file.replace('.csv', '.jpeg'))
        plt.savefig(output_path, format='jpeg')
        plt.close()
