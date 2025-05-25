from client_tests_lib import test_transaction_info
import sys
import time
import pandas as pd

if __name__ == "__main__":
    print("Script name:", sys.argv[0])
    if len(sys.argv) > 1:
        if sys.argv[1] == "1":
            # Computando o tempo
            count = 0
            total_time_ns = 0
            df = pd.DataFrame(columns=["execução", "tempo"])
            cadencia = 1000 # cadência de aparecimento no csv
            
            tempo_atual = time.time()
            tempo_execucao = 120

            while tempo_atual + tempo_execucao > time.time():
                start_time = time.perf_counter_ns()
                test_transaction_info()
                end_time = time.perf_counter_ns()

                elapsed_ns = end_time - start_time
                total_time_ns += elapsed_ns
                count += 1

                if count % cadencia == 0:
                    avg_time_ns = total_time_ns # cadência
                    print(f"{count} execuções – tempo médio: {avg_time_ns} nanosegundos")
                    total_time_ns = 0  
                    df = pd.concat([pd.DataFrame([[count,avg_time_ns]], columns=df.columns), df], ignore_index=True)

            df.to_csv("data/client_transactionsInfo.csv", index=False)

    while True:
        test_transaction_info()