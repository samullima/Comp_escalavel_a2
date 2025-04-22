# Computação Escalável: A1

O presente trabalho implementa um micro-framework de extração, carregamente e processamento de dados bancários. A ideia principal é, além das funcionalidades básicas relacionadas às etapas mencionadas, adicionar também funções que nos forneçam informações relevantes sobre os clientes e as transações do banco. Para isso, paralelizamos praticamente todo o projeto usando uma _thread pool_.

# Instruções para execução do projeto

## Pré-requisitos

Para executar o projeto, é necessário ter instalado Python, C e C++. Além disso, é necessário instalar as bibliotecas listadas no arquivo `requirements.txt`. Para isso, execute o seguinte comando:

```bash
pip install -r requirements.txt
```

## Estrutura 

* `data/`: contém os dados a serem extraídos para construção dos dataframes;
* `include/`: contém os _headers_ dos arquivos em `src/`, além do template para o thread pool;
* `dashboard/`: contém os arquivos que geram o dashboard Streamlit para visualização dos resultados dos tratadores;
* `src/`: contém as implementações dos tratadores, extratores e estrutura do dataframe, além de um arquivo que gera dados bancários sintéticos para analisarmos;
* `main.cpp`: driver code do projeto, mostra o funcionamento de alguns tratadores.

## Execução

Para rodar as demos em `main.cpp`, execute

```bash
$ g++ -Iinclude main.cpp src/csv_extractor.cpp src/df.cpp src/tratadores.cpp -o main.exe
```

E em seguida

```bash
./main.exe
```
