# Computação Escalável: A1

O presente trabalho implementa um micro-framework de extração, carregamente e processamento de dados bancários. A ideia principal é, além das funcionalidades básicas relacionadas às etapas mencionadas, adicionar também funções que nos forneçam informações relevantes sobre os clientes e as transações do banco. Para isso, paralelizamos praticamente todo o projeto usando uma _thread pool_.

# Instruções para execução do projeto

## Pré-requisitos

Para executar o projeto, é necessário ter instalado Python, C e C++. Além disso, é necessário instalar as bibliotecas listadas no arquivo `requirements.txt`. Para isso, execute o seguinte comando:

```bash
$ pip install -r requirements.txt
```

Para conseguir rodar o SQLite, execute

```bash
$ gcc -c include/sqlite3.c
```

## Estrutura 

* `data/`: contém os dados a serem extraídos para construção dos dataframes;
* `include/`: contém os _headers_ dos arquivos em `src/`, além do template para o thread pool e o código fonte do SQLite;
* `output/`: onde os dataframes resultantes do driver code são postos;
* `tests/`: contém dois arquivos de testes para os extratores de CSV e de SQLite;
* `src/`: contém as implementações dos tratadores, extratores e estrutura do dataframe, além de um arquivo que gera dados bancários sintéticos para analisarmos;
* `main.cpp`: driver code do projeto, mostra o funcionamento de alguns tratadores;
* `home.py`: arquivo que gera o dashboard com os resultados.

## Execução

Para rodar as demos em `main.cpp`, execute

```bash
$ g++ -std=c++17 -Iinclude main.cpp src/csv_extractor.cpp src/df.cpp src/tratadores.cpp -o main.exe
```

E em seguida

```bash
$ ./main.exe
```

Para visualizar o dashboard, execute

```bash
$ streamlit run home.py 
```

### Adicional: demos dos extratores

Na pasta `tests/`, há dois arquivos que reportam algumas métricas de performance dos extratores SQLite e CSV. Para rodar os testes SQLite, execute 

```bash
$ g++ -std=c++17 -Iinclude tests/db_test.cpp src/sql_extractor.cpp src/df.cpp src/tratadores.cpp -o sqlite_test.exe sqlite3.o
```

seguido de 

```bash
$ ./sqlite_test.exe
```

Da mesma forma, para rodar os testes CSV, execute

```bash
$ g++ -std=c++17 -Iinclude tests/csv_test.cpp src/csv_extractor.cpp src/df.cpp src/tratadores.cpp -o csv_test.exe
```

seguido de 

```bash
$ ./csv_test.exe
```
