Este é um fork do nosso trabalho recente de graduação para a matéria de Computação Escalável, que pode ser encontrado aqui: https://github.com/AlessandraBello/Comp_escalavel_a1

# Rodando o projeto com gRPC

Para rodar esse projeto, você precisará instalar o gRPC e os "Protocol Buffers" para C++ (https://grpc.io/docs/languages/cpp/quickstart/) e para python (https://grpc.io/docs/languages/python/quickstart/). **Observação**: é comum o processo apresentado na página de quickstart do C++ resultar em diversos erros em sistemas operacionais Windows. Portanto, para executar o projeto no Windows sem trocar de sistema operacional, recomenda-se o uso do WSL.

A parte do projeto que lida com o gRPC, como o servidor, os clientes e o proto, estão no diretório `grpc/`, dentro desse repositório. Portanto, todos os passos irão presumir que o diretório de trabalho atual seja ele (o usuário deverá estar dentro do diretório).

## Compilando o servidor

1. Crie um diretório `build/`;
2. Entre no diretório criado;
3. Chame o comando `$ cmake ..`
4. Chame o comando `$ make`

Resumo:

```sh
mkdir build
cd build/
cmake ..
make
```

No fim, o arquivo binário `server` terá sido gerado na pasta `build/`. Ele é um executável, e inicializará o servidor na porta 9999.

# Como rodar os testes de carga

Presumindo novamente que o diretório de trabalho seja o `grpc/`:

1. Baixe as dependências: `$ pip install -r requirements.txt`
2. Abra o servidor. Se o servidor foi compilado na pasta build, ele será o arquivo `build/server`
3. Crie um arquivo `.env` e dentro dele defina a variável `GRPC_SERVER=[endereço_do_servidor]`. 
    - Por exemplo, se você não fez nenhuma modificação no endereço em `server.cc`, para executar localmente, a variável será `GRPC_SERVER=localhost:9999`.
4. Para cada função, há um arquivo python diferente para fazer o teste de carga, que vai fazer a requisição repetidamente. Todos eles estão na pasta `test/`. Se quiser testar a adição de várias transações, por exemplo, execute `python ./test/client_addTransaction.py`

Cada script python que faz o teste de carga tem um parâmetro de inicialização opcional "1", que registra o tempo que as requisições nos primeiros 2 minutos levam para serem respondidas, e guarda esses valores em um csv na pasta `grpc/data/`. 

Para executar o script com a intenção de registrar os tempos de execução, rode (exemplo para addTransaction):
```sh
$ python ./test/client_addTransaction.py 1
```
