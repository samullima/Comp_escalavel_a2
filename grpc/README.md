Esse README pressupõe que seu ambiente de trabalho esteja nessa pasta (grpc)

# Passos para compilar:

1. Crie uma pasta build
2. Entre na pasta build: `$ cd build/`
3. Chame `$ cmake ..`
4. Chame `$ make`

Resumo:

```sh
mkdir build
cd build/
cmake ..
make
```

# Como rodar os testes de carga

1. Baixe as dependências: `$ pip install -r requirements.txt`
2. Abra o servidor. Se o servidor foi compilado na pasta build, ele será o arquivo `build/server`
3. Crie um arquivo `.env` e dentro dele defina a variável `GRPC_SERVER=[endereço_do_servidor]`. 
    - Por exemplo, se você não fez nenhuma modificação no endereço em `server.cc`, para executar localmente, a variável será `GRPC_SERVER=localhost:9999`.
4. Para cada função, há um arquivo python diferente para fazer o teste de carga. Todos eles estão na pasta `test/`. Se quiser testar a adição de várias transações, por exemplo, execute `python client_addTransaction.py`