# Scraper do Portal da Transparência do Governo Federal

## Instalando

```shell
pyenv virtualenv 3.7.3 transparencia-gov-br
pyenv activate transparencia-gov-br
pip install -r requirements.txt
```

## Rodando

Todos os spiders:

```shell
./run.sh
```

Apenas um spider:

```shell
./run.sh <nome-do-spider>
```

> Nota: consule os nomes dos spiders disponíveis em
> [transparenciagovbr/spiders/](transparenciagovbr/spiders/]).


## Importando no PostgreSQL

Antes, instale as dependências, rode os spiders e crie uma varíavel com a URI
de conexão com o banco:

```shell
pip install psycopg2-binary tqdm
./run.sh
export POSTGRESQL_URI="postgres://usuario:senha@host:porta/banco"
```

Depois, execute o script para importar todas as tabelas:


```shell
./import-postgresql.sh
```

Ou apenas a tabela de um spider específico:

```shell
./import-postgresql.sh <nome-do-spider>
```

> Nota: consule os nomes dos spiders disponíveis em
> [transparenciagovbr/spiders/](transparenciagovbr/spiders/]).
