# Dados de pensionistas

## Instalação

```shell
pip install -r requirements.txt
```

## Execução

Baixe os arquivos de pensionistas disponíveis [nesse
site](http://transparencia.gov.br/download-de-dados/servidores) e coloque-os em
`data/download/`. Depois, execute:

```shell
python convert.py <cadastro|observacao|remuneracao>
```

Os arquivos `cadastro.csv.gz`, `observacao.csv.gz` e `remuneracao.csv.gz` serão
gerados em `data/output/`.
