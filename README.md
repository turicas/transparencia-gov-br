# Scraper do Portal da Transparência do Governo Federal

Conjunto de scripts que baixam e limpam/tratam dados do [Portal da Transparência do Governo
Federal](https://transparencia.gov.br/).


## Ambiente

Você precisa do docker compose para executar este projeto. Depois de clonar esse repositório, execute o comando abaixo
para construir os containers (`build`), iniciá-los (`start`) e ver os logs (`logs`) de todos os serviços em execução:

```shell
make build start logs
```

> Nota: a etapa `build` precisará ser executada somente apenas na primeira vez em que você for rodar o projeto e caso
> tenham tido mudanças no `Dockerfile`. Para as próximas vezes que for trabalhar no projeto, execute apenas
> `make start logs`.

Existem diversos atalhos no `Makefile`. Digite `make help` para ver todos. Eles devem ser executados fora do container
(na máquina host, onde o docker daemon está rodando). Os principais são:

Parar containers:

```shell
make stop
```

Reiniciar containers:

```shell
make restart
```

Forçar o guia de estilo de código Python/reformatar todos os arquivos:

```shell
make lint
```


### Personalizando variáveis de ambiente

Para cada serviço disponível no `compose.yaml`, temos um arquivo de variáveis de ambiente padrão chamado
`docker/env/<service>`. Se você precisar alterar qualquer uma das variáveis, crie um arquivo
`docker/env/<service>.local` e coloque-as lá. Os arquivos `.local` presentes em `docker/env` serão ignorados pelo Git e
o docker compose o carregará logo após o padrão (sobrescrevendo os valores com sua versão).


## Coletando dados

Para baixar e converter um dataset, execute dentro do container `main` (para executá-lo, rode `make bash`):

```shell
python portal_transparencia.py <nome-do-dataset>
```

Os datasets atualmente disponíveis são:

- `auxilio_emergencial`
- `despesa_empenho`
- `despesa_favorecido`
- `despesa_item_empenho`
- `execucao_despesa`
- `orcamento_despesa`
- `pagamento`
- `pagamento_historico`
- `pessoa_exposta_politicamente`
- `sancao_acordo_leniencia`
- `sancao_ceis`
- `sancao_cepim`
- `sancao_cnep`
- `sancao_ceaf`
- `servidor_aposentado_bacen`
- `servidor_aposentado_siape`
- `servidor_bacen`
- `servidor_militar`
- `servidor_militar_reserva_reforma`
- `servidor_pensionista_bacen`
- `servidor_pensionista_defesa`
- `servidor_pensionista_siape`
- `servidor_siape`
- `transferencia_despesa`

Execute `python portal_transparencia.py --help` outras configurações (como datas de início/fim, caminho para salvar os
arquivos etc.).

A lista acima foi gerada com o seguinte código:

```python
from portal_transparencia import BaseDownloader, subclasses

scrapers = [cls.get_name() for cls in subclasses(BaseDownloader) if not cls.__name__.startswith("Base")]
for scraper in sorted(scrapers):
    print(f"- `{scraper}`")
```

### Transformações

As transformações nos dados estão nos arquivos na pasta `schema` e necessitam ter as seguintes colunas:

- `original_name`: nome original do campo (depois de rodar a função `rows.fields.slug`, ou seja, totalmente em
  minúsculas, sem acentos e espaços trocados por `_`)
- `original_type`: tipo original baseado no plugin da rows (para ser importado com COPY no postgres). Opções:
  - `binary`
  - `bool`
  - `date`
  - `datetime`
  - `decimal`
  - `float`
  - `integer`
  - `json`
  - `percent`
  - `text`
  - `uuid`
- `transformation`: expressão SQL para gerar esse campo
- `field_name`: nome final do campo
- `field_type`: tipo final do campo
