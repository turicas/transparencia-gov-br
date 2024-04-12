# Scraper do Portal da Transparência do Governo Federal

## Instalando

```shell
pyenv virtualenv 3.7.3 transparencia-gov-br
pyenv activate transparencia-gov-br
pip install -r requirements.txt
```

## Rodando

Para baixar e converter um dataset, execute:

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
