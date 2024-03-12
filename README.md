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

- `aposentados_bacen`
- `aposentados_siape`
- `auxilio_emergencial`
- `despesa_empenho`
- `despesa_favorecido`
- `despesa_item_empenho`
- `execucao_despesa`
- `militares`
- `orcamento_despesa`
- `pagamento_historico`
- `pagamento`
- `pensionistas_bacen`
- `pensionistas_defesa`
- `pensionistas_siape`
- `reserva_reforma_militares`
- `servidores_bacen`
- `servidores_siape`

Execute `python portal_transparencia.py --help` outras configurações (como datas de início/fim, caminho para salvar os
arquivos etc.).
