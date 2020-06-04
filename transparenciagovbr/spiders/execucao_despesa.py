import datetime

from transparenciagovbr.spiders.base import TransparenciaBaseSpider
from transparenciagovbr.utils.date import today


class ExecucaoDespesaSpider(TransparenciaBaseSpider):
    name = "execucao_despesa"
    base_url = "http://transparencia.gov.br/download-de-dados/despesas-execucao/{year}{month:02d}"
    start_date = datetime.date(2014, 1, 1)
    end_date = today()
    publish_frequency = "monthly"
    filename_suffix = "_Despesas.csv"
    schema_filename = "execucao_despesa.csv"
