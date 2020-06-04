import datetime

from transparenciagovbr.spiders.base import TransparenciaBaseSpider
from transparenciagovbr.utils.date import today
from transparenciagovbr.utils.fields import field_mapping_from_csv, load_schema


class ExecucaoDespesaSpider(TransparenciaBaseSpider):
    name = "execucao-despesa"
    base_url = "http://transparencia.gov.br/download-de-dados/despesas-execucao/{year}{month:02d}"
    start_date = datetime.date(2014, 1, 1)
    end_date = today()
    publish_frequency = "monthly"
    filename_suffix = "_Despesas.csv"
    schema = load_schema("execucao-despesa.csv")
    field_mapping = field_mapping_from_csv("execucao-despesa.csv")
