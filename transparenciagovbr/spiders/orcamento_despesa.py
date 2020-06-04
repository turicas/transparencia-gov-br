import datetime

from transparenciagovbr.spiders.base import TransparenciaBaseSpider
from transparenciagovbr.utils.date import today
from transparenciagovbr.utils.fields import field_mapping_from_csv, load_schema


class OrcamentoDespesaSpider(TransparenciaBaseSpider):
    name = "orcamento-despesa"
    base_url = "http://transparencia.gov.br/download-de-dados/orcamento-despesa/{year}"
    start_date = datetime.date(2014, 1, 1)
    end_date = today()
    publish_frequency = "yearly"
    filename_suffix = "_OrcamentoDespesa.zip.csv"
    schema = load_schema("orcamento-despesa.csv")
    field_mapping = field_mapping_from_csv("orcamento-despesa.csv")
