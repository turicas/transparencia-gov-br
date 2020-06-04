import datetime

from transparenciagovbr.spiders.base import TransparenciaBaseSpider
from transparenciagovbr.utils.date import today


class OrcamentoDespesaSpider(TransparenciaBaseSpider):
    name = "orcamento_despesa"
    base_url = "http://transparencia.gov.br/download-de-dados/orcamento-despesa/{year}"
    start_date = datetime.date(2014, 1, 1)
    end_date = today()
    publish_frequency = "yearly"
    filename_suffix = "_OrcamentoDespesa.zip.csv"
    schema_filename = "orcamento_despesa.csv"
