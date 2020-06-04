import datetime

from transparenciagovbr.spiders.base import TransparenciaBaseSpider
from transparenciagovbr.utils.date import today
from transparenciagovbr.utils.fields import field_mapping_from_csv, load_schema


class PagamentoSpider(TransparenciaBaseSpider):
    name = "pagamento"
    base_url = "http://www.portaldatransparencia.gov.br/download-de-dados/despesas/{year}{month:02d}{day:02d}"
    start_date = datetime.date(2013, 3, 31)
    end_date = today()
    publish_frequency = "daily"
    filename_suffix = "_Despesas_Pagamento.csv"
    schema = load_schema("pagamento.csv")
    field_mapping = field_mapping_from_csv("pagamento.csv")
