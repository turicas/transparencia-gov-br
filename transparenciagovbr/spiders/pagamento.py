import datetime

from transparenciagovbr.spiders.base import TransparenciaBaseSpider
from transparenciagovbr.utils.date import today


class PagamentoSpider(TransparenciaBaseSpider):
    name = "pagamento"
    base_url = "http://www.portaldatransparencia.gov.br/download-de-dados/despesas/{year}{month:02d}{day:02d}"
    start_date = datetime.date(2013, 3, 31)
    end_date = today()
    publish_frequency = "daily"
    filename_suffix = "_Despesas_Pagamento.csv"
    schema_filename = "pagamento.csv"
