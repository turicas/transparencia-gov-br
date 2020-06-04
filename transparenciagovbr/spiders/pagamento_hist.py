import csv
import datetime
import io
import zipfile

from transparenciagovbr.spiders.base import TransparenciaBaseSpider
from transparenciagovbr.utils.fields import field_mapping_from_csv, load_schema
from transparenciagovbr.utils.io import NotNullTextWrapper


class PagamentoHistSpider(TransparenciaBaseSpider):
    name = "pagamento-hist"
    base_url = "http://www.portaltransparencia.gov.br/download-de-dados/historico-gastos-diretos-pagamentos/{year}{month:02d}"
    start_date = datetime.date(2011, 1, 1)
    end_date = datetime.date(2012, 12, 31)
    publish_frequency = "monthly"
    schema = load_schema("pagamento-hist.csv")
    field_mapping = field_mapping_from_csv("pagamento-hist.csv")

    def parse_zip(self, response):
        zf = zipfile.ZipFile(io.BytesIO(response.body))
        assert len(zf.filelist) == 1
        fobj = NotNullTextWrapper(
            zf.open(zf.filelist[0].filename), encoding="iso-8859-1"
        )
        reader = csv.DictReader(fobj, delimiter="\t")

        for row in reader:
            yield self.convert_row(row)
