import csv
import datetime
import io
import zipfile

from transparenciagovbr.spiders.base import TransparenciaBaseSpider
from transparenciagovbr.utils.io import NotNullTextWrapper


class PagamentoHistSpider(TransparenciaBaseSpider):
    name = "pagamento_historico"
    base_url = "http://www.portaltransparencia.gov.br/download-de-dados/historico-gastos-diretos-pagamentos/{year}{month:02d}"
    start_date = datetime.date(2011, 1, 1)
    end_date = datetime.date(2012, 12, 31)
    publish_frequency = "monthly"
    schema_filename = "pagamento_historico.csv"

    def parse_zip(self, response):
        zf = zipfile.ZipFile(io.BytesIO(response.body))
        assert len(zf.filelist) == 1
        fobj = NotNullTextWrapper(
            zf.open(zf.filelist[0].filename), encoding="iso-8859-1"
        )
        reader = csv.DictReader(fobj, delimiter="\t")

        for row in reader:
            new = self.convert_row(row)
            if new is not None:
                yield new
