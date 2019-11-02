import csv
import datetime
import io
import zipfile

import rows

from transparenciagovbr import settings
from transparenciagovbr.fields import BrazilianDateField, MoneyRealField
from transparenciagovbr.spiders.base import TransparenciaBaseSpider
from transparenciagovbr.utils.io import NotNullTextWrapper


SCHEMA_PATH = str(
    (settings.REPOSITORY_PATH / "schema" / "pagamento-hist.csv").absolute()
)
SCHEMA = rows.utils.load_schema(
    SCHEMA_PATH,
    context={
        "date": BrazilianDateField,
        "text": rows.fields.TextField,
        "integer": rows.fields.IntegerField,
        "money_real": MoneyRealField,
    },
)
FIELD_MAPPING = {
    row.original_name: row.field_name for row in rows.import_from_csv(SCHEMA_PATH)
}


class PagamentoHistSpider(TransparenciaBaseSpider):
    name = "pagamento-hist"
    base_url = "http://www.portaltransparencia.gov.br/download-de-dados/historico-gastos-diretos-pagamentos/{year}{month:02d}"
    start_date = datetime.date(2011, 1, 1)
    end_date = datetime.date(2012, 12, 31)
    publish_frequency = "monthly"
    schema = SCHEMA
    field_mapping = FIELD_MAPPING

    def parse_zip(self, response):
        zf = zipfile.ZipFile(io.BytesIO(response.body))
        assert len(zf.filelist) == 1
        fobj = NotNullTextWrapper(
            zf.open(zf.filelist[0].filename), encoding="iso-8859-1"
        )
        reader = csv.DictReader(fobj, delimiter="\t")

        for row in reader:
            yield self.convert_row(row)
