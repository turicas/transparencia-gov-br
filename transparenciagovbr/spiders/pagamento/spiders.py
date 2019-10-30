import datetime

import rows

from transparenciagovbr import settings
from transparenciagovbr.spiders.base import TransparenciaBaseSpider
from transparenciagovbr.utils.date import today


class BrazilianDateField(rows.fields.DateField):
    INPUT_FORMAT = "%d/%m/%Y"


class MoneyRealField(rows.fields.DecimalField):
    @classmethod
    def deserialize(cls, value):
        """
        >>> MoneyRealField.deserialize("89188,11")
        '89188.11'
        """
        value = value.replace(",", ".")
        return super().deserialize(value)


SCHEMA_PATH = str((settings.REPOSITORY_PATH / "schema" / "pagamento.csv").absolute())
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


class PagamentoSpider(TransparenciaBaseSpider):
    name = "pagamento"
    base_url = "http://www.portaldatransparencia.gov.br/download-de-dados/despesas/{year}{month:02d}{day:02d}"
    start_date = datetime.date(2013, 1, 1)
    end_date = today()
    publish_frequency = "daily"
    filename_suffix = "_Despesas_Pagamento.csv"
    schema = SCHEMA
    field_mapping = FIELD_MAPPING
