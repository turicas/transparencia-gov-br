import csv
import datetime
import io
import zipfile
from pathlib import Path

import rows
import scrapy

from transparenciagovbr import settings


def date_range(start, stop, delta=datetime.timedelta(days=1)):
    current = start
    while current < stop:
        yield current
        current += delta


def today():
    date = datetime.datetime.now()
    return datetime.date(date.year, date.month, date.day)


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


SCHEMA_FILENAME = str(
    (settings.REPOSITORY_PATH / "schema" / "pagamento.csv").absolute()
)
SCHEMA = rows.utils.load_schema(
    SCHEMA_FILENAME,
    context={
        "date": BrazilianDateField,
        "text": rows.fields.TextField,
        "integer": rows.fields.IntegerField,
        "money_real": MoneyRealField,
    },
)
FIELD_MAPPING = {
    row.original_name: row.field_name for row in rows.import_from_csv(SCHEMA_FILENAME)
}


def convert_row(row):
    return {
        field_name: SCHEMA[field_name].deserialize(row[original_field_name])
        for original_field_name, field_name in FIELD_MAPPING.items()
    }


class PagamentoSpider(scrapy.Spider):
    name = "pagamento"
    allowed_domains = ["portaldatransparencia.gov.br"]
    base_url = "http://www.portaldatransparencia.gov.br/download-de-dados/despesas/{year}{month:02d}{day:02d}"
    archive_url = "http://www.portaltransparencia.gov.br/download-de-dados/historico-gastos-diretos-pagamentos/{year}{month:02d}"
    start_date = datetime.date(2011, 1, 1)
    end_date = today()

    def start_requests(self):
        for date in date_range(self.start_date, self.end_date):
            if datetime.date(2011, 1, 1) <= date <= datetime.date(2013, 12, 31):
                # 2011-2013 are in the "historical" section and they have data
                # per month, not per day
                if date.day != 1:  # Only one request per month
                    continue
                yield scrapy.Request(
                    self.archive_url.format(year=date.year, month=date.month),
                    callback=self.parse_archived_zip,
                )
            else:
                yield scrapy.Request(
                    self.base_url.format(year=date.year, month=date.month, day=date.day),
                    callback=self.parse_zip,
                )

    def parse_zip(self, response):
        zf = zipfile.ZipFile(io.BytesIO(response.body))
        for file_info in zf.filelist:
            # TODO: move this string to class attribute
            if file_info.filename.endswith("_Despesas_Pagamento.csv"):
                fobj = io.TextIOWrapper(
                    zf.open(file_info.filename), encoding="iso-8859-1"
                )
                reader = csv.DictReader(fobj, delimiter=";")
                for row in reader:
                    yield convert_row(row)

    def parse_archived_zip(self, response):
        zf = zipfile.ZipFile(io.BytesIO(response.body))
        assert len(zf.filelist) == 1
        fobj = io.TextIOWrapper(
            zf.open(zf.filelist[0].filename), encoding="iso-8859-1"
        )
        reader = csv.DictReader(fobj, delimiter="\t")
        for row in reader:
            # TODO: convert_row must be a class method
            yield convert_row(row)
