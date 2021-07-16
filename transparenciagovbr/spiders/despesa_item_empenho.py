import datetime
from decimal import Decimal, InvalidOperation
from urllib.parse import urlparse

from transparenciagovbr import settings
from transparenciagovbr.spiders.base import TransparenciaBaseSpider
from transparenciagovbr.utils.date import today


class Text(str):

    def until(self, substr):
        return Text(self[:self.find(substr)])

    def starting_at(self, substr):
        return Text(self[self.find(substr):])

    def after(self, substr):
        return Text(self[self.find(substr) + len(substr):])


def parse_description(text):
    """Extrai dados estruturados do texto da descrição"""

    new = {
        "descricao": "",
        "item": "",
        "item_material": "",
        "item_processo": "",
        "marca": "",
        "quantidade": "",
        "unidade": "",
    }

    if len(text) < 78 or "MARCA:" not in text:
        return new

    # TODO: verificar a possibilidade de transformar essa função num conjunto
    # de expressões regulares (provavelmente rodarão mais rapidamente)

    part1, part2 = Text(text[:78].strip()), Text(text[78:])
    try:
        new["quantidade"] = Decimal(part1.until(" ").strip().replace(".", "").replace(",", "."))
    except InvalidOperation:
        return new
    new["unidade"] = part1.after(" ").strip()

    item = part2.until(",")
    if item and item[0] == item[-1] == "'":
        item = item[1:-1]
    new["item"] = item
    rest = part2.after(",")

    new["descricao"] = rest.until("MARCA:").strip()
    rest = rest.after("MARCA:")

    new["marca"] = rest.until("ITEM DO PROCESSO:").strip()
    rest = rest.after("ITEM DO PROCESSO:")

    new["item_processo"] = rest.until("ITEM DE MATERIAL:").strip()
    rest = rest.after("ITEM DE MATERIAL:")

    new["item_material"] = rest.strip()

    return new


def extract_extra_fields(row):
    new = {
        "quantidade": None,
        "unidade": None,
        "item": None,
        "marca": None,
        "item_processo": None,
        "item_material": None,
        "descricao": None,
    }
    if row["elemento_despesa"] != "MATERIAL DE CONSUMO":
        return new
    # TODO: add fields related to services and other elements

    new.update(parse_description(row["descricao"]))
    return new

class DespesaMixin:
    def make_filename(self, url):
        return settings.DOWNLOAD_PATH / "despesa" / urlparse(url).path.rsplit("/", maxsplit=1)[-1]


class DespesaItemEmpenhoSpider(DespesaMixin, TransparenciaBaseSpider):
    name = "despesa_item_empenho"
    base_url = "http://transparencia.gov.br/download-de-dados/despesas/{year}{month:02d}{day:02d}"
    start_date = datetime.date(2013, 3, 31)
    end_date = today()
    publish_frequency = "daily"
    filename_suffix = "_Despesas_ItemEmpenho.csv"
    schema_filename = "despesa_item_empenho.csv"

    def parse_zip_response(self, response):
        for row in super().parse_zip_response(response):
            row.update(extract_extra_fields(row))
            yield row


class DespesaEmpenhoSpider(DespesaMixin, TransparenciaBaseSpider):
    name = "despesa_empenho"
    base_url = "http://transparencia.gov.br/download-de-dados/despesas/{year}{month:02d}{day:02d}"
    start_date = datetime.date(2013, 3, 31)
    end_date = today()
    publish_frequency = "daily"
    filename_suffix = "_Despesas_Empenho.csv"
    schema_filename = "despesa_empenho.csv"
