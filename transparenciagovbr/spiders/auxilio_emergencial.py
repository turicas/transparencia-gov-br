import datetime

from transparenciagovbr.spiders.base import TransparenciaBaseSpider
from transparenciagovbr.utils.cities import city_name_by_id
from transparenciagovbr.utils.date import today

day = today()
last_month = day.month - 1 if day.month > 1 else 12
year = day.year if day.month > 1 else day.year - 1
end_date = datetime.date(year, last_month, day.day)


class AuxilioEmergencialSpider(TransparenciaBaseSpider):
    name = "auxilio_emergencial"
    base_url = "http://transparencia.gov.br/download-de-dados/auxilio-emergencial/{year}{month:02d}"
    start_date = datetime.date(2020, 4, 1)
    end_date = end_date
    publish_frequency = "monthly"
    filename_suffix = "_AuxilioEmergencial.csv"
    schema_filename = "auxilio_emergencial.csv"

    def convert_row(self, row):
        row = super().convert_row(row)

        if row["codigo_ibge_municipio"] is not None:
            # Força nome de município a ser mais bonito (com acentos,
            # maiúsculas e minúsculas). :)
            row["municipio"] = city_name_by_id[row["codigo_ibge_municipio"]]
        return row
