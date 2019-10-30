import csv
import io
import zipfile

import scrapy

from transparenciagovbr.utils.date import date_range, date_to_dict


class TransparenciaBaseSpider(scrapy.Spider):
    allowed_domains = ["portaldatransparencia.gov.br"]

    def start_requests(self):
        for date in date_range(start=self.start_date, stop=self.end_date, interval=self.publish_frequency):
            yield scrapy.Request(
                self.base_url.format(**date_to_dict(date)),
                callback=self.parse_zip,
            )

    def convert_row(self, row):
        return {
            field_name: self.schema[field_name].deserialize(row[original_field_name])
            for original_field_name, field_name in self.field_mapping.items()
        }

    def parse_zip(self, response):
        zf = zipfile.ZipFile(io.BytesIO(response.body))
        for file_info in zf.filelist:
            if file_info.filename.endswith(self.filename_suffix):
                fobj = io.TextIOWrapper(
                    zf.open(file_info.filename), encoding="iso-8859-1"
                )
                reader = csv.DictReader(fobj, delimiter=";")
                for row in reader:
                    yield self.convert_row(row)
