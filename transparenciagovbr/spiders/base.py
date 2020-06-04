import csv
import io
import zipfile

import scrapy
from cached_property import cached_property

from transparenciagovbr.utils.date import date_range, date_to_dict
from transparenciagovbr.utils.fields import field_mapping_from_csv, load_schema


class TransparenciaBaseSpider(scrapy.Spider):
    allowed_domains = ["portaldatransparencia.gov.br"]

    @cached_property
    def schema(self):
        return load_schema(self.schema_filename)

    @cached_property
    def field_mapping(self):
        return field_mapping_from_csv(self.schema_filename)

    def start_requests(self):
        for date in date_range(
            start=self.start_date, stop=self.end_date, interval=self.publish_frequency
        ):
            yield scrapy.Request(
                self.base_url.format(**date_to_dict(date)), callback=self.parse_zip
            )

    def convert_row(self, row):
        new = {}
        keys_not_found = set()
        for original_field_name, field_name in self.field_mapping.items():
            try:
                value = row.pop(original_field_name)
            except KeyError:
                keys_not_found.add(original_field_name)
                value = None
            try:
                new[field_name] = self.schema[field_name].deserialize(value)
            except ValueError:
                self.logger.error(f"Wrong value for {field_name}: {repr(value)}")
                return None
        if row:
            missing_schema_keys = ", ".join(sorted(row.keys()))
            self.logger.warning(f"Missing following keys in schema: {missing_schema_keys}")
        if keys_not_found:
            keys_not_found = ", ".join(sorted(keys_not_found))
            self.logger.warning(f"Missing following keys in CSV: {keys_not_found}")
        return new

    def parse_zip(self, response):
        zf = zipfile.ZipFile(io.BytesIO(response.body))
        for file_info in zf.filelist:
            if file_info.filename.endswith(self.filename_suffix):
                fobj = io.TextIOWrapper(
                    zf.open(file_info.filename), encoding="iso-8859-1"
                )
                reader = csv.DictReader(fobj, delimiter=";")
                for row in reader:
                    new = self.convert_row(row)
                    if new is not None:
                        yield new
