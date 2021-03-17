import csv
import io
import zipfile
from urllib.parse import urlparse

import scrapy
from cached_property import cached_property

from transparenciagovbr import settings
from transparenciagovbr.utils.date import date_range, date_to_dict
from transparenciagovbr.utils.fields import Schema
from transparenciagovbr.utils.io import parse_zip


class TransparenciaBaseSpider(scrapy.Spider):
    allowed_domains = [
        "portaldatransparencia.gov.br",
        "transparencia.gov.br",
        "data.brasil.io",
    ]
    encoding = "iso-8859-1"
    mirror_url = "https://data.brasil.io/mirror/transparenciagovbr/{dataset}/{filename}"

    def __init__(self, use_mirror="False", save_file="True", *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.use_mirror = use_mirror.lower() == "true"
        self.save_file = save_file.lower() == "true"
        self.schema = Schema(self.schema_filename)

    def make_filename(self, url):
        return settings.DOWNLOAD_PATH / self.name / urlparse(url).path.rsplit("/", maxsplit=1)[-1]

    def start_requests(self):
        for date in date_range(
            start=self.start_date, stop=self.end_date, interval=self.publish_frequency
        ):
            url = self.base_url.format(**date_to_dict(date))
            if self.use_mirror:
                url = self.mirror_url.format(
                    dataset=self.name,
                    filename=urlparse(url).path.rsplit("/", maxsplit=1)[-1],
                )
            elif self.save_file:
                filename = self.make_filename(url)
                if filename.exists():
                    url = f"file://{filename.absolute()}"
            yield scrapy.Request(url, callback=self.parse_zip_response)

    def parse_zip_response(self, response):
        # If it's set to save file and the response comes from the Web, then
        # save it to the disk.
        if self.save_file and not response.request.url.startswith("file://"):
            filename = self.make_filename(response.request.url)
            if not filename.parent.exists():
                filename.parent.mkdir(parents=True)
            with open(filename, mode="wb") as fobj:
                fobj.write(response.body)

        data = parse_zip(
            filename_or_fobj=io.BytesIO(response.body),
            inner_filename_suffix=self.filename_suffix,
            encoding=self.encoding,
        )
        for row in data:
            new = self.schema.deserialize(row)
            if new is not None:
                yield new
