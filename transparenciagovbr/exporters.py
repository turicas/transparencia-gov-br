import gzip

from scrapy.exporters import CsvItemExporter


# Code from <https://github.com/scrapy/scrapy/issues/2174>
class GzipCsvItemExporter(CsvItemExporter):
    """Gzip-compressed CSV exporter

    To use it, add
    ::

        FEED_EXPORTERS = {
            'csv.gz': 'myproject.exporters.GzipCsvItemExporter',
        }
        FEED_FORMAT = 'csv.gz'

    to settings.py and then run scrapy crawl like this::

        scrapy crawl foo -o item.csv.gz

    (if `FEED_FORMAT` is not explicitly specified, you'll need to add
    `-t csv.gz` to the command above)
    """

    def __init__(self, stream, **kwargs):
        self.gzfile = gzip.GzipFile(fileobj=stream)
        super(GzipCsvItemExporter, self).__init__(self.gzfile, **kwargs)

    def finish_exporting(self):
        self.gzfile.close()
