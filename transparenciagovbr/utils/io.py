from csv import DictReader
from io import TextIOWrapper
from zipfile import ZipFile


class NotNullTextWrapper(TextIOWrapper):
    def read(self, *args, **kwargs):
        data = super().read(*args, **kwargs)
        return data.replace("\x00", "")

    def readline(self, *args, **kwargs):
        data = super().readline(*args, **kwargs)
        return data.replace("\x00", "")


def parse_zip(filename_or_fobj, inner_filename_suffix, encoding):
    zf = ZipFile(filename_or_fobj)
    for file_info in zf.filelist:
        if file_info.filename.endswith(inner_filename_suffix):
            fobj = TextIOWrapper(
                zf.open(file_info.filename), encoding=encoding
            )
            reader = DictReader(fobj, delimiter=";")
            for row in reader:
                yield row
