import re
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
        filename = file_info.filename
        if isinstance(inner_filename_suffix, re.Pattern):
            file_matches = bool(inner_filename_suffix.findall(filename))
        else:
            file_matches = filename.endswith(inner_filename_suffix)

        if file_matches:
            fobj = TextIOWrapper(
                zf.open(filename), encoding=encoding
            )
            reader = DictReader(fobj, delimiter=";")
            for row in reader:
                yield row
