import csv
import datetime
import io
import zipfile
from collections import OrderedDict
from pathlib import Path
from urllib.parse import urlparse

import rows
from rows.plugins.postgresql import PostgresCopy
from rows.utils import ProgressBar
from rows.utils.date import date_range
from rows.utils.download import Aria2cDownloader, Download

DOWNLOAD_PATH = Path(__file__).parent.parent / "data" / "download"


def today():
    return datetime.datetime.now().date()


def last_month():
    now = today()
    return now - datetime.timedelta(days=now.day + 1)


class BaseDownloader:
    base_url = None
    name = None
    start_date = None
    end_date = None
    publish_frequency = None
    filename_suffix = None

    @classmethod
    def get_name(cls):
        raise NotImplementedError()

    @classmethod
    def _name(cls):
        if not hasattr(cls, "__name"):
            try:
                cls.__name = cls.get_name()
            except NotImplementedError:
                cls.__name = cls.name
        return cls.__name

    @classmethod
    def make_filename(cls, download_path, url):
        filename = urlparse(url).path.rsplit("/", maxsplit=1)[-1] + ".zip"
        return Path(download_path) / cls._name() / filename

    @classmethod
    def urls(cls, start_date, end_date):
        for date in date_range(start=start_date, stop=end_date, step=cls.publish_frequency):
            try:
                url = cls.get_base_url(year=date.year, month=date.month, day=date.day)
            except NotImplementedError:
                url = cls.base_url.format(year=date.year, month=date.month, day=date.day)
            yield url

    @classmethod
    def get_base_url(cls, year, month, day):
        raise NotImplementedError()

    @classmethod
    def download(cls, download_path, start_date=None, end_date=None):
        downloader = Aria2cDownloader(
            max_concurrent_downloads=2,
            max_connections_per_download=1,
            split_download_parts=1,
        )
        for url in cls.urls(start_date=start_date or cls.start_date, end_date=end_date or cls.end_date):
            downloader.add(
                Download(
                    url=url,
                    filename=cls.make_filename(download_path, url),
                )
            )
        downloader.run()

    @classmethod
    def import_psql(
        cls,
        download_path,
        database_url,
        start_date=None,
        end_date=None,
        table_name=None,
        encoding="iso-8859-15",
        dialect="excel-semicolon",
        unlogged=False,
        columnar=False,
    ):
        table_name = table_name or cls._name()

        rows_imported = 0
        pgcopy = PostgresCopy(database_url)
        for url in cls.urls(start_date=start_date or cls.start_date, end_date=end_date or cls.end_date):
            zip_filename = cls.make_filename(download_path, url)
            zf = zipfile.ZipFile(str(zip_filename))

            selected_file_info = None
            for file_info in zf.filelist:
                if file_info.filename.endswith(cls.filename_suffix):
                    selected_file_info = file_info
                    break
            if selected_file_info is None:
                print(f"WARNING: inner file not found for {zip_filename}")
                continue

            fobj = io.TextIOWrapper(zf.open(selected_file_info.filename), encoding=encoding)
            field_names = next(csv.reader(fobj, dialect=dialect))
            fobj.close()
            schema = OrderedDict(
                [
                    (field_name, rows.fields.TextField)
                    for field_name in rows.fields.make_header(field_names, max_size=63)
                ]
            )

            progress_bar = ProgressBar(prefix=f"Importing {Path(zip_filename).name}", unit="bytes")
            result = pgcopy.import_from_fobj(
                fobj=zf.open(selected_file_info.filename),
                table_name=table_name,
                encoding=encoding,
                dialect=dialect,
                schema=schema,
                has_header=True,
                unlogged=unlogged,
                access_method="columnar" if columnar is True else None,
                callback=progress_bar.update,
            )
            progress_bar.close()
            rows_imported += result["rows_imported"]
        print(f"Total rows imported: {rows_imported}")


class AuxilioEmergencialDownloader(BaseDownloader):
    name = "auxilio_emergencial"
    base_url = "https://transparencia.gov.br/download-de-dados/auxilio-emergencial/{year}{month:02d}"
    start_date = datetime.date(2020, 4, 1)
    end_date = last_month()
    publish_frequency = "monthly"
    filename_suffix = "_AuxilioEmergencial.csv"
    # TODO: fix city name


class DespesaEmpenhoDownloader(BaseDownloader):
    name = "despesa_empenho"
    base_url = "https://transparencia.gov.br/download-de-dados/despesas/{year}{month:02d}{day:02d}"
    start_date = datetime.date(2013, 3, 31)
    end_date = today()
    publish_frequency = "daily"
    filename_suffix = "_Despesas_Empenho.csv"


class DespesaItemEmpenhoDownloader(BaseDownloader):
    name = "despesa_item_empenho"
    base_url = "https://transparencia.gov.br/download-de-dados/despesas/{year}{month:02d}{day:02d}"
    start_date = datetime.date(2013, 3, 31)
    end_date = today()
    publish_frequency = "daily"
    filename_suffix = "_Despesas_ItemEmpenho.csv"
    # TODO: parse row["descricao"] if row["elemento_despesa"] == "MATERIAL DE CONSUMO"


class DespesaFavorecidoDownloader(BaseDownloader):
    name = "despesa_favorecido"
    base_url = "https://transparencia.gov.br/download-de-dados/despesas-favorecidos/{year}{month:02d}"
    start_date = datetime.date(2013, 3, 1)
    end_date = last_month()
    publish_frequency = "monthly"
    filename_suffix = "_RecebimentosRecursosPorFavorecido.csv"


class ExecucaoDespesaDownloader(BaseDownloader):
    name = "execucao_despesa"
    base_url = "https://transparencia.gov.br/download-de-dados/despesas-execucao/{year}{month:02d}"
    start_date = datetime.date(2014, 1, 1)
    end_date = today()
    publish_frequency = "monthly"
    filename_suffix = "_Despesas.csv"


class OrcamentoDespesaDownloader(BaseDownloader):
    name = "orcamento_despesa"
    base_url = "https://transparencia.gov.br/download-de-dados/orcamento-despesa/{year}"
    start_date = datetime.date(2014, 1, 1)
    end_date = today()
    publish_frequency = "yearly"
    filename_suffix = "_OrcamentoDespesa.zip.csv"


class PagamentoDownloader(BaseDownloader):
    name = "pagamento"
    base_url = "https://transparencia.gov.br/download-de-dados/despesas/{year}{month:02d}{day:02d}"
    start_date = datetime.date(2013, 3, 31)
    end_date = today()
    publish_frequency = "daily"
    filename_suffix = "_Despesas_Pagamento.csv"


class PagamentoHistoricoDownloader(BaseDownloader):
    name = "pagamento_historico"
    base_url = "https://transparencia.gov.br/download-de-dados/historico-gastos-diretos-pagamentos/{year}{month:02d}"
    start_date = datetime.date(2011, 1, 1)
    end_date = datetime.date(2012, 12, 31)
    publish_frequency = "monthly"
    # TODO: check if NotNullTextWrapper is needed here


class BaseServidorDownloader(BaseDownloader):
    dataset = None
    start_date = last_month()
    end_date = last_month()
    publish_frequency = "monthly"
    filename_suffix = "_Cadastro.csv"
    # TODO: implement schema?

    @classmethod
    def get_name(cls):
        return cls.dataset.lower()

    @classmethod
    def get_base_url(cls, year, month, day=None):
        return f"https://transparencia.gov.br/download-de-dados/servidores/{year:04d}{month:02d}_{cls.dataset}"


class ServidorAposentadosBacenDownloader(BaseServidorDownloader):
    dataset = "Aposentados_BACEN"


class ServidorAposentadosSiapeDownloader(BaseServidorDownloader):
    dataset = "Aposentados_SIAPE"


class ServidorMilitaresDownloader(BaseServidorDownloader):
    dataset = "Militares"


class ServidorPensionistasBacenDownloader(BaseServidorDownloader):
    dataset = "Pensionistas_BACEN"


class ServidorPensionistasDefesaDownloader(BaseServidorDownloader):
    dataset = "Pensionistas_DEFESA"


class ServidorPensionistasSiapeDownloader(BaseServidorDownloader):
    dataset = "Pensionistas_SIAPE"


class ServidorReservaReformaMilitaresDownloader(BaseServidorDownloader):
    dataset = "Reserva_Reforma_Militares"


class ServidorBacenDownloader(BaseServidorDownloader):
    dataset = "Servidores_BACEN"


class ServidorSiapeDownloader(BaseServidorDownloader):
    dataset = "Servidores_SIAPE"


def subclasses(cls):
    """Return all subclasses of a class, recursively"""
    children = cls.__subclasses__()
    return set(children).union(set(grandchild for child in children for grandchild in subclasses(child)))


if __name__ == "__main__":
    import argparse
    import datetime
    import os

    datasets = {cls._name(): cls for cls in subclasses(BaseDownloader) if not cls.__name__.startswith("Base")}
    parser = argparse.ArgumentParser()
    parser.add_argument("--download-path")
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument("--database-url")
    parser.add_argument("dataset", nargs="+", choices=list(datasets.keys()))
    args = parser.parse_args()
    start_date = args.start_date
    if start_date:
        start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d").date()
    end_date = args.end_date
    if end_date:
        end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d").date()
    database_url = args.database_url or os.environ["DATABASE_URL"]
    if args.download_path:
        download_path = Path(args.download_path)
    else:
        download_path = Path(__file__).parent / "data" / "portal-transparencia"
    if not download_path.exists():
        download_path.mkdir()

    for dataset in args.dataset:
        Downloader = datasets[dataset]
        Downloader.download(download_path, start_date, end_date)
        Downloader.import_psql(
            download_path,
            database_url,
            table_name=f"{Downloader.get_name()}_orig",
            start_date=start_date,
            end_date=end_date,
            columnar=True,
        )
