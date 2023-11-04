import csv
import datetime
import io
import zipfile
from collections import OrderedDict
from pathlib import Path
from urllib.parse import urlparse

import rows
from rows.fields import slug
from rows.plugins.postgresql import PostgresCopy, get_psql_command
from rows.utils import ProgressBar, execute_command
from rows.utils.date import date_range
from rows.utils.download import Aria2cDownloader, Download


DOWNLOAD_PATH = Path(__file__).parent.parent / "data" / "download"
SCHEMA_PATH = Path(__file__).parent / "schema"


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
    schema_filename = None

    @classmethod
    def get_name(cls):
        if cls.name is None:
            raise NotImplementedError()
        return cls.name

    @classmethod
    def make_filename(cls, download_path, url):
        filename = urlparse(url).path.rsplit("/", maxsplit=1)[-1] + ".zip"
        return Path(download_path) / cls.get_name() / filename

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
    def get_date_range_from_url(cls, url):
        return urlparse(url).path.rsplit("/", maxsplit=1)[-1]

    @classmethod
    def read_schema(cls):
        with open(SCHEMA_PATH / cls.schema_filename) as fobj:
            return list(csv.DictReader(fobj))

    @classmethod
    def schema_field_names(cls):
        return rows.fields.make_header(
            [row["original_name"] for row in cls.read_schema() if row["original_name"]],
            max_size=63,
        )

    @classmethod
    def zipped_csv_field_names(cls, zipfobj, inner_filename, encoding, dialect):
        fobj = io.TextIOWrapper(zipfobj.open(inner_filename), encoding=encoding)
        field_names = next(csv.reader(fobj, dialect=dialect))
        fobj.close()
        return rows.fields.make_header(field_names, max_size=63)

    @classmethod
    def text_schema_from_field_names(cls, field_names):
        return OrderedDict([(field_name, rows.fields.TextField) for field_name in field_names])

    @classmethod
    def select_sql(cls, date_range, temp_table_name):
        "Create SQL SELECT with transformations needed to insert data into final (clean) table, including date range"
        if len(date_range) == 6: # YYYYMM
            date_value = f"{date_range[:4]}-{date_range[4:]}-01"
        elif len(date_range) == 8: # YYYYMMDD
            date_value = f"{date_range[:4]}-{date_range[4:6]}-{date_range[6:]}"
        else:
            raise ValueError(f"Invalid date_range format: {date_range}")
        # TODO: escape SQL table/field names with quotes
        transformations = [
            f"{row['transformation']} AS {row['field_name']}"
            for row in cls.read_schema()
            if row["field_name"] and row["transformation"]
        ]
        return (
            f"SELECT '{date_value}'::date AS periodo, "
            + ", ".join(transformations)
            + f" FROM {temp_table_name}"
        )

    @classmethod
    def download(cls, download_path, start_date=None, end_date=None):
        downloader = Aria2cDownloader(
            max_concurrent_downloads=2,
            max_connections_per_download=1,
            split_download_parts=1,
            user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36",
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
    def create_temp_table_name(cls, pattern, downloader, date_range):
        return pattern.format(
            downloader=downloader,
            date_range=date_range,
            date_range_slug=slug(date_range),
        )

    @classmethod
    def import_psql(
        cls,
        download_path,
        database_url,
        start_date=None,
        end_date=None,
        temp_table_name_pattern=None,
        encoding="iso-8859-15",
        dialect="excel-semicolon",
        delete_files_after=False,
    ):
        downloader = cls.get_name()
        table_name = downloader
        drop_sql = f'DROP TABLE IF EXISTS "{table_name}"'
        print("Ensuring final table does not exist...")
        execute_command(get_psql_command(drop_sql, database_uri=database_url))

        rows_imported = 0
        pgcopy = PostgresCopy(database_url)
        for url in cls.urls(start_date=start_date or cls.start_date, end_date=end_date or cls.end_date):
            date_range = cls.get_date_range_from_url(url)
            zip_filename = cls.make_filename(download_path, url)
            print(f"Processing {Path(zip_filename).name}")
            if not zip_filename.exists():
                print(f"  WARNING: file {zip_filename} not found - skipping")
                continue
            zf = zipfile.ZipFile(str(zip_filename))

            selected_file_info = None
            for file_info in zf.filelist:
                if file_info.filename.endswith(cls.filename_suffix):
                    selected_file_info = file_info
                    break
            if selected_file_info is None:
                print(f"  WARNING: inner file not found for {zip_filename}")
                continue

            csv_field_names = cls.zipped_csv_field_names(zf, selected_file_info.filename, encoding=encoding, dialect=dialect)
            expected_field_names = cls.schema_field_names()
            assert csv_field_names == expected_field_names, f"Invalid field names in CSV - expected: {expected_field_names}, got: {csv_field_names}"
            temp_table_name = cls.create_temp_table_name(
                temp_table_name_pattern,
                downloader=downloader,
                date_range=date_range,
            )
            drop_sql = f'DROP TABLE IF EXISTS "{temp_table_name}"'
            print("  Ensuring temporary table does not exist...")
            execute_command(get_psql_command(drop_sql, database_uri=database_url))

            progress_bar = ProgressBar(prefix="  Importing", unit="bytes")
            result = pgcopy.import_from_fobj(
                fobj=zf.open(selected_file_info.filename),
                table_name=temp_table_name,
                encoding=encoding,
                dialect=dialect,
                schema=cls.text_schema_from_field_names(csv_field_names),
                create_table=True,
                has_header=True,
                unlogged=True,  # This is a temporary table, so let's save time
                callback=progress_bar.update,
            )
            progress_bar.close()
            if delete_files_after:
                zip_filename.unlink()
            if rows_imported == 0:
                insert_sql = f'CREATE TABLE "{table_name}" AS {cls.select_sql(date_range, temp_table_name)}'
            else:
                insert_sql = f'INSERT INTO "{table_name}" {cls.select_sql(date_range, temp_table_name)}'
            print(f"  {result['rows_imported']} rows imported, converting and inserting into final table...")
            execute_command(get_psql_command(insert_sql, database_uri=database_url))
            print("  Deleting temporary table...")
            execute_command(get_psql_command(drop_sql, database_uri=database_url))
            rows_imported += result["rows_imported"]
        print(f"Total rows imported: {rows_imported}")


class AuxilioEmergencialDownloader(BaseDownloader):
    name = "auxilio_emergencial"
    base_url = "https://transparencia.gov.br/download-de-dados/auxilio-emergencial/{year}{month:02d}"
    start_date = datetime.date(2020, 4, 1)
    end_date = last_month()
    publish_frequency = "monthly"
    filename_suffix = "_AuxilioEmergencial.csv"
    # TODO: schema_filename = "auxilio_emergencial.csv"
    # TODO: fix city name


class BaseDespesaDownloader(BaseDownloader):
    base_url = "https://transparencia.gov.br/download-de-dados/despesas/{year}{month:02d}{day:02d}"
    start_date = datetime.date(2013, 3, 31)
    end_date = today()
    publish_frequency = "daily"


class DespesaEmpenhoDownloader(BaseDespesaDownloader):
    name = "despesa_empenho"
    filename_suffix = "_Despesas_Empenho.csv"
    # TODO: schema_filename = "despesa_empenho.csv"


class DespesaItemEmpenhoDownloader(BaseDespesaDownloader):
    name = "despesa_item_empenho"
    filename_suffix = "_Despesas_ItemEmpenho.csv"
    # TODO: parse row["descricao"] if row["elemento_despesa"] == "MATERIAL DE CONSUMO"
    # TODO: schema_filename = "despesa_item_empenho.csv"


class PagamentoDownloader(BaseDespesaDownloader):
    name = "pagamento"
    filename_suffix = "_Despesas_Pagamento.csv"
    # TODO: schema_filename = "pagamento.csv"


class DespesaFavorecidoDownloader(BaseDownloader):
    name = "despesa_favorecido"
    base_url = "https://transparencia.gov.br/download-de-dados/despesas-favorecidos/{year}{month:02d}"
    start_date = datetime.date(2013, 3, 1)
    end_date = last_month()
    publish_frequency = "monthly"
    filename_suffix = "_RecebimentosRecursosPorFavorecido.csv"
    # TODO: schema_filename = "despesa_favorecido.csv"


class ExecucaoDespesaDownloader(BaseDownloader):
    name = "execucao_despesa"
    base_url = "https://transparencia.gov.br/download-de-dados/despesas-execucao/{year}{month:02d}"
    start_date = datetime.date(2014, 1, 1)
    end_date = today()
    publish_frequency = "monthly"
    filename_suffix = "_Despesas.csv"
    # TODO: schema_filename = "execucao_despesa.csv"


class OrcamentoDespesaDownloader(BaseDownloader):
    name = "orcamento_despesa"
    base_url = "https://transparencia.gov.br/download-de-dados/orcamento-despesa/{year}"
    start_date = datetime.date(2014, 1, 1)
    end_date = today()
    publish_frequency = "yearly"
    filename_suffix = "_OrcamentoDespesa.zip.csv"
    # TODO: schema_filename = "orcamento_despesa.csv"


class PagamentoHistoricoDownloader(BaseDownloader):
    name = "pagamento_historico"
    base_url = "https://transparencia.gov.br/download-de-dados/historico-gastos-diretos-pagamentos/{year}{month:02d}"
    start_date = datetime.date(2011, 1, 1)
    end_date = datetime.date(2012, 12, 31)
    publish_frequency = "monthly"
    # TODO: schema_filename = "pagamento_historico.csv"
    # TODO: check if NotNullTextWrapper is needed here


class BaseServidorDownloader(BaseDownloader):
    dataset = None  # Must define in subclass
    end_date = last_month()
    filename_suffix = "_Cadastro.csv"
    publish_frequency = "monthly"
    schema_filename = None  # Must define in subclass
    start_date = None  # Must define in subclass

    @classmethod
    def get_name(cls):
        return cls.dataset.lower()

    @classmethod
    def get_base_url(cls, year, month, day=None):
        return f"https://transparencia.gov.br/download-de-dados/servidores/{year:04d}{month:02d}_{cls.dataset}"

    @classmethod
    def get_date_range_from_url(cls, url):
        return urlparse(url).path.rsplit("/", maxsplit=1)[-1].split("_")[0]


class ServidorAposentadosBacenDownloader(BaseServidorDownloader):
    dataset = "Aposentados_BACEN"
    # TODO: schema_filename = "aposentado_bacen.csv"
    start_date = datetime.date(2020, 1, 1)


class ServidorAposentadosSiapeDownloader(BaseServidorDownloader):
    dataset = "Aposentados_SIAPE"
    schema_filename = "aposentados_siape.csv"
    start_date = datetime.date(2020, 1, 1)


class ServidorMilitaresDownloader(BaseServidorDownloader):
    dataset = "Militares"
    # TODO: schema_filename = "servidor_militar.csv"
    start_date = datetime.date(2013, 1, 1)


class ServidorPensionistasBacenDownloader(BaseServidorDownloader):
    dataset = "Pensionistas_BACEN"
    # TODO: schema_filename = "pensionista_bacen.csv"
    start_date = datetime.date(2020, 1, 1)


class ServidorPensionistasDefesaDownloader(BaseServidorDownloader):
    dataset = "Pensionistas_DEFESA"
    schema_filename = "pensionista_defesa.csv"
    start_date = datetime.date(2020, 1, 1)


class ServidorPensionistasSiapeDownloader(BaseServidorDownloader):
    dataset = "Pensionistas_SIAPE"
    # TODO: schema_filename = "pensionista_siape.csv"
    start_date = datetime.date(2020, 1, 1)


class ServidorReservaReformaMilitaresDownloader(BaseServidorDownloader):
    dataset = "Reserva_Reforma_Militares"
    # TODO: schema_filename = "reserva_militar.csv"
    start_date = datetime.date(2020, 1, 1)


class ServidorBacenDownloader(BaseServidorDownloader):
    dataset = "Servidores_BACEN"
    # TODO: schema_filename = "servidor_bacen.csv"
    start_date = datetime.date(2013, 1, 1)


class ServidorSiapeDownloader(BaseServidorDownloader):
    dataset = "Servidores_SIAPE"
    # TODO: schema_filename = "servidor_siape.csv"
    start_date = datetime.date(2013, 1, 1)


def subclasses(cls):
    """Return all subclasses of a class, recursively"""
    children = cls.__subclasses__()
    return set(children).union(set(grandchild for child in children for grandchild in subclasses(child)))


if __name__ == "__main__":
    import argparse
    import datetime
    import os

    datasets = {cls.get_name(): cls for cls in subclasses(BaseDownloader) if not cls.__name__.startswith("Base")}
    parser = argparse.ArgumentParser()
    parser.add_argument("--download-only", action="store_true")
    parser.add_argument("--delete-files-after", action="store_true")
    parser.add_argument("--download-path")
    parser.add_argument("--temp-table-name-pattern", default="{downloader}_{date_range_slug}_orig")
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument("--database-url")
    parser.add_argument("dataset", nargs="+", choices=sorted(datasets.keys()))
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

    # TODO: create a compose.yml with postgres service
    # TODO: execute `sql/functions.sql` and `sql/urlid.sql` before everything
    for dataset in args.dataset:
        Downloader = datasets[dataset]
        Downloader.download(download_path, start_date, end_date)
        if not args.download_only:
            Downloader.import_psql(
                download_path,
                database_url,
                temp_table_name_pattern=args.temp_table_name_pattern,
                start_date=start_date,
                end_date=end_date,
                delete_files_after=args.delete_files_after,
            )
