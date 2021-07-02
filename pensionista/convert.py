import csv
import datetime
import io
from functools import lru_cache
from pathlib import Path
from uuid import NAMESPACE_URL, uuid5
from zipfile import ZipFile

from rows.fields import slug
from rows.utils import CsvLazyDictWriter, open_compressed
from tqdm import tqdm


strptime = datetime.datetime.strptime

@lru_cache(maxsize=1024 ** 2)
def convert_number(value):
    return value.replace(".", "").replace(",", ".")


@lru_cache(maxsize=32 * 1024)
def convert_date(value):
    value = value.strip()
    if not value:
        return None
    return str(strptime(value, "%d/%m/%Y").date())


@lru_cache(maxsize=1024 * 1024)
def person_uuid(cpf, name):
    """Create UUID based on URLid methodology"""

    if cpf is None:
        cpf = "***********"
    assert len(cpf) == 11, f"Invalid CPF: {repr(cpf)}"
    internal_id = cpf[3:9] + "-" + slug(name).upper().replace("_", "-")
    return str(uuid5(NAMESPACE_URL, f"https://id.brasil.io/person/v1/{internal_id}/"))


@lru_cache(maxsize=128)
def normalize_key(text):

    text = text.replace("(R$)", "_brl_").replace("(U$)", "_usd_")
    result = (
        slug(text)
        .replace("_registradas_em_sistemas_de_pessoal_", "_")
        .replace("_programa_desligamento_voluntario_mp_792_2017_", "_deslig_voluntario_")
    )
    return result


def convert_row(row):
    new = {}
    for original_key, value in row.items():
        key = normalize_key(original_key)
        value = value.strip()
        if (value and value[0] == "0" and value[-1] == "0" and set(value) == {"0"}) or value in ("-", "--"):
            value = None
        if not key and not value:
            continue

        if key.startswith("data_") and value is not None:
            value = convert_date(value)
        elif value is not None and ("R$" in original_key or "U$" in original_key):
            value = convert_number(value)

        new[key] = value
    return new


def read_csv(fobj, table_name, year, month, input_encoding="iso-8859-1", delimiter=";"):
    """Read binary `fobj` as CSV, convert each row, adding `table_name` as a column"""

    fobj = io.TextIOWrapper(fobj, encoding=input_encoding)
    reader = csv.DictReader(fobj, delimiter=delimiter)
    for row in reader:
        new = convert_row(row)
        if "(*)" in new.get("ano", ""):  # Invalid row
            continue
        if "ano" not in new:
            new["ano"] = year
        if "mes" not in new:
            new["mes"] = month
        if "PENSIONISTA MENOR DE 16 ANOS" in new["cpf"]:
            new["menor_16"] = True
            new["cpf"] = None
        else:
            new["menor_16"] = False
            new["cpf"] = new["cpf"].replace(".", "").replace("-", "")
        new["sistema_origem"] = table_name
        new["pessoa_uuid"] = person_uuid(new["cpf"], new["nome"])
        if table_name == "cadastro":
            for key in ("representante_legal", "instituidor"):
                new[f"cpf_{key}"] = new[f"cpf_{key}"].replace(".", "").replace("-", "")
                new[f"{key}_uuid"] = person_uuid(new[f"cpf_{key}"], new[f"nome_{key}"])
        yield new


def extract_year_month(filename):
    """Extract year and month from ZIP filename"""

    part = filename.name.lower().split(".zip")[0]
    return int(part[:4]), int(part[4:6])


def extract_origin_system(filename):
    return filename.split(".zip")[0].split("_")[-1]


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("table_name", choices=("cadastro", "remuneracao", "observacao"))
    args = parser.parse_args()

    # Make sure all working paths exist before anything
    DATA_PATH = Path(__file__).parent / "data"
    DOWNLOAD_PATH = DATA_PATH / "download"
    OUTPUT_PATH = DATA_PATH / "output"
    for path in (DATA_PATH, DOWNLOAD_PATH, OUTPUT_PATH):
        if not path.exists():
            path.mkdir(parents=True)

    # Create one compressed-CSV writer
    filename = OUTPUT_PATH / f"pensionista_{args.table_name}.csv.gz"
    fobj = open_compressed(filename, mode="w", buffering=8 * 1024 * 1024)
    writer = CsvLazyDictWriter(fobj)

    # Read each ZIP file, then each inner ZIP file, then filter desired
    # inner-inner CSV file, convert it and write to the output CSV.
    progress_bar = tqdm()
    filenames = DOWNLOAD_PATH.glob("*.zip")
    for filename in sorted(filenames, key=extract_year_month):
        year, month = extract_year_month(filename)
        progress_bar.desc = f"{year}-{month:02d}"
        progress_bar.refresh()
        zf = ZipFile(filename)
        for fileinfo in zf.filelist:
            origin_system = extract_origin_system(fileinfo.filename)
            progress_bar.desc = f"{year}-{month:02d}/{origin_system}"
            inner_zf = ZipFile(zf.open(fileinfo.filename))
            for inner_fileinfo in inner_zf.filelist:
                table_name = inner_fileinfo.filename.split(".")[0].split("_")[-1].lower().replace("observacoes", "observacao")
                if table_name != args.table_name:  # We don't want this file
                    continue
                progress_bar.desc = f"{year}-{month:02d}/{origin_system}.{table_name}"
                fobj = inner_zf.open(inner_fileinfo.filename)
                reader = read_csv(fobj, origin_system, year, month)
                for row in reader:
                    writer.writerow(row)
                    progress_bar.update()
    progress_bar.close()
