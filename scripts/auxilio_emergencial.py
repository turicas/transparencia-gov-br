import argparse

from pathlib import Path

import rows
from tqdm import tqdm

from transparenciagovbr.utils.cities import city_name_by_id
from transparenciagovbr.utils.fields import Schema
from transparenciagovbr.utils.io import parse_zip


def extract_rows(schema, filename):
    data = parse_zip(
        filename_or_fobj=filename,
        inner_filename_suffix="_AuxilioEmergencial.csv",
        encoding="iso-8859-1",
    )
    for row in data:
        new = schema.deserialize(row)
        if new is not None:
            if new["codigo_ibge_municipio"] is not None:
                # Força nome de município a ser mais bonito (com acentos,
                # maiúsculas e minúsculas). :)
                new["municipio"] = city_name_by_id[new["codigo_ibge_municipio"]]
            yield new


def main():
    BASE_PATH = Path(__file__).parent
    DATA_PATH = BASE_PATH / "data"
    DOWNLOAD_PATH = DATA_PATH / "download"
    OUTPUT_PATH = DATA_PATH / "output"

    parser = argparse.ArgumentParser()
    parser.add_argument("input_filename")
    parser.add_argument("output_filename")
    args = parser.parse_args()

    schema_filename = "auxilio_emergencial.csv"
    schema = Schema(schema_filename)

    writer = rows.utils.CsvLazyDictWriter(args.output_filename)
    for row in tqdm(extract_rows(schema, args.input_filename)):
        writer.writerow(row)


if __name__ == "__main__":
    main()
