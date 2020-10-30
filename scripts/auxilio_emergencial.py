import argparse
import sys

from pathlib import Path

import rows
from tqdm import tqdm

sys.path.insert(0, str(Path(__file__).parent.parent.absolute()))  # noqa
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
    # TODO: move this `main` to a general command-line interface so we can run
    # any extractor by command-line.

    BASE_PATH = Path(__file__).parent
    DATA_PATH = BASE_PATH / "data"
    DOWNLOAD_PATH = DATA_PATH / "download"
    OUTPUT_PATH = DATA_PATH / "output"

    parser = argparse.ArgumentParser()
    parser.add_argument("input_filename")
    parser.add_argument("output_filename")
    parser.add_argument("--buffering", default=4 * 1024 * 1024)
    parser.add_argument("--schema-filename", default="auxilio_emergencial.csv")
    args = parser.parse_args()

    schema = Schema(args.schema_filename)
    filename = Path(args.input_filename)
    fobj = rows.utils.open_compressed(args.output_filename, mode="w", buffering=args.buffering)
    writer = rows.utils.CsvLazyDictWriter(fobj)

    data = extract_rows(schema, filename)
    for row in tqdm(data, desc=f"Extracting {filename.name}"):
        writer.writerow(row)
    fobj.close()


if __name__ == "__main__":
    main()
