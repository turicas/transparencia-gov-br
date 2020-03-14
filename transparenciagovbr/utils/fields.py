import rows

from transparenciagovbr import settings
from transparenciagovbr.fields import BrazilianDateField, MoneyRealField


def schema_path_from_filename(filename):
    return str((settings.REPOSITORY_PATH / "schema" / filename).absolute())


def load_schema(filename):
    schema_path = schema_path_from_filename(filename)
    return rows.utils.load_schema(
        schema_path,
        context={
            "date": BrazilianDateField,
            "text": rows.fields.TextField,
            "integer": rows.fields.IntegerField,
            "money_real": MoneyRealField,
        },
    )


def field_mapping_from_csv(csvfile):
    schema_path = schema_path_from_filename(csvfile)
    return {
        row.original_name: row.field_name for row in rows.import_from_csv(schema_path)
    }
