from collections import OrderedDict

import rows

from transparenciagovbr import settings
from transparenciagovbr.fields import BrazilianDateField, CustomIntegerField, MoneyRealField


def schema_path_from_filename(filename):
    return str((settings.REPOSITORY_PATH / "schema" / filename).absolute())


def load_schema(filename):
    schema_path = schema_path_from_filename(filename)
    table = rows.import_from_csv(schema_path)
    table.field_names
    context = {
        "date": BrazilianDateField,
        "text": rows.fields.TextField,
        "custom_integer": CustomIntegerField,
        "money_real": MoneyRealField,
    }
    return OrderedDict(
        [(row.field_name, context[row.internal_field_type]) for row in table]
    )


def field_mapping_from_csv(csvfile):
    schema_path = schema_path_from_filename(csvfile)
    return {
        row.original_name: row.field_name for row in rows.import_from_csv(schema_path)
    }
