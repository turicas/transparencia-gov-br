from collections import OrderedDict

import rows

from transparenciagovbr import fields, settings


def schema_path_from_filename(filename):
    return str((settings.REPOSITORY_PATH / "schema" / filename).absolute())


def load_schema(filename):
    schema_path = schema_path_from_filename(filename)
    table = rows.import_from_csv(schema_path)
    table.field_names
    # Our internal context will be all available rows.fields + our custom
    # fields
    rows_context = {
        field_name.replace("Field", "").lower(): getattr(rows.fields, field_name)
        for field_name in rows.fields.__all__
        if "Field" in field_name and field_name != "Field"
    }
    custom_context = {}
    for type_name in dir(fields):
        FieldClass = getattr(fields, type_name)
        if "Field" in type_name and FieldClass.__module__ != "rows.fields":
            custom_context[FieldClass.name] = FieldClass
    context = {**rows_context, **custom_context}
    return OrderedDict(
        [(row.field_name, context[row.internal_field_type]) for row in table]
    )


def field_mapping_from_csv(csvfile):
    schema_path = schema_path_from_filename(csvfile)
    return {
        row.original_name: row.field_name for row in rows.import_from_csv(schema_path)
    }
