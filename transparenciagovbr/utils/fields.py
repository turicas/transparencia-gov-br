from collections import OrderedDict

import rows

from transparenciagovbr import fields, settings


EM_SIGILO_STRINGS = (
    "Detalhamento das informações bloqueado.",
    "Informações protegidas por sigilo, nos termos da legislação, para garantia da segurança da sociedade e do Estado",
)


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


class Schema:

    def __init__(self, schema_filename):
        schema = load_schema(schema_filename)
        field_mapping = field_mapping_from_csv(schema_filename)

        self.fields = []
        for original_field_name, field_name in field_mapping.items():
            if field_name == "em_sigilo":
                deserialize = lambda value: "f"
            else:
                deserialize = schema[field_name].deserialize
            self.fields.append((field_name, original_field_name, deserialize))

    def deserialize(self, row):
        new = {
            field_name: deserialize(row.pop(original_field_name, None))
            for field_name, original_field_name, deserialize in self.fields
        }
        if row:
            raise ValueError(f"Missing fields during deserialization: {', '.join(row.keys())}")
        for key, value in new.items():
            if value in EM_SIGILO_STRINGS:
                new[key] = None
                new["em_sigilo"] = "t"
        return new
