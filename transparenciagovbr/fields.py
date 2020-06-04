import rows


class BrazilianDateField(rows.fields.DateField):
    INPUT_FORMAT = "%d/%m/%Y"


class MoneyRealField(rows.fields.DecimalField):
    @classmethod
    def deserialize(cls, value):
        """
        >>> MoneyRealField.deserialize("89188,11")
        '89188.11'
        """
        value = value.replace(",", ".")
        return super().deserialize(value)


class CustomIntegerField(rows.fields.IntegerField):
    """Locale-aware field class to represent integer. Accepts numbers starting with 0"""

    @classmethod
    def deserialize(cls, value, *args, **kwargs):
        if value is None or isinstance(value, cls.TYPE):
            return value
        value = rows.fields.as_string(value).strip()
        while value.startswith("0"):
            value = value[1:]
        return super().deserialize(value)
