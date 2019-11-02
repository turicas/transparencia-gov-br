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
