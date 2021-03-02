from rows.fields import (
    BoolField,
    DateField,
    DecimalField,
    IntegerField,
    TextField,
    as_string,
    is_null,
)


class BrazilianBoolField(BoolField):
    name = "brazilian_bool"
    TRUE_VALUES = ("SIM", "sim", "Sim")
    FALSE_VALUES = ("NÃO", "NAO", "Não", "Nao", "não", "nao")


class BrazilianDateField(DateField):
    name = "brazilian_date"
    INPUT_FORMAT = "%d/%m/%Y"


class CPFField(TextField):
    """TextField to clean-up unneeded chars in CPF"""

    name = "cpf"

    @classmethod
    def deserialize(cls, value, *args, **kwargs):
        if is_null(value):
            return None

        value = as_string(value).strip()
        value = value.replace(".", "").replace("-", "")
        assert len(value) == 11
        return value


class CustomIntegerField(IntegerField):
    """Locale-aware field class to represent integer

    Accepts numbers starting with 0 and removes unnecessary characters.
    """

    name = "custom_integer"

    @classmethod
    def deserialize(cls, value, *args, **kwargs):
        if is_null(value):
            return None
        elif isinstance(value, cls.TYPE):
            return value

        value = as_string(value).strip()
        value = value.replace("ª", "")
        while value.startswith("0"):
            value = value[1:]
        return super().deserialize(value)


class CustomTextField(TextField):
    """TextField to clean-up a value that should be empty"""

    name = "custom_text"

    @classmethod
    def deserialize(cls, value, *args, **kwargs):
        if is_null(value) or value in ("Não há", "Não se aplica"):
            return None

        return value


class MoneyRealField(DecimalField):
    name = "money_real"

    @classmethod
    def deserialize(cls, value):
        """
        >>> MoneyRealField.deserialize("89188,11")
        '89188.11'
        """
        if is_null(value):
            return None
        elif isinstance(value, cls.TYPE):
            return value

        value = value.replace(",", ".")
        return super().deserialize(value)
