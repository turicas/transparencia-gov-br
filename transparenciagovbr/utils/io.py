from io import TextIOWrapper


class NotNullTextWrapper(TextIOWrapper):
    def read(self, *args, **kwargs):
        data = super().read(*args, **kwargs)
        return data.replace('\x00', '')

    def readline(self, *args, **kwargs):
        data = super().readline(*args, **kwargs)
        return data.replace('\x00', '')
