class ParseException(Exception):
    def __init__(self, message):
        self.message = message


class ConvertException(Exception):
    def __init__(self, message):
        self.message = message


class UnIdentifiedDateTimeException(Exception):
    def __init__(self, message):
        self.message = message
