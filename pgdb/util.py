from collections import OrderedDict
from six import string_types

row_type = OrderedDict


class DatasetException(Exception):
    pass


def normalize_column_name(name):
    if not isinstance(name, string_types):
        raise ValueError('%r is not a valid column name.' % name)
    name = name.lower().strip()
    if not len(name) or '.' in name or '-' in name:
        raise ValueError('%r is not a valid column name.' % name)
    return name
