from __future__ import absolute_import
from collections import OrderedDict
from six import string_types
from inspect import isgenerator
import pkg_resources
import os
import six

try:
    from urllib.parse import urlparse
except ImportError:
     from urlparse import urlparse


row_type = OrderedDict


class QueryDict(object):
    """Provide a dict like interface to files in the /sql folder
    """
    def __init__(self, path=None):
        self.queries = None
        self.path = path

    def __getitem__(self, query_name):

        if self.path:
            filename = os.path.join(self.path, query_name+".sql")
            if os.path.exists(filename):
                with open(filename, 'r') as f:
                    return six.text_type(f.read())

        elif pkg_resources.resource_exists(__name__,
                                           os.path.join("sql",
                                                        query_name+'.sql')):
            return pkg_resources.resource_string(
                __name__,
                os.path.join("sql", query_name+'.sql')).decode('utf-8')

        else:
            raise ValueError("Invalid query name: %r" % query_name)


class DatasetException(Exception):
    pass


def normalize_column_name(name):
    if not isinstance(name, string_types):
        raise ValueError('%r is not a valid column name.' % name)
    name = name.lower().strip()
    if not len(name) or '.' in name or '-' in name:
        raise ValueError('%r is not a valid column name.' % name)
    return name


def convert_row(row_type, row):
    if row is None:
        return None
    return row_type(list(row.items()))


class ResultIter(object):
    """
    SQLAlchemy ResultProxies are not iterable to get a list of dictionaries.
    This is to wrap them.
    """
    def __init__(self, result_proxies, row_type=row_type):
        self.row_type = row_type
        if not isgenerator(result_proxies):
            result_proxies = iter((result_proxies, ))
        self.result_proxies = result_proxies
        self._iter = None

    def _next_rp(self):
        try:
            rp = next(self.result_proxies)
            self.keys = list(rp.keys())
            self._iter = iter(rp.fetchall())
            return True
        except StopIteration:
            return False

    def __next__(self):
        if self._iter is None:
            if not self._next_rp():
                raise StopIteration
        try:
            return convert_row(self.row_type, next(self._iter))
        except StopIteration:
            self._iter = None
            return self.__next__()

    next = __next__

    def __iter__(self):
        return self
