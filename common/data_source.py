# coding: utf-8
"""Universal user source definitions.
"""
import os
import csv
import logging
import re

from abc import ABCMeta
from abc import abstractmethod
from ast import literal_eval
from collections import namedtuple

from common.utils import (REGEX_PATTERN, NUM_PATTERN, MYSQL_CONN_PATTERN,
                          CSV_FILE_PATTERN, SET_PATTERN, CONN_PATTERN,
                          BOOL_PATTERN, QUOTE_PATTERN)


SINK_PATTERN = re.compile(r"(?P<type>[^:]+)://.+")


def _sink(value):
    sink_type = SINK_PATTERN.match(value).groupdict()['type']

    type_switch = {
        'mysql': _mysql_pattern,
        'csv': _csv_pattern
    }

    result = type_switch[sink_type](value)
    result.update({"sink_type": sink_type})

    return result


def _mysql_pattern(value):
    result = MYSQL_CONN_PATTERN.match(value).groupdict()

    port = result.get('port')
    charset = result.get('charset')

    result.update({'port': int(port) if port else 3306,
                   'charset': charset if charset else 'utf8'})

    return result


def _csv_pattern(value):
    result = CSV_FILE_PATTERN.match(value).groupdict()

    encoding = result.get('encoding')

    result.update({'encoding': encoding if encoding else 'utf-8'})

    return result


_parse_pattern = [
    (REGEX_PATTERN, lambda v: re.compile(
        REGEX_PATTERN.match(v).groupdict()['pattern'])),
    (CONN_PATTERN, lambda v: CONN_PATTERN.match(v).groupdict()),
    (SINK_PATTERN, _sink),
    (SET_PATTERN, lambda v: set([ele.strip() for ele in v.split('|')])),
    (BOOL_PATTERN, lambda v: v.lower()[0] in ['y', 't'])
]


class NamedTupleDictDataMixin(object):
    @classmethod
    def headers(cls):
        return getattr(cls, '_fields')

    def to_dict(self):
        return getattr(self, '_asdict')()


class Data(object):
    """Base class for user importing.
    """

    __metaclass__ = ABCMeta

    def __init__(self):
        self._record_obj = object
        self.column_headers = list()
        self.records = list()
        self.line_count = 0

    @property
    def record_object(self):
        return self._record_obj

    @property
    def columns_count(self):
        """
        Get columns counts.
        :return:
        """
        return len(self.column_headers)

    @property
    def record_count(self):
        """
        Get records count property.
        :return: records count
        """

        return len(self.records)

    @abstractmethod
    def parse_from_data(self, **kwargs):
        """
        Parse user columns read from outside.
        :param kwargs:
        :return:
        """
        pass


class CatalogedMixin(object):
    """Provide a catalog functions for Data's sub class.
    """

    def __init__(self, keynames):
        self._keynames = keynames
        self.cataloged_records = {}
        self._judge_key_name()
        self._make_cataloged_records()

    def _judge_key_name(self):
        for key_name in self._keynames:
            if not hasattr(getattr(self, "_record_obj"), key_name):
                raise ValueError(
                    u'Invalid key name["{}"], '
                    u'available key names: {}'.format(
                        key_name, getattr(self, "_column_headers")))

    def _make_cataloged_records(self):
        for record in getattr(self, "records"):
            self._build_catalog(record, self._keynames, self.cataloged_records)

    def _build_catalog(self, record, keynames, catalog):
        key_name = keynames[0]
        key_value = getattr(record, key_name)
        if len(keynames) > 1:
            if key_value not in catalog:
                catalog[key_value] = {}
            self._build_catalog(record, keynames[1:], catalog[key_value])
        else:
            if key_value not in catalog:
                catalog[key_value] = record
            elif isinstance(catalog[key_value], list):
                catalog[key_value].append(record)
            else:
                catalog[key_value] = [catalog[key_value], record]

    def __getitem__(self, item_name, default=None):
        return self.cataloged_records.get(item_name, default)


class CSVData(Data):
    """CSV user class.
    """

    def __init__(self, filename, ignore_invalid=False, splitter=',',
                 nan_columns=None, filter_func=None):
        Data.__init__(self)
        self._nan_columns = nan_columns
        self._file_path = filename
        self._ignore_invalid = ignore_invalid
        self._splitter = splitter
        self._filter_func = filter_func
        self.invalid_lines = []
        self.filtered_lines = []
        self.parse_from_data()

    @property
    def invalid_count(self):
        """
        Invalid records count property.
        :return:
        """

        return len(self.invalid_lines)

    @property
    def filtered_count(self):
        """
        Filtered records count property.
        :return:
        """

        return len(self.filtered_lines)

    @property
    def file_path(self):
        """
        Source file path property.
        :return:
        """

        return self._file_path

    @staticmethod
    def _parse_column(cols, col_defines, nan_columns=None):
        if col_defines:
            for idx, func in col_defines:
                cols[idx] = func(cols[idx])

            return

        col_length = len(cols)

        # 处理nan_columns中的负值index
        if nan_columns and nan_columns != '*':
            nan_columns = map(
                lambda x: x if x >= 0 else col_length + x,
                nan_columns)

        for idx in range(col_length):
            parsed = False

            value_data = cols[idx]

            for pattern, pattern_func in _parse_pattern:
                if pattern.match(value_data):
                    cols[idx] = pattern_func(value_data)

                    parsed = True

                    col_defines.append((idx, pattern_func))

                    break

            if not parsed:
                if nan_columns and (nan_columns == '*' or
                                    idx in nan_columns):
                    continue

                if NUM_PATTERN.match(value_data) or QUOTE_PATTERN.match(
                        value_data):
                    cols[idx] = literal_eval(value_data)
                    col_defines.append((idx, literal_eval))

    @staticmethod
    def read(filename, splitter=',', has_head=True, skip_head=False,
             nan_columns=None):
        """
        Read user from csv file.
        :param filename: csv file path
        :param splitter: csv user separator
        :param has_head: whether csv file has a header
        :param skip_head: whether to read header
        :param nan_columns: column indexes for not a number column
        :return:
        """

        def _decode(values):
            for idx in range(len(values)):
                try:
                    values[idx] = values[idx].decode('utf-8')
                except UnicodeDecodeError:
                    values[idx] = values[idx].decode('gbk')
            return values

        col_defines = []
        with open(filename) as file_stream:
            csv_file = csv.reader(file_stream, delimiter=splitter)
            if has_head:
                headers = next(csv_file)
                if not skip_head:
                    if headers:
                        yield headers
                    else:
                        raise ValueError(
                            u'No column header in CSV[{}] file.'.format(
                                filename))
            for data in csv_file:
                if data:
                    CSVData._parse_column(
                        _decode(data), col_defines, nan_columns)
                yield data

    def parse_from_data(self, **kwargs):
        """
        Parse user from imported records.
        :param kwargs:
        :return:
        """

        for line_data in CSVData.read(self._file_path, self._splitter,
                                      nan_columns=self._nan_columns):
            self.line_count += 1

            if self.line_count == 1:
                self.column_headers = line_data

                file_name = u"".join(
                    map(lambda v: v.capitalize(),
                        os.path.basename(self._file_path).split('.')[0:-1]))

                self._record_obj = type(
                    str(file_name),
                    (namedtuple(file_name + '_records',
                                self.column_headers),
                     NamedTupleDictDataMixin),
                    dict())

                continue

            if len(line_data) != self.columns_count:
                if not self._ignore_invalid:
                    raise ValueError(
                        u'Invalid user record in line[{no}], '
                        u'column count mismatch: '
                        u'header[{hd_count}], user[{dt_count}]'.format(
                            no=self.line_count, hd_count=self.columns_count,
                            dt_count=len(line_data)))

                logging.warning(
                    u'Invalid user record in line[%d], '
                    u'skipped by configure.', self.line_count)
                self.invalid_lines.append(self.line_count)

            rec = self._record_obj(*line_data)

            if self._filter_func and self._filter_func(rec):
                self.filtered_lines.append(self.line_count)
            else:
                self.records.append(rec)

        logging.debug(u'Total lines[%d] imported, '
                      u'records[%d] imported with '
                      u'[%d] invalid records in lines: %s, and '
                      u'[%d] filtered records in lines: %s.',
                      self.line_count, self.record_count,
                      self.invalid_count, self.invalid_lines,
                      self.filtered_count, self.filtered_lines)


class CatalogedCSVData(CSVData, CatalogedMixin):
    """Cataloged csv user by specified key columns.
    """

    def __init__(self, filename, ignore_invalid=False, splitter=',',
                 nan_columns=None, filter_func=None, **kwargs):
        CSVData.__init__(
            self, filename=filename, ignore_invalid=ignore_invalid,
            filter_func=filter_func, splitter=splitter,
            nan_columns=nan_columns)
        CatalogedMixin.__init__(self, **kwargs)


class MySqlData(Data):
    """Data class for mysql server.
    """
    import pymysql

    _statement_filter = ('update', 'insert', 'delete', 'truncate')

    _statement_pattern = re.compile(
        r'^ *(?:(?:delete|select).*from|insert +into|update|truncate) +'
        r'`?(?P<table_name>[a-zA-Z0-9_$]+)`? *.*;?',
        re.IGNORECASE)

    def __init__(self, mysql_config, sql, *sql_params):
        if not mysql_config:
            raise ValueError(u'Invalid mysql connection config.')

        Data.__init__(self)

        self._mysql_config = {
            'port': 3306,
            'charset': 'utf8',
            'cursorclass': MySqlData.pymysql.cursors.Cursor
        }

        self._mysql_config.update(mysql_config)
        self._sql = sql
        self._sql_params = sql_params
        self._table_name = u''
        self._conn = None
        self._cursor = None
        self.parse_from_data(mysql_config=self._mysql_config,
                             sql=sql, sql_params=sql_params)

    @property
    def sql_statement(self):
        """
        Get full sql statement property.
        :return:
        """

        return self._sql % self._sql_params

    @property
    def data_source(self):
        """
        Get user source definition.
        :return:
        """

        return (u'mysql://{user}@{host}:{port}/'
                u'{db}.{table_name}?charset={charset}').format(
                user=self._mysql_config['user'],
                host=self._mysql_config['host'],
                port=self._mysql_config['port'],
                db=self._mysql_config['db'],
                table_name=self._table_name,
                charset=self._mysql_config['charset'])

    @staticmethod
    def read(mysql_config, sql, *sql_params, **kwargs):
        """
        Retrieve user records from mysql server.
        :param mysql_config: mysql connection configs
        :param sql: sql statement
        :param sql_params: sql value variable list
        :param kwargs: other variables
        :return:
        """

        for statement in MySqlData._statement_filter:
            if statement in sql.lower():
                raise ValueError(
                    u'Invalid sql statement, only SELECT action allowed.')

        try:
            conn = MySqlData.pymysql.connect(**mysql_config)
        except MySqlData.pymysql.err.Error as err:
            raise IOError(err)

        try:
            with conn.cursor() as cursor:
                cursor.execute(sql, sql_params)
                if not kwargs.get('skip_head'):
                    # noinspection PyUnresolvedReferences
                    yield zip(*cursor.description)[0]
                for row in cursor.fetchall():
                    yield row
        except MySqlData.pymysql.err.Error as err:
            raise ValueError(err)
        finally:
            conn.close()

    def parse_from_data(self, **kwargs):
        """
        Parse user retrieved from mysql server.
        :param kwargs:
        :return:
        """

        if 'mysql_config' not in kwargs:
            raise ValueError('No mysql config specified in parameters.')

        if 'sql' not in kwargs:
            raise ValueError('No sql statement specified in parameters.')

        mysql_config = kwargs.get('mysql_config')
        sql = kwargs.get('sql')
        sql_params = kwargs.get('sql_params')

        self._table_name = MySqlData._statement_pattern.match(
            sql % sql_params).groupdict()['table_name']

        for line_data in MySqlData.read(mysql_config, sql, *sql_params):
            self.line_count += 1

            if self.line_count == 1:
                self.column_headers = line_data

                db_name = u"{db}_{table}".format(
                    db=mysql_config['db'], table=self._table_name)

                self._record_obj = type(
                    str(db_name),
                    (namedtuple(db_name + '_records',
                                self.column_headers),
                     NamedTupleDictDataMixin),
                    dict())
            else:
                self.line_count += 1
                self.records.append(self._record_obj(*line_data))

        logging.debug(
            u'Total [%d] records retrieved from %s: %s.%s',
            self.record_count, mysql_config['host'],
            mysql_config['db'], self._table_name)

    def __enter__(self):
        conn = MySqlData.pymysql.connect(**self._mysql_config)
        self._cursor = conn.cursor()
        return self._cursor

    def __exit__(self, *exc_info):
        if self._cursor:
            self._cursor.commit()
            self._cursor.connection.close()


class CatalogedMySqlData(MySqlData, CatalogedMixin):
    """Cataloged mysql user by specified key columns.
    """

    def __init__(self, mysql_config, sql, *sql_params, **kwargs):
        MySqlData.__init__(self, mysql_config, sql, *sql_params)
        CatalogedMixin.__init__(self, **kwargs)
