import sys
import re
import logging

from texttables import Dialect
from texttables.dynamic import writer


LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.StreamHandler())

PAT_SCHEMA_DB = re.compile(r'^-- .*Database: (\w+)$', flags=re.MULTILINE)
PAT_SCHEMA_VERSION = re.compile(r'-- Server version\W+(.+)$', flags=re.MULTILINE)
PAT_TABLE_SPLIT = re.compile(r'\n\n', flags=re.MULTILINE)
PAT_TABLE_NAME = re.compile(r'^CREATE TABLE `(\w+)` \($')


def format_table(name):
    return '---- %s ----' % name.upper()


class TableDialect(Dialect):
    header_delimiter = '='
    row_delimiter = '-'
    top_border = '='
    bottom_border = '_'
    left_border = '|'
    cell_delimiter = '|'
    right_border = '|'
    corner_border = '+'


class Column(object):
    def __init__(self, sql, name=None):
        self.sql = sql
        self.name = name
        self.type = None
        self.length = None
        self.precision = None
        self.auto = False
        self.nullable = None

    def __eq__(self, right):
        self_length, right_length = self.length, right.length
        if self.type == 'datetime':
            self_length = right_length = None

        return self.name == right.name and \
               self.type == right.type and \
               self_length == right_length and \
               self.precision == right.precision and \
               self.auto == right.auto and \
               self.nullable == right.nullable

    def diff(self, pt, column):
        if self != column:
            pt.writerow([self.sql, column.sql])

    @staticmethod
    def parse(sql):
        col = Column(sql)

        col.nullable = 'NOT NULL' not in sql

        parts = sql.strip(' ,').split()
        col.name = parts[0].strip('`')

        col.type = parts[1]
        if '(' in col.type:
            col.type, length = col.type.strip(')').split('(')
            if ',' in length:
                length, precision = length.split(',')
                col.precision = int(precision)
            col.length = int(length)

        for part in parts[2:]:
            if part == 'AUTO_INCREMENT':
                col.auto = True

        return col


class Table(object):
    def __init__(self, sql, name=None):
        self.sql = sql
        self.name = name
        self.columns = {}

    @property
    def names(self):
        return set(self.columns.keys())

    def __getitem__(self, key):
        return self.columns[key]

    def __setitem__(self, key, value):
        self.columns[key] = value

    def add(self, col):
        col = Column.parse(col)
        self.columns[col.name] = col

    def diff(self, pt, table):
        if self.columns == table.columns:
            return

        left_cols = self.names
        right_cols = table.names

        pt.writerow([format_table(self.name), format_table(table.name)])
        for m_left in left_cols.difference(right_cols):
            pt.writerow([m_left, ''])
        for m_right in right_cols.difference(left_cols):
            pt.writerow(['', m_right])

        for both in left_cols.intersection(right_cols):
            self[both].diff(pt, table[both])

    @staticmethod
    def parse(sql):
        table = Table(sql)
        lines = sql.split('\n')
        for line in lines:
            line = line.strip()

            m = PAT_TABLE_NAME.match(line)
            if m:
                table.name = m.group(1)
                continue

            if line.startswith('`'):
                try:
                    table.add(line)
                except ValueError as e:
                    LOGGER.exception(e)
                    raise
                continue

            if line.startswith(')'):
                break

        if not table.name:
            raise ValueError('Invalid table definition')

        return table


class Schema(object):
    def __init__(self, sql, name=None):
        self.sql = sql
        self.name = name
        self.db = None
        self.version = None
        self.tables = {}

    @property
    def names(self):
        return set(self.tables.keys())

    def __getitem__(self, key):
        return self.tables[key]

    def __setitem__(self, key, value):
        self.tables[key] = value

    def add(self, sql):
        table = Table.parse(sql)
        self.tables[table.name] = table

    def diff(self, schema):
        left_tables = self.names
        right_tables = schema.names

        with writer(sys.stdout, dialect=TableDialect) as pt:
            pt.writeheader([
                '%s: %s (%s)' % (self.name, self.db, self.version),
                '%s: %s (%s)' % (schema.name, schema.db, schema.version),
            ])

            for m_left in left_tables.difference(right_tables):
                pt.writerow([format_table(m_left), ''])
            for m_right in right_tables.difference(left_tables):
                pt.writerow(['', format_table(m_right)])

            for both in left_tables.intersection(right_tables):
                self[both].diff(pt, schema[both])

    @staticmethod
    def parse(sql_path):
        with open(sql_path, 'r') as f:
            sql = f.read()
            schema = Schema(sql, name=sql_path)

            m = PAT_SCHEMA_DB.search(sql)
            if m:
                schema.db = m.group(1)
            m = PAT_SCHEMA_VERSION.search(sql)
            if m:
                schema.version = m.group(1)

            for m in PAT_TABLE_SPLIT.split(sql):
                try:
                    schema.add(m)
                except ValueError as e:
                    continue

            return schema


def main(args):
    schemaA = Schema.parse(args[0])
    schemaB = Schema.parse(args[1])

    schemaA.diff(schemaB)


if __name__ == '__main__':
    main(sys.argv[1:])
