import sys
import re
import logging

from docopt import docopt

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
    bottom_border = '-'
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


class Key(object):
    def __init__(self, sql):
        self.sql = sql
        self.name = None

    @staticmethod
    def parse(sql):
        key = Key(sql)

        if sql.startswith('PRIMARY KEY'):
            pass

        return key


class Table(object):
    def __init__(self, sql, name=None):
        self.sql = sql
        self.name = name
        self.columns = {}
        self.keys = {}

    @property
    def names(self):
        return set(self.columns.keys())

    def __getitem__(self, key):
        return self.columns[key]

    def __setitem__(self, key, value):
        self.columns[key] = value

    def add_column(self, col):
        col = Column.parse(col)
        self.columns[col.name] = col

    def add_key(self, key):
        key = Key.parse(key)
        self.keys[key.name] = key

    @staticmethod
    def parse(sql):
        table = Table(sql)
        lines = sql.split('\n')
        for line in lines:
            line = line.strip(' ,')

            m = PAT_TABLE_NAME.match(line)
            if m:
                table.name = m.group(1)
                continue

            elif line.startswith('`'):
                try:
                    table.add_column(line)
                except ValueError as e:
                    LOGGER.exception(e)
                    raise
                continue

            elif 'KEY' in line:
                try:
                    table.add_key(line)
                except ValueError as e:
                    LOGGER.exception(e)
                    raise
                continue

            elif line.startswith(')'):
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

    def diff(self, destination, keys=False, constraints=False, collation=False):
        return Differences(self, destination, keys, constraints, collation)

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


class Differences(object):
    def __init__(self, source, destination, keys, constraints, collation):
        self.src = source
        self.dst = destination
        self.keys = keys
        self.constraints = constraints
        self.collation = collation

    def sql_drop_tables(self):
        src_tables = self.src.names
        dst_tables = self.dst.names

        for m_dst in dst_tables.difference(src_tables):
            print('DROP TABLE `%s`;' % m_dst)

    def sql_alter_tables(self):
        src_tables = self.src.names
        dst_tables = self.dst.names

        for both in src_tables.intersection(dst_tables):
            src_table = self.src[both]
            dst_table = self.dst[both]

            for col in src_table.names.union(dst_table.names):
                if col in src_table.names and col not in dst_table.names:
                    print('ALTER TABLE `%s` ADD COLUMN %s;' % (
                        dst_table.name, src_table.columns[col].sql))
                elif col in dst_table.names and col not in src_table.names:
                    print('ALTER TABLE `%s` DROP COLUMN `%s`;' % (dst_table.name, col))
                elif src_table[col] != dst_table[col]:
                    # print('/* %s */' % dst_table.columns[col].sql)
                    print('ALTER TABLE `%s` MODIFY COLUMN %s;' % (
                        dst_table.name, src_table.columns[col].sql))

    def print_columns(self, pt, src_table, dst_table):
        if src_table.columns == dst_table.columns:
            return

        src_cols = src_table.names
        dst_cols = dst_table.names

        pt.writerow([format_table(src_table.name), format_table(dst_table.name)])

        for col in src_cols.union(dst_cols):
            if col in src_cols and col not in dst_cols:
                pt.writerow([src_table.columns[col].sql, ''])
            elif col in dst_cols and col not in src_cols:
                pt.writerow(['', dst_table.columns[col].sql])
            else:
                src_col = src_table.columns[col]
                dst_col = dst_table.columns[col]

                if src_col != dst_col:
                    pt.writerow([src_col.sql, dst_col.sql])

    def print_tables(self):
        src_tables = self.src.names
        dst_tables = self.dst.names

        with writer(sys.stdout, dialect=TableDialect) as pt:
            pt.writeheader([
                '%s: %s (%s)' % (self.src.name, self.src.db,
                                 self.src.version),
                '%s: %s (%s)' % (self.dst.name, self.dst.db,
                                 self.dst.version),
            ])

            for table in src_tables.union(dst_tables):
                if table in src_tables and table not in dst_tables:
                    pt.writerow(['', format_table(table)])
                elif table in dst_tables and table not in src_tables:
                    pt.writerow([format_table(table), ''])
                else:
                    self.print_columns(pt, self.src[table], self.dst[table])


def main(opts):
    """
    SQLdiff

    Usage:
        sqldiff (<source.sql> <destination.sql>) [--keys] [--constraints]
                [--collation] [--include=<NAME>... | --exclude=<NAME>...]
                [--drop-tables] [--alter-tables]
    
    Options:
        --keys              Compare keys.
        --constraints       Compare constraints.
        --collation         Compare collation.
        --include=<NAME>... Only consider listed tables.
        --exclude=<NAME>... Do not consider listed tables.
        --drop-tables       Generate SQL to drop tables from destination.
        --alter-tables      Generate SQL to alter tables / columns on destination.
    """
    kwargs = {
        'keys': opts['--keys'],
        'constraints': opts['--constraints'],
        'collation': opts['--collation'],
    }
    schemaA = Schema.parse(opts['<source.sql>'])
    schemaB = Schema.parse(opts['<destination.sql>'])
    diff = schemaA.diff(schemaB, **kwargs)

    if not opts['--drop-tables'] and not opts['--alter-tables']:
        diff.print_tables()
        return

    print('/* Generated by sqldiff. */')
    print('/* To be run on %s */' % schemaB.name)

    if opts['--drop-tables']:
        diff.sql_drop_tables()

    if opts['--alter-tables']:
        diff.sql_alter_tables()


if __name__ == '__main__':
    opts = docopt(main.__doc__)
    main(opts)
