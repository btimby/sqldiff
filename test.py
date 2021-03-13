import unittest
from parameterized import parameterized

from sqldiff import (
    Schema, Table, Column,
)


COLS = [
    (
        'foo',
        '`foo` int(11) NOT NULL AUTO_INCREMENT',
        'int', 11, None, False, True,
    ),
    (
        'remote_addr_three',
        '`remote_addr_three` smallint(5) unsigned DEFAULT NULL',
        'smallint', 5, None, True, False,
    ),
    (
        'score',
        '`score` decimal(5,2) DEFAULT NULL',
        'decimal', 5, 2, True, False,
    ),
]


class SchemaTestCase(unittest.TestCase):
    pass


class TableTestCase(unittest.TestCase):
    pass


class ColumnTestCase(unittest.TestCase):
    @parameterized.expand(COLS)
    def test_parse(self, name, sql, type, length, precision, nullable, auto):
        col = Column.parse(sql)
        self.assertEqual(col.name, name)
        self.assertEqual(col.type, type)
        self.assertEqual(col.length, length)
        self.assertEqual(col.precision, precision)
        self.assertEqual(col.nullable, nullable, 'Nullable should be %s' % nullable)
        self.assertEqual(col.auto, auto, 'auto increment should be %s' % auto)


if __name__ == '__main__':
    unittest.main()
