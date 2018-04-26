# -*- coding:utf-8 -*-
"""

__author__ = "zhangyu"

"""

from unittest import TestCase

from hbasepy import Connection, column_descriptor
from hbasepy.load import hbase_thrift
from tests.config import ip


class TestTable(TestCase):
    def setUp(self):
        self.connection = Connection(ip)
        self.flush()

    def flush(self):
        tables = self.connection.tables()
        for table in tables:
            self.assertIsInstance(table, str)
            self.connection.delete_table(table, True)
        tables = self.connection.tables()
        self.assertEqual(tables, [])

    def test_create_table(self):
        self.connection.create_table("tables1", column_families=[column_descriptor("cf1")])

        with self.assertRaises(hbase_thrift.AlreadyExists):
            self.connection.create_table("tables1", column_families=[column_descriptor("cf1")])

    def test_delete_table(self):
        with self.assertRaises(hbase_thrift.IOError):
            self.connection.delete_table("tables1")

        self.connection.create_table("tables1", column_families=[column_descriptor("cf1")])
        b = self.connection.is_table_enabled("tables1")
        self.assertTrue(b)
        with self.assertRaises(hbase_thrift.IOError):
            # TableNotDisabledException
            self.connection.delete_table("tables1")
        self.connection.delete_table("tables1", True)
        with self.assertRaises(hbase_thrift.IOError):
            # TableNotFoundException
            self.connection.delete_table("tables1", True)

    def test_get_column_descriptors(self):
        self.connection.create_table("tables1", column_families=[
            column_descriptor("cf1", max_versions=10),
            column_descriptor("cf2:", max_versions=2)])
        res = self.connection.get_column_descriptors("tables1")

    def test_get_table_regions(self):
        self.connection.create_table("tables1", column_families=[
            column_descriptor("cf1", max_versions=10),
            column_descriptor("cf2:", max_versions=2)])
        res = self.connection.get_table_regions("tables1")
        print(res)

    def tearDown(self):
        self.connection.close()
