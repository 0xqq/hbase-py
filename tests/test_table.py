# -*- coding:utf-8 -*-
"""

__author__ = "zhangyu"

"""

from unittest import TestCase

import time

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
        res = self.connection.table("tables1").families()

        for family, descriptor in res.items():
            if family == "cf1:":
                self.assertEqual(descriptor.name, "cf1:")
                self.assertEqual(descriptor.maxVersions, 10)
            elif family == "cf2:":
                self.assertEqual(descriptor.name, "cf2:")
                self.assertEqual(descriptor.maxVersions, 2)

    def test_get_table_regions(self):
        self.connection.create_table("tables1", column_families=[
            column_descriptor("cf1", max_versions=10),
            column_descriptor("cf2:", max_versions=2)])
        self.connection.table("tables1").regions()

    def test_row_with_timestamp(self):
        now = int(time.time())
        self.connection.create_table("tables1", column_families=[
            column_descriptor("cf1", max_versions=10),
            column_descriptor("cf2:", max_versions=2)])

        resp = self.connection.table("tables1").row("cf1:", [], None, now)
        print(resp)

    def tearDown(self):
        self.connection.close()
