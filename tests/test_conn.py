# -*- coding:utf-8 -*-
"""

__author__ = "zhangyu"

"""
from unittest import TestCase

from hbasepy import Connection
from tests.config import ip


class TestConnection(TestCase):
    def test_init_conn(self):
        connection = Connection(ip)
        connection.tables()
        connection.close()
        with self.assertRaises(AttributeError):
            connection.tables()

    def test_open_conn(self):
        connection = Connection(ip, auto_connect=False)
        with self.assertRaises(AttributeError):
            connection.tables()
        connection.open()
        connection.tables()
        connection.close()

        with self.assertRaises(AttributeError):
            connection.tables()
