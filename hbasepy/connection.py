# -*- coding:utf-8 -*-
"""

__author__ = "zhangyu"

"""
import logging

from thriftpy.protocol import TBinaryProtocol
from thriftpy.thrift import TClient
from thriftpy.transport import TSocket, TBufferedTransport
from hbasepy.load import hbase_thrift
from hbasepy.tables import Table

logger = logging.getLogger(__name__)


class Connection:
    def __init__(self, host="127.0.0.1",
                 port=9090, timeout=5000, auto_connect=True, table_prefix="",
                 compat="0.98", transport_class=None, protocol_class=None):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.table_prefix = table_prefix  # not used
        self.compat = compat
        self.transport_class = transport_class or TBufferedTransport
        self.protocol_class = protocol_class or TBinaryProtocol

        self.transport = None
        self.protocol = None
        self.client = None

        if auto_connect:
            self.open()

    def open(self):
        if self.transport and self.transport.is_open():
            logger.debug("transport was opened.")
            return
        logger.debug("new transport...")
        socket = TSocket(host=self.host, port=self.port, socket_timeout=self.timeout)
        self.transport = self.transport_class(socket)
        self.protocol = self.protocol_class(self.transport)
        self.client = TClient(hbase_thrift.Hbase, self.protocol)

        self.transport.open()

    def close(self):
        if self.transport and self.transport.is_open():
            logger.debug("transport is closing.")
            self.transport.close()
            self.transport = None
            self.protocol = None
            self.client = None

    # ############## todo: 分割出下面方法
    def table(self, name):
        return Table(name, self)

    def tables(self):
        """
        List all the userspace tables.

        ['emp', 'project1_emp']
        :rtype: list
        :return:
        """
        return self.client.getTableNames()

    def create_table(self, table_name, column_families):
        """
        http://hbase.apache.org/0.94/book/columnfamily.html

        create table

        :type table_name: str
        :type column_families: list

        :param table_name:
        :param column_families:

        :return: None
        :rtype: None

        """

        return self.client.createTable(table_name, column_families)

    def delete_table(self, name, disable=False):
        """
        delete table
        :type name: str
        :type disable: bool

        :param name:
        :param disable:
        :return: None
        :rtype None
        """
        if disable and self.is_table_enabled(name):
            self.disable_table(name)
        self.client.deleteTable(name)

    def enable_table(self, name):
        """
        Brings a table on-line (enables it)
        :type name: str
        :param name:
        :return:
        """
        if not self.is_table_enabled(name):
            self.client.enableTable(name)

    def disable_table(self, name):
        """
        Disables a table (takes it off-line) If it is being served, the master
        will tell the servers to stop serving it.
        :type name: str
        :param name:
        :return:
        """
        if self.is_table_enabled(name):
            return self.client.disableTable(name)

    def is_table_enabled(self, name):
        """
        name of the table to check
        :type name: str
        :param name:
        :return:
        """
        return self.client.isTableEnabled(name)

    def compact_table(self, table_name_or_region_name, major=False):
        """
        Compact the specified table

        :type table_name_or_region_name: str
        :param table_name_or_region_name:
        :param major:
        :return:
        """
        if major:
            return self.client.majorCompact(table_name_or_region_name)
        return self.client.compact(table_name_or_region_name)

