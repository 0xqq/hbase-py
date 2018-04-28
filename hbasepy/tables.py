# -*- coding:utf-8 -*-
"""

__author__ = "zhangyu"

"""


class Table(object):
    def __init__(self, name, connection):
        """
        :type name: str
        :type connection: Connection
        :param name:
        :param connection:
        """
        self.name = name
        self.connection = connection

    def get_column_descriptors(self, table_name):
        """
        :param table_name:
        :return:
        :rtype: dict


        {'cf1:': ColumnDescriptor(name='cf1:', maxVersions=10, compression='NONE', inMemory=False,
                    bloomFilterType='NONE', bloomFilterVectorSize=0, bloomFilterNbHashes=0,
                    blockCacheEnabled=False, timeToLive=2147483647),
         'cf2:': ColumnDescriptor(name='cf2:', maxVersions=2, compression='NONE', inMemory=False,
                    bloomFilterType='NONE', bloomFilterVectorSize=0, bloomFilterNbHashes=0,
                    blockCacheEnabled=False, timeToLive=2147483647)}
        """
        return self.connection.client.getColumnDescriptors(table_name)

    def families(self):
        return self.get_column_descriptors(self.name)

    def get_table_regions(self, table_name):
        """
        [
        TRegionInfo(startKey='', endKey='', id=1524734540419,
                    name='tables1,,1524734540419.28f9de6fc7e5dd90d3cd06c40c606726.',
                    version=1, serverName='60808495b152', port=46275)
          ]
        :param table_name:
        :return:
        :rtype: list
        """
        return self.connection.client.getTableRegions(table_name)

    def regions(self):
        return self.get_table_regions(self.name)

    def get_row_with_columns(self, table_name, row, columns=None, attributes=None):
        """
            Get the specified columns for the specified table and row at the latest
            timestamp. Returns an empty list if the row does not exist.
        :type table_name: str
        :type row: str
        :type columns: list
        :type attributes: dict

        :param table_name:
        :param row:
        :param columns:
        :param attributes:
        :return:
        """
        return self.connection.client.getRowWithColumns(table_name, row, columns, attributes)

    def get_row_with_columns_with_ts(self, table_name, row, columns, timestamp, attributes):
        """
            Get the specified columns for the specified table and row at the specified
            timestamp. Returns an empty list if the row does not exist.
        :type table_name: str
        :type row: str
        :type timestamp: int
        :type attributes: dict

        :param table_name:
        :param row:
        :param columns:
        :param timestamp:
        :param attributes:
        :return:
        """
        return self.connection.client.getRowWithColumnsTs(table_name, row, columns, timestamp, attributes)

    def row(self, row, columns=None, attributes=None, timestamp=None):
        """
        getRowWithColumns,getRowWithColumnsTs
        :type row: str
        :type columns: list
        :type attributes: dict
        :type timestamp: int
        :type

        :param row:
        :param columns:
        :param attributes:
        :param timestamp:
        :return:
        :rtype: list
        list<TRowResult>
        """
        if attributes is None:
            attributes = {}
        if timestamp is not None:
            return self.get_row_with_columns_with_ts(self.name, row, columns, timestamp, attributes)
        return self.get_row_with_columns(self.name, row, columns, attributes)

    def get_rows_with_columns(self, table_name, rows, columns, attributes):
        """
        :type table_name: str
        :type rows: list
        :type columns: list
        :type attributes: dict
        :param table_name:
        :param rows:
        :param columns:
        :param attributes:
        :return:
        """
        return self.connection.client.getRowsWithColumns(table_name, rows, columns, attributes)

    def get_rows_with_columns_with_ts(self, table_name, rows, columns, timestamp, attributes):
        """
        :type table_name: str
        :type rows: list
        :type columns: list
        :type timestamp: int
        :type attributes: dict
        :param table_name:
        :param rows:
        :param columns:
        :param timestamp:
        :param attributes:
        :return:
        """
        return self.connection.client.getRowsWithColumnsTs(table_name, rows, columns, timestamp, attributes)

    def rows(self, rows, columns, timestamp=None, attributes=None):
        """
        :type rows: list
        :type columns: list
        :type timestamp: int
        :type attributes: dict
        :param rows:
        :param columns:
        :param timestamp:
        :param attributes:
        :return:
        """

        if attributes is None:
            attributes = {}

        if timestamp is None:
            return self.get_rows_with_columns(self.name, rows, columns, attributes)
        return self.get_rows_with_columns_with_ts(self.name, rows, columns, timestamp, attributes)

    def get(self, table_name, row, column, attributes):
        """
        :type table_name: str
        :type row: str
        :type column: str
        :type attributes: dict
        :param table_name:
        :param row:
        :param column:
        :param attributes:
        :return:
        """
        return self.connection.client.get(table_name, row, column, attributes)

    def get_ver(self, table_name, row, column, num_versions, attributes):
        """
        :type table_name: str
        :type row: str
        :type column: str
        :type num_versions: int
        :type attributes: dict
        :param table_name:
        :param row:
        :param column:
        :param num_versions:
        :param attributes:
        :return:
        :rtype:list
        """
        return self.connection.client.getVer(table_name, row, column, num_versions, attributes)

    def get_ver_with_timestamp(self, table_name, row, column, timestamp, num_versions, attributes):
        """
        :type table_name: str
        :type row: str
        :type column: str
        :type timestamp: int
        :type num_versions: int
        :type attributes: dict
        :param table_name:
        :param row:
        :param column:
        :param timestamp:
        :param num_versions:
        :param attributes:
        :return:
        """
        return self.connection.client.getVer(table_name, row, column, timestamp, num_versions, attributes)

    def cells(self, row, column, num_versions, timestamp=None, attributes=None):
        """
        :type row: str
        :type column: str
        :type timestamp: int
        :type num_versions: int
        :type attributes: dict
        :param row:
        :param column:
        :param timestamp:
        :param num_versions:
        :param attributes:
        :return:
        :rtype: list
        """
        if attributes is None:
            attributes = {}
        if timestamp is None:
            return self.get_ver(self.name, row, column, num_versions, attributes)
        return self.get_ver_with_timestamp(self.name, row, column, timestamp, num_versions, attributes)

    def put(self):
        pass
