# -*- coding:utf-8 -*-
"""

__author__ = "zhangyu"

"""
import pkg_resources
import thriftpy

hbase_thrift = thriftpy.load(pkg_resources.resource_filename('hbasepy', 'hbase.thrift'),
                             module_name='hbase_thrift')
