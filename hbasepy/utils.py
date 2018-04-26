# -*- coding:utf-8 -*-
"""

__author__ = "zhangyu"

"""

from hbasepy.load import hbase_thrift


def column_descriptor(name, max_versions=3, compression='NONE', in_memory=False, bloom_filter_type='NONE',
                      bloom_filter_vector_size=0, bloom_filter_nb_hashes=0, block_cache_enabled=False,
                      time_to_live=-1):
    """
    :type name: str
    :type max_versions: int
    :type compression: str
    :type in_memory: bool
    :type bloom_filter_type: str
    :type bloom_filter_vector_size: int
    :type bloom_filter_nb_hashes: int
    :type block_cache_enabled: bool
    :type time_to_live: int

    :param name:
    :param max_versions:
    :param compression:
    :param in_memory:
    :param bloom_filter_type:
    :param bloom_filter_vector_size:
    :param bloom_filter_nb_hashes:
    :param block_cache_enabled:
    :param time_to_live:
    :return:
    """
    return hbase_thrift.ColumnDescriptor(**{
        "name": name,
        "maxVersions": max_versions,
        "compression": compression,
        "inMemory": in_memory,
        "bloomFilterType": bloom_filter_type,
        "bloomFilterVectorSize": bloom_filter_vector_size,
        "bloomFilterNbHashes": bloom_filter_nb_hashes,
        "blockCacheEnabled": block_cache_enabled,
        "timeToLive": time_to_live
    })
