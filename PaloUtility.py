# coding=utf8
from NHqlOp import NHqlPrepareTmpTableForPaloOp, NHqlCreateHiveTableOp
from Hive import Hive
from PaloClient import PaloClient
from NPaloOp import NPaloCreateTableOp, NPaloLoadDataOp
import os
import random
import time


def create_tmp_table(hql, tmpdb, tmptable, schema, hiveInit=''):
    """
    创建临时表，表中不包含 NULL 值。
    """
    fields, hive_schema, palo_schema = PaloClient.convert_schema(schema)
    createHql = NHqlPrepareTmpTableForPaloOp.create_statement(
        fields, hive_schema, hql, 's'
    )
    NHqlCreateHiveTableOp(tmpdb, tmptable,
            partition={},
            hiveInit=hiveInit,
            overwrite=True,
            fields=fields,
            fieldType=hive_schema,
            fileformat='TEXTFILE',
            createCols=False
    ).process(createHql)


def createPaloTable(dbterm, table_name, schema, key_fields,
                    data_file_type, rollup_tables=None, overwrite=False):
    """
    创建 PALO 中的表。
    """
    # schema
    fields, hive_schema, palo_schema = PaloClient.convert_schema(schema)
    NPaloCreateTableOp(
        dbterm, table_name,
        fields, hive_schema, palo_schema, key_fields,
        overwrite=overwrite,
        data_file_type=data_file_type,
        rollup_tables=rollup_tables
    ).process('%s.%s' % (dbterm, table_name))


def loadData(dbterm, table_name, hivedb, hivetable, label,
             max_filter_ratio, timeout, partition):
    """
    从 hive 中的一张表格导入数据到 palo 中。
    """
    NPaloLoadDataOp(
        dbterm, table_name, hivedb, hivetable, label,
        max_filter_ratio=max_filter_ratio,
        timeout=timeout,
        partition=partition
    ).process('%s.%s' % (dbterm, table_name))


def loadHiveDataToPalo(
        paloterm, palotable,
        schema, key_fields,
        hql, overwrite=False,
        data_file_type='column', rollup_tables=None,
        hiveInit='', label=None, timeout=3600,
        max_filter_ratio=0,
        partition=None, tmpdb='exchange_db'):
    """
    将 hql 执行结果导入到 palo 中。
    paloterm   : palo 表的 dbterm。
    palotable  : palo 的表名。
    schema     : 字段样式，[('字段名','hive类型','palo类型'),(...)]
    key_fields : 维度列的列表，注意顺序
    hql        : 输入的 hql
    overwrite  : 删除整张表
    data_file_type : row / column
    rollup_tables : 上卷表配置
    hiveInit   : hive 执行环境参数配置
    label      : palo 灌库任务标识
    timeout    : 灌库任务超时时间
    max_filter_ratio : 灌库任务允许出错的比例（0=0%，1=100%）
    partition  : 灌库之前删除的部分
    tmpdb      : hive 临时表所用的 db
    """

    # generate name for tmptable
    timestamp = time.strftime('%Y%m%d%H%M%S')
    tmptable = 'tmp_%s_%d_%d' % (timestamp,
                                 int(random.random() * 10000),
                                 os.getpid())
    createPaloTable(paloterm, palotable, schema,
                    key_fields, data_file_type, rollup_tables, overwrite)
    create_tmp_table(hql, tmpdb, tmptable, schema, hiveInit)
    loadData(paloterm, palotable, tmpdb, tmptable,
             label, max_filter_ratio, timeout, partition)
    # drop tmp table
    Hive().Execute('use %s;drop table %s;' % (tmpdb, tmptable))
