# coding: utf8

import os
import time
import random
from NHqlOp import parse_schema_and_options
from NOp import NOp
from PaloClient import PaloClient, TableFamilyInfo
from NLog import NLogger

class NPaloDropTableFamilyOp(NOp):
    """删除 table family。
    参数：
        db : DBConf 中的数据库名称。
        tbName : 表名。
    """
    def __init__(self, db, tbname):
        NOp.__init__(self)
        self.db = db
        self.tbname = tbname

    def process(self, data):
        self.palo = PaloClient(self.db)
        self.palo.drop_table_family(self.tbname)


class NPaloGetRowCountOp(NOp):
    """估算数据行数。
    参数：
        db : DBConf 中的数据库名称
        tbName : 表名。
    """
    def __init__(self, db, tbname):
        NOp.__init__(self)
        self.db = db
        self.tbname = tbname

    def process(self, data):
        self.palo = PaloClient(self.db)
        self.value = self.palo.get_row_count(self.tbname)

    def get_value(self):
        return self.value


class NPaloCreateTableOp(NOp):
    """创建新的 table family。
    参数：
        db : DBConf 中的数据库名称。
        tbName : 表名。
        fields : 字段名称的列表。
        fieldType : 一个字典，各字段对应的 hive 数据类型，{字段名: 类型}。
            支持的数据类型为 tinyint, smallint, int, bigint, boolean,
            float, double, decimal, string, varchar。
        overrideFields : 一个字典，覆盖默认设置的字段 {字段名: 字段配置}。
            默认为 {}，用于修改字段的长度、默认值和聚合方法。字段配置的写法与
            palo-client 命令行中的相同，忽略字段名称与类型，
            例如 'DEF(0),AGG(MAX)'。
            各个类型的默认设置为：
                tiny/small/int/long : DEF(0)
                float/double : AGG(ADD)
                decimal: AGG(ADD),PRE(10),SCA(6)
                varchar/string: DEF(''),LEN(50)
        keyFields : 维度列字段名称的列表。
            非维度列的字段，根据类型指定默认聚合方法：
                tiny/small/int/long/decimal/double/float : AGG(ADD)
                string/varchar : AGG(REPLACE)
        overwrite: 是否删除数据中原有的同名表，默认为 False。
        hash_mod, partition_columns, hash_method : 创建表时的参数，默认情况下
            hash_method='full_key'，hash_mod=61。
    """
    def __init__(self, db, tbName,
                 fields, fieldType,
                 overrideFields=None,
                 keyFields=None,
                 overwrite=False,
                 hash_mod=61,
                 partition_columns=None,
                 hash_method='full_key',
                 data_file_type='row',
                 rollup_tables=None,
                 type_map_type='hive'):

        NOp.__init__(self)
        self.db = db
        self.tbName = tbName

        self.fields = fields
        self.fieldType = fieldType
        self.overrideFields = overrideFields
        self.keyFields = keyFields
        self.type_map_type = type_map_type

        self.overwrite = overwrite
        self.hash_mod = hash_mod
        self.partition_columns = partition_columns
        self.hash_method = hash_method
        self.data_file_type = data_file_type
        self.rollup_tables = rollup_tables

    def process(self, data):
        palo = PaloClient(self.db)
        modified_schema = palo.create_schema(self.fields, self.fieldType,
                                             self.overrideFields, self.keyFields, self.type_map_type)
        table_info = TableFamilyInfo(
            self.tbName,
            modified_schema,
            partition_method='hash',
            data_file_type=self.data_file_type,
            hash_mod=self.hash_mod,
            hash_method=self.hash_method,
            partition_columns=self.partition_columns
        )
        if self.rollup_tables:
            for table in self.rollup_tables:
                table_info.add_rollup_dict(table)

        if self.overwrite and palo.is_table_exist(self.tbName):
            palo.drop_table_family(self.tbName)
        if not palo.is_table_exist(self.tbName):
            palo.create_table_family(self.tbName, table_info, overwrite=False)
        if self.next:
            self.next.process(data)


class NPaloLoadDataOp(NOp):
    """从 hive 的表中载入数据。
    参数：
        palodb : DBConf 中目标的数据库的名称。
        palotable : 目标表名。
        hivedb : hive 中的数据库名。
        hivetable : hive 中的表名。
        label : 导入任务的label。
        max_filter_ratio : 容错的比率,0..1。
        separator : hive 中的分隔字段。
        timeout : 超时时间。
        partition : 待删除的原始数据的部分。
    """
    def __init__(self, palodb, palotable, hivedb, hivetable, label,
                 max_filter_ratio=0,
                 separator='\t',
                 timeout=3600,
                 partition=None):
        self.palodb = palodb
        self.palotable = palotable
        self.hivedb = hivedb
        self.hivetable = hivetable
        self.label = label
        self.separator = separator
        self.max_filter_ratio = max_filter_ratio
        self.timeout = timeout
        self.partition = partition

    def process(self, data):
        schema, option = parse_schema_and_options(
            self.hivedb,
            self.hivetable,
            ''
        )

        fields = schema.keys()
        locations = ['%s/*' % (loc) for loc in option['location']]
        palo = PaloClient(self.palodb)
        palo.batch_load_sync(
            self.label,
            self.palotable,
            urls=locations,
            fields=fields,
            separator=self.separator,
            timeout=self.timeout,
            max_filter_ratio=self.max_filter_ratio,
            partition=self.partition
        )
        return data
