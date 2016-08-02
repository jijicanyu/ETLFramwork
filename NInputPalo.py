# coding=utf8
from NInputDB import NInputDB
from NPaloOp import *


class NInputPalo(NInputDB):
    def __init__(self, db, tableName):
        NInputDB.__init__(self, db, tableName)

    def registerTask(self):
        self.taskEnv.registerTask(self.db, self.tableName,
                                  self.__class__.__name__,
                                  self.getTaskIOType())
        self.setTaskIOType(None)

    @classmethod
    def createTable(cls, db, tableName, schema, key_fields,
                    hash_mod=61,
                    partition_columns=None,
                    partition_method='hash',
                    hash_method='full_key',
                    data_file_type='row',
                    rollup_tables=None,
                    overwrite=False):
        fields, hive_schema, palo_schema = PaloClient.convert_schema(schema)
        op = NPaloCreateTableOp(
            self.database, self.tableName,
            fields, hive_schema, palo_schema, key_fields,
            partition_method=partition_method,
            partition_columns=partition_columns,
            hash_method=hash_method,
            hash_mod=hash_mod,
            data_file_type=data_file_type,
            rollup_tables=rollup_tables,
            overwrite=overwrite)
        self.opMgr.appendOp(op)
        self.opMgr.process('%s.%s' % (self.database, self.tableName))
        return NInputPalo(db, tableName)

    def loadData(self, hivedb, hivetable, label,
                 max_filter_ratio=0,
                 timeout=3600,
                 partition=None):
        op = NPaloLoadDataOp(self.database, self.tableName,
                             hivedb, hivetable,
                             label, max_filter_ratio=max_filter_ratio,
                             timeout=timeout, partition=partition)
        self.opMgr.appendOp(op)
        self.opMgr.process('%s.%s' % (self.database, self.tableName))
        return self

    def createPaloTable(self, db, tableName, keyFields, overrideFields=None,
                        overwrite=False, max_filter_ratio=0, label=None,
                        timeout=3600, partition=None):
        """
        f = self.dumpTable()
        self.opMgr.clear()
        self.opMgr.appendOp(NPaloCreateTableOp(
            db, tableName, self.fields, self.fieldType,
            overrideFields, keyFields,
            overwrite=overwrite,
            data_file_type='column',
            type_map_type='mysql'))
        self.opMgr.appendOp(NPaloLoadRawDataOp(
            db, tableName, f.fields,
            label=label,
            max_filter_ratio=max_filter_ratio,
            timeout=timeout,
            partition=partition))
        self.opMgr.process(f.filename)
        self.setTaskIOType("output")
        f.removeFile()
        return NInputPalo(db, tableName)
        """
        return

    def dropTable(self, tb=None):
        """
        忽略删除整张表的操作。
        """
        """
        if not tb:
            tb = self.tableName
        op = NPaloDropTableFamilyOp(self.db, tb)
        self.opMgr.appendOp(op)
        self.opMgr.process('%s.%s' % (self.db, tb))
        """
        return self

    def dumpTable(self, filename=None, charset='utf8'):
        return NInputDB.dumpTable(self, filename, charset)

    def getRowCount(self):
        op = NPaloGetRowCountOp(self.db, self.tableName)
        op.process('%s.%s' % (self.db, self.tableName))
        return op.get_value()

if __name__ == '__main__':
    p = NInputPalo('test_palo_db', 'table_5').dropTable()
