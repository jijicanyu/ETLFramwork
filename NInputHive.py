#!/usr/bin/env python
# coding=gbk

from NLog import NLogger
import copy
from Hive import Hive
from NInput import NInput
from NHqlOp import *
import datetime
import time
import random
import ordereddict
import os


class NInputHive(NInput):
    def __init__(self, db, tableName, init="", alias="a"):
        self.db = db
        self.tableName = copy.deepcopy(tableName)
        self.hv = Hive()
        self.hiveInit = init
        self.alias = alias
        self.hql = None

        super(NInputHive, self).__init__()
        # subQuery
        if isinstance(self.tableName, NInputHive):
            self.dbTable = " (" + self.tableName.hql + ") " + self.alias + " "
            self.fields = self.tableName.fields
            self.fieldType = self.tableName.fieldType
            self.paloFieldType = self.tableName.paloFieldType
            self.fieldComment = self.tableName.fieldComment
            self.fieldTypeCache = {}
        else:
            self.dbTable = self.alias\
                and (db + "." + tableName + " " + self.alias + " ")\
                or (db + "." + tableName)
            self.schema = self.get_schema()
            self.fields = [i for i in self.schema[0]]
            self.fieldType = self.schema[0]
            self.fieldTypeCache={}
            self.fieldComment = self.schema[1]
            self.paloFieldType = {}
            # update fieldComment
            """
            for s in self.schema[0]:
                self.fieldType.update(s)
            for t in self.schema[1]:
                self.fieldComment.update(s)
            """
        self.hiveInit = init

    def registerTask(self):
        # subquery
        if isinstance(self.tableName, NInputHive):
            pass
        else:
            self.taskEnv.registerTask(self.db,
                                      self.tableName,
                                      self.__class__.__name__,
                                      self.getTaskIOType())
            self.setTaskIOType(None)

    def get_schema(self):
        self.hv.Add("use %s;" % (self.db))
        self.hv.Add("desc %s;" % (self.tableName))
        schema_str = self.hv.fetchall()
        import Schema
        schema = Schema.get_schema_list(schema_str)
        schema_comment = Schema.get_schema_comment_list(schema_str)
        schema_ol = ordereddict.OrderedDict().appendList(schema)
        schema_comment_ol = ordereddict.OrderedDict().appendList(schema_comment)
        return [schema_ol, schema_comment_ol]

    def select(self, fields, asField=None):
        expandFields = []
        for field in fields:
            if field == "*":
                expandFields.extend(self.fields)
            else:
                expandFields.append(field)
        self.opMgr.appendOp(NHqlSelectOp(expandFields, asField))
        return NInput.select(self, expandFields)

    def transformSelect(self, fields, scriptName=""):
        expandFields = []
        for field in fields:
            if field == "*":
                expandFields.extend(self.fields)
            else:
                expandFields.append(field)
        self.opMgr.appendOp(NHqlTransformSelectOp(expandFields, scriptName))
        return NInput.select(self, expandFields)

    def lateralView(self, expr, alias, asField=None):
        self.opMgr.appendOp(NHqlLateralviewOp(expr, alias, asField))
        return self

    def where(self, whereStat):
        self.opMgr.appendOp(NHqlWhereOp(whereStat))
        return NInput.where(self, whereStat)

    def group(self, fields):
        new_fields = []
        for field in fields:
            if field in self.fieldTypeCache.keys():
                new_fields.append(self.fieldTypeCache[field])
            else:
                new_fields.append(field)
        self.opMgr.appendOp(NHqlGroupOp(new_fields))
        return NInput.group(self, new_fields)

    def orderby(self, fields):
        self.opMgr.appendOp(NHqlOrderOp(fields))
        return self

    def sortby(self, fields):
        self.opMgr.appendOp(NHqlSortOp(fields))
        return self

    def distributeby(self, fields):
        self.opMgr.appendOp(NHqlDistributeOp(fields))
        return self

    def having(self, fields):
        self.opMgr.appendOp(NHqlHavingOp(fields))
        return self

    def limit(self, fields):
        self.opMgr.appendOp(NHqlLimitOp(fields))
        return self

    def each(self, statement, asField=None, type=None, insertIdx=-1, comment=''):
        """
        @Desc: overwrite each function
        @Author: xiangyuanfei
        """
        self.opMgr.appendOp(NHqlEachOp(statement, asField))
        if asField is not None:
            if asField not in self.fields:
                if insertIdx < 0:
                    self.fields.append(asField)
                else:
                    self.fields.insert(insertIdx, asField)
            # temp fix bugs with "," for mysql
            # added by xiangyaunfei, 20150528
            if type and type.find(" , ") >=0:
                self.fieldType[asField] = type
                # store statment in fieldTypeCache
                self.fieldTypeCache[asField] = statement
                self.fieldComment[asField] = comment
                return self
                
            if type and type.find(',') >= 0:
                type, paloType = type.split(',', 1)
                self.paloFieldType[asField] = paloType

            self.fieldType[asField] = type
            # store statment in fieldTypeCache
            self.fieldTypeCache[asField] = statement
            self.fieldComment[asField] = comment
        else:
            self.fieldComment[statement] = comment
        return self

    def subSelect(self, fields):
        for field in fields:
            if field not in self.fieldType.keys():
                raise Exception("field %s not found" % (field))
            self.opMgr.appendOp(NHqlEachOp(field, None))
            self.fields.append(field)
        return self

    def join(self, inputhive, alias="b", joinType="LEFT", cond=""):
        # copy schema
        for field in self.fields:
            if field not in self.fieldType.keys():
                self.fieldType[field]= copy.deepcopy(inputhive.fieldType[field])
            if field not in self.fieldComment.keys():
                if  field in inputhive.fieldComment.keys():
                    self.fieldComment[field] = copy.deepcopy(inputhive.fieldComment[field])
                else:
                    self.fieldComment[field] = "NULL"
        op = NHqlJoinOp(inputhive, alias, joinType, cond)
        self.opMgr.appendOp(op)
        return self

    def writeToFile(self, filename, hql, singlefile=True):
        if self.taskEnv.enable_mfs and (filename.find("/") == -1):
            mfs_path = self.taskEnv.mfs_path
            mfsfilename = mfs_path + "/" + filename
            NLogger.info("Write to file: %s" % (mfsfilename))
        else:
            mfsfilename = filename
        dir = os.path.dirname(os.path.abspath(mfsfilename))
        if not os.path.exists(dir):
            os.makedirs(dir)
        op = NHqlWriteToFileOp(mfsfilename, hql, singlefile, self.hiveInit)
        self.opMgr.appendOp(op)
        self.opMgr.process(self.dbTable)
        self.setTaskIOType("output")
        import NInputFile
        inputfile = NInputFile.NInputFile(mfsfilename)
        inputfile.fields = copy.deepcopy(self.fields)
        inputfile.fieldType = copy.deepcopy(self.fieldType)
        inputfile.fieldComment = copy.deepcopy(self.fieldComment)
        return inputfile

    def createFile(self, filename=None, singlefile=True):
        if not filename:
            filename = self.mkTmpFileName()
        if self.taskEnv.enable_mfs and (filename.find("/") == -1):
            mfs_path = self.taskEnv.mfs_path
            mfsfilename = mfs_path + "/" + filename
            NLogger.info("Create file: %s" % (mfsfilename))
        else:
            mfsfilename = filename
        dir = os.path.dirname(os.path.abspath(mfsfilename))
        if not os.path.exists(dir):
            os.makedirs(dir)
        op = NHqlCreateFileOp(mfsfilename, singlefile, self.hiveInit)
        self.opMgr.appendOp(op)
        self.opMgr.process(self.dbTable)
        self.setTaskIOType("output")
        import NInputFile
        inputfile = NInputFile.NInputFile(mfsfilename)
        inputfile.fields = copy.deepcopy(self.fields)
        inputfile.fieldType = copy.deepcopy(self.fieldType)
        inputfile.fieldComment = copy.deepcopy(self.fieldComment)
        return inputfile

    def createHiveTable(self, db, tableName, partition=None, overwrite=False,
                        fileformat="TEXTFILE", createCols=False):
        op = NHqlCreateHiveTableOp(db, tableName, partition, self.hiveInit,
                                   overwrite, self.fields, self.fieldType,
                                   fileformat, False, 
                                   fieldComment=self.fieldComment)

        self.opMgr.appendOp(op)
        self.opMgr.process(self.dbTable)
        self.setTaskIOType("output")
        return NInputHive(db, tableName, self.hiveInit)

    def execHql(self, hql):
        op = NHqlExecHqlOp(hql, self.hiveInit)
        self.opMgr.appendOp(op)
        self.opMgr.process(self.dbTable)
        return self

    def subHql(self, hql, alias):
        self.opMgr.appendOp(NHqlFromOp(hql, alias))
        return self

    def createHql(self):
        op = NHqlCreateHqlOp(self.hiveInit)
        self.opMgr.appendOp(op)
        self.opMgr.process(self.dbTable)
        return op.getHql()

    def createSubQuery(self):
        op = NHqlCreateHqlOp(self.hiveInit)
        self.opMgr.appendOp(op)
        self.opMgr.process(self.dbTable)
        self.hql = op.getHql()
        return self

    def dropTable(self):
        op = NHqlDropTableOp(self.db, self.tableName)
        self.opMgr.appendOp(op)
        self.opMgr.process(self.db + "." + self.tableName)
        return self

    @classmethod
    def createExternalHiveTable(cls, db, tbName, location, partition,
                                fieldType, hiveInit="", alias='c',
                                overwrite=False, tmpTable=False, tryTimes=16,
                                interval=5 * 60, charset="gbk",
                                fieldDelimiter="\t", storedAs="TEXTFILE"):
        # record field
        if tbName is None:
            tableName = "tmp_%s_%s" % (
                datetime.datetime.now().strftime("%Y%m%d%H%M%S"),
                random.randrange(1, 10000)
            )
        else:
            tableName = tbName
        op = NHqlCreateExternalHiveTableOp(db, tableName, location, partition,
                                           fieldType, hiveInit, overwrite,
                                           tryTimes, interval, charset, fieldDelimiter, storedAs)\
            .process(db + "."+tableName)
        if tmpTable:
            return NInputTempHive(db, tableName, hiveInit, alias)
        else:
            return NInputHive(db, tableName, hiveInit, alias)

    def prepareTmpTableForPalo(self, temp_db, temp_table_name):
        schema_dict = {}
        for field in self.fields:
            schema_dict[field] = self.fieldType[field]
        init, self.hiveInit = self.hiveInit, ''
        self.createSubQuery()
        alias_name = 't'

        fields = [key for key in self.fields]

        # create temp hive table
        self.opMgr.clear()
        self.opMgr.appendOp(NHqlPrepareTmpTableForPaloOp(self.hql,
                                                         fields,
                                                         schema_dict,
                                                         alias_name))
        self.opMgr.appendOp(NHqlCreateHiveTableOp(temp_db,
                                                  temp_table_name,
                                                  partition={},
                                                  hiveInit=init,
                                                  overwrite=True,
                                                  fields=fields,
                                                  fieldType=schema_dict,
                                                  fileformat='TEXTFILE',
                                                  createCols=False))
        self.opMgr.process(temp_table_name)

        hv = NInputHive(temp_db, temp_table_name, init)
        return hv, fields, schema_dict

    def createPaloTableFamily(self, db, tbName,
                              label=None,
                              overwrite=False,
                              keyFields=None,
                              hash_mod=61,
                              hash_method='full_key',
                              partition_columns=None,
                              data_file_type='row',
                              max_filter_ratio=0,
                              timeout=3600,
                              is_negative=False,
                              partition=None,
                              rollup_tables=None):
        """
        batch load selected data into PALO table family.
        If not specified, label is automatically generated.

        rollup_tables is a list of dicts [{key : value, ....}, ...]
        for each roll up table, following keys must be set:
            'table_name' : name of the rollup table
            'column_ref' : a list of columns that appeared in roll up tables
        other options use base_table's value or default value if not overrided:
            'base_table_name' : name of table based on
            'base_index_name' : index of base table
            'index_name' : name of index (default: 'PRIMARY' or
                                          equals to base_index_name)
            'partition_method', 'partition_columns',
            'key_ranges', 'hash_mod',
            'hash_method', 'data_file_type'
        """
        timestamp = time.strftime('%Y%m%d%H%M%S')
        temp_table_name = 'tmp_%s_%d_%d' % (timestamp,
                                            int(random.random() * 10000),
                                            os.getpid())

        temp_db_name = 'exchange_db'

        hv, fields, schema_dict = self.prepareTmpTableForPalo(temp_db_name,
                                                              temp_table_name)

        # create and load to palo table
        NLogger.info('hive schema : ' + repr(schema_dict))
        NLogger.info('palo schema : ' + repr(self.paloFieldType))
        NLogger.info('keyFields : ' + repr(keyFields))

        try:
            op = NHqlCreatePaloTableFamilyOp(
                db, tbName, overwrite=overwrite,
                srcHiveDB=temp_db_name, srcHiveTable=temp_table_name,
                fields=fields, fieldType=schema_dict,
                overrideField=self.paloFieldType, keyFields=keyFields,
                hash_mod=hash_mod, hash_method=hash_method,
                partition_columns=partition_columns,
                data_file_type=data_file_type,
                rollup_tables=rollup_tables,
                label=label,
                max_filter_ratio=max_filter_ratio, timeout=timeout,
                is_negative=is_negative, partition=partition)

            op.process('%s.%s' % (self.db, temp_table_name))
            hv.setTaskIOType('output')
        finally:
            hv.dropTable()

        import NInputPalo
        return NInputPalo.NInputPalo(db, tbName)


class NInputTempHive(NInputHive):
    def __init__(self, db, tableName, init="", alias='a'):
        NInputHive.__init__(self, db, tableName, init, alias)

    def registerTask(self):
        self.setTaskIOType(None)

    def __del__(self):
        self.opMgr.clear()
        if isinstance(self.tableName, NInputHive):
            pass
        else:
            self.dropTable()
