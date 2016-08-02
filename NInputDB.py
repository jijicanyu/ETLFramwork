#!/usr/bin/env python
# coding=gbk

from NInput import NInput
from NOp import NOpMgr
from NSqlOp import *
from NSqlDB import NSqlDB
from NLog import NLogger
import DBConf 
import copy
import datetime
import random

class NInputDB(NInput):
    def __init__(self, db, tableName):
        self.db = db
        self.tableName = tableName
        self.DBConf = DBConf.DBConf[db]
        ignore_scheme = DBConf.DBConf[db].get("ignore_scheme", False)
        super(NInputDB, self).__init__()
        if not ignore_scheme:
            self.get_schema()
        self.fieldTypeCache = {}
    
    def get_schema(self):
        sqlDB = NSqlDB(self.db)
        sqlDB.useDictCursor()
        sqlDB.execute("SHOW TABLES LIKE '%s'" % (self.tableName.replace('`', '')))
        exist_info = sqlDB.fetchall()
        if exist_info:
            sqlDB.execute("SHOW FULL COLUMNS FROM %s" % (self.tableName))
            columns_attr = sqlDB.fetchall()
            for col in columns_attr:
                self.fields.append(col['Field'])
                self.fieldType[col['Field']] = col['Type']
                self.fieldComment[col['Field']] = col['Comment']
        sqlDB.close()

    def registerTask(self):
        # register task input/output
        self.taskEnv.registerTask(self.db, self.tableName, self.__class__.__name__, self.getTaskIOType())
        self.setTaskIOType(None)
    
    def createDBTable(self, db, tableName, keyFields=None, partition=None,
            indexList=None, overwrite=True, createCols=False, charset='gbk'):
        self.opMgr.appendOp(NSqlCreateDBTableOp(self.db, db, tableName, self.fields,
                    self.fieldType, keyFields, partition, indexList=indexList, overwrite=overwrite,
                    createCols=createCols, fieldComment = self.fieldComment, charset = charset))
        self.opMgr.process(self.tableName)
        self.setTaskIOType("output")
        return NInputDB(db, tableName)
    
    def createSQL(self):
        op = NSqlCreateSQLOp()
        self.opMgr.appendOp(op)
        self.opMgr.process(self.tableName)
        return op.value()
        
    def createDBView(self, viewName):
        self.opMgr.appendOp(NSqlCreateDBViewOp(self.db, viewName))
        self.opMgr.process(self.tableName)
        self.setTaskIOType("output")
        return NInputDB(self.db, viewName)

    def updateDBTable(self, db, tableName, updateFields, keyFields=None, createDB=True,
            createCols=False, partition=None, indexList=None, charset='gbk'):
        self.opMgr.appendOp(NSqlUpdateDBTableOp(self.db, db, tableName, keyFields + updateFields,
                    self.fieldType, updateFields, keyFields, createDB, createCols, partition,
                    indexList=indexList, fieldComment = self.fieldComment, charset = charset))
        self.opMgr.process(self.tableName)
        self.setTaskIOType("output")
        return NInputDB(db, tableName)

    def createFile(self, filename=None, charset="gbk"):
        if not filename:
            filename = self.mkTmpFileName()
        if self.taskEnv.enable_mfs and (filename.find("/") == -1):
            mfs_path = self.taskEnv.mfs_path
            mfsfilename = mfs_path + "/" + filename
            NLogger.info("File path: %s"%(mfsfilename))
        else:
            mfsfilename = filename
        dir = os.path.dirname(os.path.abspath(mfsfilename))
        if not os.path.exists(dir):
            os.makedirs(dir)
        op = NSqlCreateFileOp(self.db, mfsfilename, charset)
        self.opMgr.appendOp(op)
        self.opMgr.process(self.tableName)
        self.setTaskIOType("output")
        import NInputFile
        inputfile = NInputFile.NInputFile(mfsfilename)
        inputfile.fields = copy.deepcopy(self.fields)
        inputfile.fieldType = copy.deepcopy(self.fieldType)
        
#self.opMgr.appendOp(NSqlCreateFileOp(self.db, filename))
#        self.opMgr.process(self.tableName)
#        self.setTaskIOType("output")
#        inputfile = NInputFile.NInputFile(filename)
#        inputfile.fields = copy.deepcopy(self.fields)
#        inputfile.fieldType = copy.deepcopy(self.fieldType)
        inputfile.fieldComment = copy.deepcopy(self.fieldComment)
        return inputfile
        
    def createDict(self, keyField, valueField):
        op = NSqlCreateDictOp(self.db, keyField, valueField)
        self.opMgr.appendOp(op)
        self.opMgr.process(self.tableName)
        return op.value()

    def createMultiDict(self, keyField, valueFields):
        op = NSqlCreateMultiDictOp(self.db, keyField, valueFields)
        self.opMgr.appendOp(op)
        self.opMgr.process(self.tableName)
        return op.value()
    
    def createDataTable(self):
        op = NSqlCreateDataTableOp(self.db)
        self.opMgr.appendOp(op)
        self.opMgr.process(self.tableName)
        import NInputData
        dataTable = NInputData.NInputData(op.value())
        dataTable.fields = copy.deepcopy(self.fields)
        dataTable.fieldType = copy.deepcopy(self.fieldType)
        dataTable.fieldComment = copy.deepcopy(self.fieldComment)
        return dataTable
    
    def dumpTable(self, filename=None, charset="gbk"):
        if not filename:
            filename = self.mkTmpFileName()
        if self.taskEnv.enable_mfs and (filename.find("/") == -1):
            mfs_path = self.taskEnv.mfs_path
            mfsfilename = mfs_path + "/" + filename
            NLogger.info("File path: %s"%(mfsfilename))
        else:
            mfsfilename = filename
        dir = os.path.dirname(os.path.abspath(mfsfilename))
        if not os.path.exists(dir):
            os.makedirs(dir)
        op = NSqlDumpOp(self.DBConf, mfsfilename, charset)
        self.opMgr.appendOp(op)
        self.opMgr.process(self.tableName)
        self.setTaskIOType("output")
        import NInputFile
        inputfile = NInputFile.NInputFile(mfsfilename)
        inputfile.fields = copy.deepcopy(self.fields)
        inputfile.fieldType = copy.deepcopy(self.fieldType)
        inputfile.fieldComment = copy.deepcopy(self.fieldComment)
        return inputfile

    def select(self, fields, asField = None):
        expandFields = []
        for field in fields:
            if field == "*":
                expandFields.extend(self.fields)
            else:
                expandFields.append(field)
        self.opMgr.appendOp(NSqlSelectOp(expandFields, asField))
        return NInput.select(self, expandFields)

    def where(self, whereStat):
        self.opMgr.appendOp(NSqlWhereOp(whereStat))
        return NInput.where(self, whereStat)

    def group(self, fields):
        new_fields = []
        for field in fields:
            if field in self.fieldTypeCache.keys():
                new_fields.append(self.fieldTypeCache[field])
            else:
                new_fields.append(field)
        self.opMgr.appendOp(NSqlGroupOp(new_fields))
        return NInput.group(self, new_fields)
    
    def orderby(self, fields):
        self.opMgr.appendOp(NSqlOrderOp(fields))
        return self
    def having(self, fields):
        self.opMgr.appendOp(NSqlHavingOp(fields))
        return self
    
    def limit(self, fields):
        self.opMgr.appendOp(NSqlLimitOp(fields))
        return self

    def each(self, statement, asField=None, type=None, insertIdx=-1, comment=''):
        """
        @Desc: each function
        @Author: xiangyuanfei
        """
        self.opMgr.appendOp(NSqlEachOp(statement, asField))
        if insertIdx < 0:
            self.fields.append(asField)
        else:
            self.fields.insert(insertIdx, asField)
        if(type):
            self.fieldType[asField] = type
        # store statment in fieldTypeCache
        self.fieldTypeCache[asField] = statement
        self.fieldComment[asField] = comment
        return self
    
    def eachList(self, stList, asFieldList, typeList):
        for i in range(len(stList)):
            self.opMgr.appendOp(NSqlEachOp(stList[i], asFieldList[i]))
            self.fields.insert(-1, asFieldList[i])
            self.fieldType[asFieldList[i]] = typeList[i]
        return self
    
    def dropTable(self, tb=None):
        if tb:
            tableName = tb
        else:
            tableName = self.tableName 
            self.tableName = None
        op = NSqlDropTableOp(self.db, tableName)
        # clear because opMgr has bug, if self.opMgr.root is not None, self.opMgr.process can not drop table
        self.opMgr.clear()
        self.opMgr.appendOp(op)
        self.opMgr.process(tableName)
        return self

    def delete(self, condition):
        """Written by majianqing@baidu.com
           Date: 2016-03-30
        """
        conn = NSqlDB(self.db)
        if conn.isTableExists(self.tableName):
            sql = "DELETE FROM %s WHERE %s;" % (self.tableName, condition)
            conn.execute(sql)
        else:
            NLogger.info("[WARNING] Try to delete data, but %s.%s does not exist."\
                % (self.db, self.tableName))
        conn.close()
        return self

    def execSQL(self, sql):
        op = NSqlExecSQLOp(self.db, sql)
        self.opMgr.appendOp(op)
        self.opMgr.process(self.tableName)
        self.setTaskIOType("output")
        return NInputDB(self.db, self.tableName)

class  NInputTempDB(NInputDB):
    def __init__(self, db, tablename):
        super(NInputDB, self).__init__( db, tablename)

    def __del__(self):
        super(NInputTempDB, self).__del__()
        op = NSqlDropTableOp(self.db,self.tableName)
        self.opMgr.appendOp(op)
        self.opMgr.process(self.tableName)

