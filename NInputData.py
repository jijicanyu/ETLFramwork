#!/usr/bin/env python
# coding=gbk

from NLog import NLogger
from NInput import NInput
from NSqlDB import NSqlDB
from NOp import NOpMgr
from NDataOp import *
from NSqlOp import *
import datetime
import random

class NInputData(NInput):
    def __init__(self, dataSet):
        super(NInputData, self).__init__()
        self.dataSet = dataSet
        self.fieldComment = {}
    # 传递opMgr参数是为了实现NInputMulti的折中
    def iterator(self, opMgr, begin=0, debug=0):
        for data in self.dataSet:
            opMgr.process(data)
    def registerTask(self):
        # register task input/output
        pass
    
    def doProcess(self):
        self.preProcess()
        self.iterator(self.opMgr)
        self.opMgr.processEnd()
        self.postProcess()
        self.dataSet = None
        
    def createDBTable(self, db, tableName, overwrite=True, keyFields=None, partition=None,
            indexList=None, createCols=False, charset='gbk'):
        sqlDB = NSqlDB(db)

        sqlDB.createTable(tableName, self.fields, self.fieldType, overwrite, keyFields,
                partition=partition, indexList=indexList,
                fieldComment=self.fieldComment, charset=charset)
        self.opMgr.appendOp(NDBInsertOp(db, tableName, self.fields))
        self.doProcess()
        self.setTaskIOType("output")
        import NInputDB
        return NInputDB.NInputDB(db, tableName)

    def updateDBTable(self, db, tableName, updateFields, keyFields=None, createCols=False,
            partition=None, indexList=None, createTable=True, charset='gbk'):
        sqlDB = NSqlDB(db)
        sqlDB.createTable(tableName, keyFields + updateFields, self.fieldType, False,
                keyFields, partition=partition, indexList=indexList, 
                fieldComment=self.fieldComment, charset=charset)
        self.opMgr.appendOp(NDBUpdateWithFieldsOp(db, tableName, keyFields + updateFields, updateFields, self.fieldType, createCols))
        self.doProcess()
        self.setTaskIOType("output")
        import NInputDB
        return NInputDB.NInputDB(db, tableName)
        
    def updateDBTableWithFields(self, db, tableName, updateFields, keyFields=None, partition=None, indexList=None):
        sqlDB = NSqlDB(db)
        sqlDB.createTable(tableName, keyFields + updateFields, self.fieldType, False, keyFields,
                partition, indexList=indexList,
                fieldComment = self.fieldComment)
        self.opMgr.appendOp(NDBUpdateWithFieldsOp(db, tableName, keyFields + updateFields, updateFields))
        self.doProcess()
        self.setTaskIOType("output")
        import NInputDB
        return NInputDB.NInputDB(db, tableName)

    def createDict(self, keyField, valueField):
        op = NCreateDictOp(keyField, valueField)
        self.opMgr.appendOp(op)
        self.doProcess()
        return op.value()

    def createMultiDict(self, keyField, valueFields):
        op = NCreateMultiDictOp(keyField, valueFields)
        self.opMgr.appendOp(op)
        self.doProcess()
        return op.value()
    
    def createMultiList(self, keyFields):
        op = NCreateMultiListOp(keyFields)
        self.opMgr.appendOp(op)
        self.doProcess()
        return op.value()
    
    def createDataTable(self):
        op = NCreateDataTableOp()
        self.opMgr.appendOp(op)
        self.doProcess()
        dataTable = NInputData(op.value())
        dataTable.fields = copy.deepcopy(self.fields)
        dataTable.fieldType = copy.deepcopy(self.fieldType)
        dataTable.fieldComment = copy.deepcopy(self.fieldComment)
        return dataTable

    def createFile(self, filename=None):
        if not filename:
            filename = self.mkTmpFileName()
        if self.taskEnv.enable_mfs and (filename.find("/") == -1):
            mfs_path = self.taskEnv.mfs_path
            mfsfilename = mfs_path + "/" + filename
            NLogger.info("Create file: %s"%(mfsfilename))
        else:
            mfsfilename = filename
        dir = os.path.dirname(os.path.abspath(mfsfilename))
        if not os.path.exists(dir):
            os.makedirs(dir)
        self.opMgr.appendOp(NCreateFileOp(mfsfilename, self.fields))
        self.doProcess()
        self.setTaskIOType("output")
        import NInputFile
        inputFile = NInputFile.NInputFile(mfsfilename)
        inputFile.fields = copy.deepcopy(self.fields)
        inputFile.fieldType = copy.deepcopy(self.fieldType)
        inputFile.fieldComment = copy.deepcopy(self.fieldComment)
        return inputFile
    
    def sum(self, field, asField, type=None):
        self.opMgr.appendOp(NSumOp(field, asField))
        return NInput.sum(self, field, asField, type)

    def bitOr(self, field, asField, type=None):
        self.opMgr.appendOp(NBitOrOp(field, asField))
        return self
    
    def count(self, field, asField, type=None, distinct=False):
        """
            when distinct is True, count with no repeat
        """
        self.opMgr.appendOp(NCountOp(field, asField, distinct))
        return NInput.count(self, asField, type)
    
    def min(self, field, asField, type=None):
        self.opMgr.appendOp(NMinOp(field, asField))
        return NInput.min(self, field, asField, type)

    def average(self, field, asField, type=None):
        self.opMgr.appendOp(NAverageOp(field, asField))
        return NInput.average(self, field, asField, type)
 
    def top(self, field, topCnt):
        self.opMgr.appendOp(NTopOp(field, topCnt))
        return self
    
    def select(self, fields):
        expandFields = []
        for field in fields:
            if field == "*":
                expandFields.extend(self.fields)
            else:
                expandFields.append(field)
        self.opMgr.appendOp(NSelectOp(expandFields))
        return NInput.select(self, expandFields)
    
    def each(self, statment, asField, type, comment=None):
        """
        @Desc: each function
        @Author: xiangyuanfei
        """
        if(not self.fields.__contains__(asField)):
            self.fields.append(asField)
        self.fieldType[asField] = type
        if self.fieldComment is None:
            self.fieldComment = {}
        self.fieldComment[asField] = comment
        self.opMgr.appendOp(NEachOp(statment, asField))
        return self
    
    def join(self, inputData, joinFields, linkFields, defaultValue=None):
        for field in joinFields:
            if self.fields.__contains__(field): continue
            self.fields.append(field)
            self.fieldType[field] = inputData.fieldType[field]
            if inputData.fieldComment and (field in inputData.fieldComment.keys()):
                self.fieldComment[field] = inputData.fieldComment[field]
            else:
                self.fieldComment[field] =  ""
        self.opMgr.appendOp(NJoinOp(inputData.dataSet,joinFields,linkFields, defaultValue))
        return self
        
    def group(self, fields):
        self.opMgr.appendOp(NGroupOp(fields))
        return NInput.group(self, fields)
    
    def filter(self, callback):
        self.opMgr.appendOp(NFilterOp(callback))
        return NInput.where(self, callback)

    def swapGroup(self, fields):
        self.opMgr.appendOp(NSwapGroupOp(fields))
        return NInput.group(self, fields)

    def cloneGroup(self, fields):
        self.opMgr.appendOp(NCloneGroupOp(fields))
        return NInput.group(self, fields)

    def fakeGroup(self, fields):
        self.opMgr.appendOp(NFakeGroupOp(fields))
        return self
    
    def process(self, callback, userData=None, fieldTypes=None):
        if fieldTypes:
            for field in fieldTypes:
                self.fields.append(field)
                self.fieldType[field] = fieldTypes[field]
        
        # 允许传入函数或processor对象
        # processor对象必须支持preProcess, process, postProcess三种方法
        # python不支持函数重载，暂时将这两种process方式放在同一函数内
        if hasattr(callback, '__call__'):
            self.opMgr.appendOp(NProcessOp(callback, userData))
        else:
            self.opMgr.appendOp(NProcessorOp(callback))
        return self

    def processFast(self, callback, userData=None, fieldTypes=None):
        self.opMgr.appendOp(NProcessFastOp(callback, userData))
        return self

    def processEach(self, processor):
        processor.processFields(self.fields)
        processor.processFieldType(self.fieldType)
        self.opMgr.appendOp(NProcessEachOp(processor))
        return self
        
    def unionAll(self, inputData):
        # inputdata has the same shema with basic
        op = NUnionAllOp(inputData.dataSet)
        self.opMgr.appendOp(op)
        self.doProcess()
        dataTable = NInputData(op.value())
        dataTable.fields = copy.deepcopy(self.fields)
        dataTable.fieldType = copy.deepcopy(self.fieldType)
        dataTable.fieldComment = copy.deepcopy(self.fieldComment)
        return dataTable

    def union(self, inputData, uniqKey=[], overwrite=True):
        # inputdata has the same shema with basic, uniq by uniqKey
        op = NUnionOp(inputData, uniqKey,overwrite)
        self.opMgr.appendOp(op)
        self.doProcess()
        dataTable = NInputData(op.value())
        dataTable.fields = copy.deepcopy(self.fields)
        dataTable.fieldType = copy.deepcopy(self.fieldType)
        return dataTable

    def fullJoin(self, inputData, joinFields, linkFields, defaultValue=0):
    # linkField must be uniq
        for field in joinFields:
            self.fields.append(field)
            self.fieldType[field] = inputData.fieldType[field]
            if inputData.fieldComment  and (field in inputData.fieldComment.keys()):
                self.fieldComment[field] = inputData.fieldComment[field]
            else:
                self.fieldComment[field] = ""
        op = NFullJoinOp(inputData.dataSet,joinFields,linkFields, defaultValue)
        self.opMgr.appendOp(op)
        self.doProcess()
        dataTable = NInputData(op.value())
        dataTable.fields = copy.deepcopy(self.fields)
        dataTable.fieldType = copy.deepcopy(self.fieldType)
        dataTable.fieldComment = copy.deepcopy(self.fieldComment)
        return dataTable

    def updateDataTable(self, inputData, updateFields, keyFields, updateCallback=None):
        op=NUpdateDataTableOp(inputData.dataSet, updateFields, keyFields, updateCallback)
        self.opMgr.appendOp(op)
        self.doProcess()
        dataTable = NInputData(op.value())
        dataTable.fields = copy.deepcopy(self.fields)
        dataTable.fieldType = copy.deepcopy(self.fieldType)
        dataTable.fieldComment = copy.deepcopy(self.fieldComment)
        return dataTable

    def renameField(self, field, asField, type=None, comment=None):
        """
        @Desc:
        @Author: xiangyuanfei
        """
        newField=[]
        for t_field in self.fields:
            if t_field == field:
                newField.append(asField)
            else:
                newField.append(t_field)
        if not type:
            self.fieldType[asField] = self.fieldType[field]
        else:
            self.fieldType[asField] = type
        if not comment:
            self.fieldComment[asField] = comment
        else:
            self.fieldComment[asField] = field in self.fieldComment.keys() and \
                                        self.fieldComent[field] or "NULL"
        self.fields = copy.deepcopy(newField)
        op = NRenameOp(field, asField)    
        self.opMgr.appendOp(op)
        self.doProcess()
        inputData = NInputData(op.value())
        inputData.fields = copy.deepcopy(self.fields)
        inputData.fieldType = copy.deepcopy(self.fieldType)
        dataTable.fieldComment = copy.deepcopy(self.fieldComment)
        return inputData
