#!/usr/bin/env python
# coding=gb2312

from NInput import NInput
from NSqlDB import NSqlDB
from NOp import NOpMgr
from NDataOp import *
from NSqlOp import *
import time
import datetime
import sys
from NLog import NLogger

class NInputSql(NInput):
    def __init__(self, db,sql, readyCheck = True,tryTimes = 15, interval = 15):
        self.tryTimes= tryTimes
        self.readyCheck = readyCheck
        self.interval = interval
        self.sql = sql
        # used to check if data is ready
        self.dataLen = 0
        self.db = db
        self.dataSet = None
        NInput.__init__(self)

        if self.readyCheck:
            self.get_data()
            self.wait_for_data_ready()
        else:
            self.get_data()
        schema = self.get_schema(self.find_data(self.dataSet))
        # guess field and fieldtype, if you'd better use assign method to fix field and fieldtype
        self.fields = [item.keys()[0] for item in schema]
        self.fieldType={}
        for ft in schema:
            self.fieldType.update(ft)
    def get_schema(self, sampledata):
        if not sampledata: return {}
        fieldTypeDict= {"str":"varchar(128)","long":"bigint","int":"int","float":"float","NoneType":"varchar(128)","Decimal":"Decimal","date":"DATE","datetime":"DATETIME"}
        schema = [{k:fieldTypeDict[type(v).__name__]} for k,v in sampledata.items()] 
        return schema
    # lookup data to find one line with no-None valuse
    def find_data(self, data):
        if not data: return data
        for d in data:
            nonFlag = False
            for v in d.values():
                if v is None:
                    nonFlag=True
                    break
            if nonFlag:
                continue
            else:
                return copy.deepcopy(d)
        # return last line
        return copy.deepcopy(d)
    def registerTask(self):
        # register task input/output
        self.taskEnv.registerTask(self.db, self.sql , self.__class__.__name__, self.getTaskIOType())
        self.setTaskIOType(None)
    def createDataTable(self):
        import NInputData
        dataTable = NInputData.NInputData(copy.deepcopy(self.dataSet))
        dataTable.fields = copy.deepcopy(self.fields)
        dataTable.fieldType = copy.deepcopy(self.fieldType)
        return dataTable

    def get_data(self):
        cursor = NSqlDB(self.db)
        cursor.useDictCursor()
        for sql in self.sql.strip(";").split(";"):
            if not sql.strip(" \n"): continue
            cursor.execute(sql)
        self.dataSet = cursor.fetchall()
        cursor.close()
    # it's not the best way to check for data ready,
    # but  there is no better way than this for sync with  fetching data from  sql
    def wait_for_data_ready(self):
        isOk = False
        for i in range(self.tryTimes):
            if self.dataSet:
               NLogger.debug("dataLen before: %s, dataLen now :%s "%(self.dataLen, len(self.dataSet)))
               if self.dataLen!=len(self.dataSet):
                    self.dataLen = len(self.dataSet)
                    isOk = False
                    NLogger.info("wait for :%s "%(self.sql)) 
                    time.sleep(60)
                    self.get_data()
                    continue
               isOk = True
               break
            else:
                NLogger.info("wait for :%s "%(self.sql)) 
                time.sleep(self.interval*60)
                self.get_data()

        if not isOk:
            raise Exception("Data NOT READY! sql: %s failed" %(self.sql))
    
            
