#!/usr/bin/env python
# coding=gbk

from NInput import NInput
from NInputData import NInputData
from NSqlDB import NSqlDB
from NOp import NOpMgr
from NDataOp import *
from NDataOpThread import *
import os
from multiprocessing import Process, Queue, Pipe


def fileThreadFunc(worker):
    worker.process()

class NInputFileThread(NInputData):
    def __init__(self, filename, threadCnt=1):
        NInputData.__init__(self, None)
        self.filename = filename
        self.threadCnt = threadCnt
        self.workers = []

    def run(self, idx):
        print "this io thread %d"%(os.getpid())
        
        len = os.path.getsize(self.filename)
        fileStream = file(self.filename, 'r')
        endPos = int(float(idx + 1) / self.threadCnt * len)
        fileStream.seek(int(float(idx) / self.threadCnt * len))

        #丢弃非首块的第一行
        if idx:
            fileStream.readline()

        cnt = 0
        while True:
            pos = fileStream.tell()
            if pos > endPos:
                break
            line = fileStream.readline().strip()
            if not line:
                break
            
            if idx == 0 and cnt % 10000 == 0:
                print cnt
            cnt += 1
            itemArr = line.split("\t")
            data = {}
            for field in self.fieldIdx:
                data[field] = itemArr[self.fieldIdx[field]]
             
            if self.opMgr.root:
                self.opMgr.root.dispatch(data)
        
        self.opMgr.processEnd()
        print "line cnt is %d" % (cnt)


        
    def preProcess(self):
        NInputData.preProcess(self)
        for i in xrange(self.threadCnt):
            p = Process(target=NInputFileThread.run, args=(self, i))
            self.workers.append(p)

            
    def iterator(self, opMgr):
        for p in self.workers:
            p.start()

    def postProcess(self):
        for p in self.workers:
            p.join()
        if self.opMgr.root:
            self.opMgr.root.dispatch(0)
        NInputData.postProcess(self) 

    def removeFile(self):
        os.remove(self.filename)

    def group(self, fields, threadCnt=0):
        self.opMgr.appendOp(NGroupOpThread(fields, threadCnt))
        return NInput.group(self, fields)

    def group_1(self, fields, threadCnt=0):
        self.opMgr.appendOp(NGroupOpThread_1(fields, threadCnt))
        return NInput.group(self, fields)


    def select(self, fields, threadCnt=0):
        self.opMgr.appendOp(NSelectOpThread(fields, threadCnt))
        return NInput.select(self, fields)

    def process(self, callback, userData=None, fieldTypes=None, threadCnt=0):
        if fieldTypes:
            for field in fieldTypes:
                self.fields.append(field)
                self.fieldType[field] = fieldTypes[field]

        self.opMgr.appendOp(NProcessOpThread(callback, userData, threadCnt))
        return self


    def createFile(self, filename, threadCnt=1):
        self.opMgr.appendOp(NCreateFileOpThread(filename, self.fields))
        self.doProcess()
        import NInputFile
        inputFile = NInputFile.NInputFile(filename)
        inputFile.fields = copy.deepcopy(self.fields)
        inputFile.fieldType = copy.deepcopy(self.fieldType)
        return inputFile
    
    def createDBTable(self, db, tableName, overwrite=True, threadCnt=1):
        sqlDB = NSqlDB(db)
        sqlDB.createTable(tableName, self.fields, self.fieldType, overwrite)
        self.opMgr.appendOp(NDBInsertOpThread(sqlDB, tableName, self.fields, threadCnt))
        self.doProcess()
        import NInputDB
        return NInputDB.NInputDB(db, tableName)



    
class NInputFileThreadWorker():
    def __init__(self, filename, idx, total, fieldIdx, op):
        self.op = op
        self.fieldIdx = fieldIdx

        len = os.path.getsize(filename)
        self.fileStream = file(filename, 'r')
        self.readEnd = int(float(idx + 1) / total * len)
        self.fileStream.seek(int(float(idx) / total * len))
        self.idx = idx

        #丢弃非首块的第一行
        if idx:
            self.fileStream.readline()

    def process(self):
        print "begin subprocess"
        cnt = 0
        while True:
            pos = self.fileStream.tell()
            if pos > self.readEnd:
                break
            line = self.fileStream.readline().strip()
            if not line:
                break
            
            if self.idx == 0 and cnt % 10000 == 0:
                print cnt
            cnt += 1
            itemArr = line.split("\t")
            data = {}
            for field in self.fieldIdx:
                data[field] = itemArr[self.fieldIdx[field]]
             
            if self.op:
                self.op.dispatch(data)
        
        print "line cnt is %d" % (cnt)


