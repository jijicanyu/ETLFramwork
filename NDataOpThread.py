#!/usr/bin/env python
# coding=gbk

from NSqlDB import NSqlDB
from NOp import NOp, NOpChild, NOpThread
from heapq import heappush, heappop
from multiprocessing import Process
from NUtils import NLiteralMap
import Queue
import copy

##########################################
## class:   NSelectOp
## desc:
class NSelectOpThread(NOpThread):
    def __init__(self, fields, threadCnt=0):
        NOpThread.__init__(self, threadCnt)
        self.fields = copy.deepcopy(fields)
        self.saveData = []
        self.idx = 0

    def doProcess(self, data):
        selectData = {}
        for field in self.fields:
            selectData[field] = data[field]
        return selectData

    def process(self, data):
        selectData = self.doProcess(data)
        if not selectData:
            return
        if not isinstance(selectData, list):
            selectData = [selectData]

        if self.child:
            for d in selectData:
                self.saveData.append(d)
                for op in self.child:
                    op.process(d)
        elif self.next:
            for d in selectData:
                self.next.dispatch(d)

    def processEnd(self):
        if self.next:
            self.childPostProcessThread(self.child, 0, self.saveData)
            self.saveData = []
            self.next.processEnd()
     
    def doDispatch(self, data):
        self.queues[self.idx % len(self.queues)].put(data)
        self.idx += 1
            
class NProcessOpThread(NSelectOpThread):
    def __init__(self, callback, userData, threadCnt=0):
        NSelectOpThread.__init__(self, None, threadCnt)
        self.callback = callback
        self.userData = userData

    def doProcess(self, data):
        processData = self.callback(data, self.userData)
        return processData

            
class NGroupOpThread(NOpThread):
    def __init__(self, fields, threadCnt=4):
        NOpThread.__init__(self, threadCnt)
        self.fields = list(fields)
        self.groupData = {}

    def process(self, data):
        groupData = self.groupData
        for field in self.fields:
            if data[field] not in groupData:
                groupData[data[field]] = {}
            groupData = groupData[data[field]]
        if '__data__' not in groupData:
            groupData['__data__'] = data
            groupData['__child__'] = copy.deepcopy(self.child)
        for op in groupData['__child__']:
            op.process(data)

    def processRecurse(self, groupData, idx):
        if idx == len(self.fields):
            self.childPostProcessThread(groupData['__child__'], 0, groupData['__data__'])
        else:
            for field in groupData:
                self.processRecurse(groupData[field], idx + 1)
 
    def processEnd(self):
        if self.next and self.groupData:
            self.processRecurse(self.groupData, 0)
            self.groupData = {}
            self.next.processEnd()

    def doDispatch(self, data):
            idx = 0
            field = data[self.fields[0]]
            if isinstance(field, str):
                for i in range(min(len(field), 5)):
                    idx += ord(field[i])
            else:
                idx = int(field)
            self.queues[idx % len(self.queues)].put(data)

            
class NGroupOpThread_1(NOpThread):
    def __init__(self, fields, threadCnt=4):
        NOpThread.__init__(self, threadCnt)
        self.fields = list(fields)
        self.groupData = {}

    def process(self, data):
        key = data[self.fields[0]]
        value = self.groupData[key]
        if not value:
            value = {}
            self.groupData[key] = value
        if '__data__' not in value:
            value['__data__'] = data
            value['__child__'] = copy.deepcopy(self.child)
        for op in value['__child__']:
            op.process(data)

    def processEnd(self):
        if self.next and self.groupData:
            for i in self.groupData:
                self.childPostProcessThread(i['__child__'], 0, i['__data__'])
            self.groupData = None
            self.next.processEnd()

    def doDispatch(self, data):
            idx = 0
            field = data[self.fields[0]]
            if isinstance(field, str):
                for i in range(min(len(field), 5)):
                    idx += ord(field[i])
            else:
                idx = int(field)
            self.queues[idx % len(self.queues)].put(data)


##########################################
## class:   NCreataFileOp
## desc:
class NCreateFileOpThread(NOp):
    def __init__(self, filename, fields):
        NOp.__init__(self)
        self.fields = list(fields)
        self.fileStream = open(filename, 'w')
        self.q = None
        
    def run(self, q):
        while True:
            data = q.get()
            if not data:
                break
            self.process(data)
        self.processEnd()


    def preProcess(self):
        self.q = Queue.Queue()
        self.p = Process(target=NCreateFileOpThread.run, args=(self, self.q))
        self.p.start()
    
    def process(self, data):
        itemArr = []
        for field in self.fields:
            itemArr.append(str(data[field]))
        self.fileStream.write("%s\n" % ("\t".join(itemArr)))

        
    def processEnd(self):
        self.fileStream.close()

    def postProcess(self):
        self.p.join()

    def dispatch(self, data):
        self.q.put(data)


##########################################
## class:   NCreataFileOp
## desc:
class NDBInsertOpThread(NOpThread):
    def __init__(self, sqlDB, tableName, fields, threadCnt):
        NOpThread.__init__(self, threadCnt)
        self.fields = list(fields)
        self.sqlDB = sqlDB
        self.tableName = tableName
        self.idx = 0

    def process(self, data):
        self.sqlDB.insertCache(self.tableName, data, self.fields)

    def processEnd(self):
        self.sqlDB.insertFlush(self.tableName)

    def doDispatch(self, data):
        self.queues[self.idx % len(self.queues)].put(data)
        self.idx += 1

