#!/usr/bin/env python
# coding=gbk

from NInputData import NInputData

class NInputMulti(NInputData):
    def __init__(self, *inputDataList):
        NInputData.__init__(self, None)
        self.inputDataList = []
        for inputData in inputDataList:
            self.setInputData(inputData)
        self.debug = 0

    def setDebug(self):
        self.debug = 1
    
    def iterator(self, opMgr):
        cnt = 0
        for inputData in self.inputDataList:
            cnt = inputData.iterator(opMgr)
            
    
    def setInputData(self, inputData):
        self.inputDataList.append(inputData)
        for field in inputData.fields:
            if field not in self.fields:
                self.fields.append(field)
        for field in inputData.fieldType:
            self.fieldType[field] = inputData.fieldType[field]
        
