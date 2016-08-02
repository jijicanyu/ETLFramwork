#!/usr/bin/env python
# coding=gbk

from NOpThread import NOpMgrThread
from NOp import NOpMgr
from NTaskEnv import NTaskEnv
import copy
import re
import weakref
from NLog import NLogger
import datetime
import random

class NInput(object):
    taskEnv = NTaskEnv()
    taskIOType= None
    refCnt = 0
    def __init__(self):
        self.fieldIdx = {}
        self.fieldType = {}
        self.fields = []
        self.fieldComment = {}
        self.opMgr = NOpMgr()
        # first enter 
        if (NInput.taskIOType!="output" and  NInput.refCnt==0) or NInput.taskIOType==None:
            NInput.taskIOType="input"
        self.registerTask()
        NInput.refCnt+=1
# NLogger.debug( "add: %s, %s"%(NInput.refCnt, self))
    def clear(self):
        self.fieldIdx = {}
        self.fieldType = {}
        self.fields = []
        self.fieldComment= {}
        self.opMgr = NOpMgr()
    
    def addFieldComment(self, field, Comment):
        """
        @Desc: add field Comment
        @Author: xiangyuanfei
        """
        self.fieldComment[field] = Comment
        return self

    def preProcess(self):
        self.opMgr.preProcess()
    
    def postProcess(self):
        self.opMgr.postProcess()
    
    def select(self, fields):
        has_star = False
        for field in fields:
            if field == "*":
                has_star = True
            else:
                self.fields.append(field)
        if not has_star:
            self.fields = fields
        return self
    
    def group(self, fields):
        return self

    def assign(self, fields, overwrite=False):
        """
        modify column names, type and comment
        if overwrite=True and column already exists, no new columns would be added.
        """
        for idx in fields:
            self.fieldIdx[fields[idx][0]] = idx
            self.fieldType[fields[idx][0]] = fields[idx][1]
            # field comment   
            if len(fields[idx]) >2:
                self.fieldComment[fields[idx][0]] = fields[idx][2]
            else:
                self.fieldComment[fields[idx][0]] = None
            if not overwrite or fields[idx][0] not in self.fields:
                self.fields.append(fields[idx][0])
        return self
       
    def sum(self, field, asField, type):
        self.fields.append(asField)
        self.fieldType[asField] = type
        return self
    
    def count(self, asField, type):
        self.fields.append(asField)
        self.fieldType[asField] = type
        return self

    def min(self, field, asField, type):
        self.fields.append(asField)
        self.fieldType[asField] = type
        return self

    def average(self, field, asField, type):
        self.fields.append(asField)
        self.fieldType[asField] = type
        return self

    def each(self, statement, asField, type, comment=None):
        """
        @Desc: each function
        @Author: xiangyuanfei
        """
        self.fields.append(asField)
        self.fieldType[asField] = type
        self.fieldComment[asField] = comment
        return self

    def where(self, whereStat):
        return self
    
    def alterSchemaForIB(self):
        # remove unsigned
        p = re.compile("(unsigned|zerofill|tiny|small|medium)")
        for field in self.fields:
            oldtype = self.fieldType[field]
            self.fieldType[field] = p.sub("", oldtype)
        return self
    
    def alterSchemaForHive(self):
        # change varchar to string
        p = re.compile("(varchar|char|longtext|tinytext|mediumtext|text)")
        for field in self.fields:
            oldtype = self.fieldType[field]
            self.fieldType[field] = p.sub("string", oldtype)
        # remove '(*)' like
        p = re.compile("(\(.*\))")
        for field in self.fields:
            oldtype = self.fieldType[field]
            self.fieldType[field] = p.sub("", oldtype)
        # change unsigned int to bigint
        p = re.compile("(unsigned)")
        for field in self.fields:
            oldtype = self.fieldType[field]
            if p.search(oldtype):
                self.fieldType[field] = "bigint"
        # change date int to string
        p = re.compile("(datetime|date|timestamp|enum|time)")
        for field in self.fields:
            oldtype = self.fieldType[field]
            if p.search(oldtype):
                self.fieldType[field] = "string"
        # change decimal to double
        p = re.compile("(decimal|Decimal)")
        for field in self.fields:
            oldtype = self.fieldType[field]
            if p.search(oldtype):
                self.fieldType[field] = "double"
        # change bit to int
        p = re.compile("(bit)")
        for field in self.fields:
            oldtype = self.fieldType[field]
            if p.search(oldtype):
                self.fieldType[field] = "int"
        return self

    def alterSchemaForMysql(self):
        # change string to varchar, default varchar(50)
        p = re.compile("string")
        for field in self.fields:
            oldtype = self.fieldType[field]
            self.fieldType[field] = p.sub("varchar(50)", oldtype)
        return  self

    def clone(self):
        return copy.deepcopy(self)

    def renameField(self, field, asField, type=None):
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
        self.fields = copy.deepcopy(newField)
        return self

    def setTaskIOType(self, iotype):
        NInput.taskIOType = iotype

    def getTaskIOType(self):
        return NInput.taskIOType

    def registerTask(self):
        pass

    def split_type(self, text):
        """
        extract type and alternative argument separated by comma
        though commas appeared in (), "" and '' are ignored.
        """
        pattern = re.compile(r'^(([^,\'"(]|\(.*?\)|".*?"|\'.*?\')*),.*$')
        m = pattern.match(text)
        if m:
            l = len(m.group(1))
            return text[:l], text[l + 1:]
        else:
            return text, ''

    def mkTmpFileName(self):
        """
            tmp filename format: file_${scriptName}_${datetimeParameter}_pid_curTime_randInt
        """
        filename = "file_%s_%s_%s_%s_%s" % (self.taskEnv.scriptName, self.taskEnv.execParam, \
            self.taskEnv.getRuntimePid(), datetime.datetime.now().strftime("%Y%m%d%H%M%S%f"), \
            random.randrange(1, 10000)) 
        return filename


    def __del__(self):
        NInput.refCnt-=1
#    NLogger.debug( "del: %s, %s"%(NInput.refCnt, self))
if __name__=="__main__":
    a = NInput()
