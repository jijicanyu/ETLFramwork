#!/usr/bin/env python
# coding=gbk
import re
##########################################
## class:   CHECKCOL
## desc:
# $CHECKCOL(field) 
# 该方法目前不支持子查询， join 等功能
class CHECK_COL(object):
    def __init__(self, col):
        self.col = col
    def process(self, sql, func_str, cursor):
        self.cursor = cursor
        self.tableName = self.getTableName(sql)
        self.sql = sql.replace(func_str, self.isColExists())
    def postProcess(self):
        return self.sql

    def isColExists(self):
        sql ="show columns from %s where field = '%s' " %(self.tableName,self.col.strip("'"))
        self.cursor.execute(sql)
        res = self.cursor.fetchone()
        if (res): return str(1)
        else: return str(0)

    def getTableName(self,sql):
        return re.findall("(?<=FROM) \w+", sql, re.I)[0].strip(" ")

##########################################
## class:   CHECK_TAB_COL
## desc:
# $CHECK_TAB_COL(field, table) 
# 该方法目前不支持子查询， join 等功能
class CHECK_TAB_COL(object):
    def __init__(self, col,tab):
        self.col = col
        self.tableName = tab
    def process(self, sql, func_str, cursor):
        self.cursor = cursor
        self.sql = sql.replace(func_str, self.isColExists())
    def postProcess(self):
        return self.sql

    def isColExists(self):
        sql ="show columns from %s where field = '%s' " %(self.tableName,self.col.strip("'"))
        self.cursor.execute(sql)
        res = self.cursor.fetchone()
        if (res): return str(1)
        else: return str(0)

    def getTableName(self,sql):
        return  self.tabelName

##########################################
## class:   IF
## desc:
# $IF(cond1, res1, res2), like  if function in sql
# if cond1 True, return res1, else return res2
class IF(object):
    def __init__(self, cond, res1, res2):
        self.cond=cond
        self.res1 = str(res1)
        self.res2 = str(res2)
    def process(self, sql, func_str, cursor):
        if(int(self.cond)):
            self.sql = sql.replace(func_str, self.res1)
        else:
            self.sql = sql.replace(func_str, self.res2)
    def postProcess(self):
        return self.sql

class  NSqlUtils(object):
    def __init__(self, cursor):
        self.cursor = cursor
# self.rule = re.compile("\$\w+(\([\w,' =\(\)]+?\))")
        # re not support balance group, so not used
        #rule =re.compile("\([^\(\)]*(((?'Open'\()[^\(\)]*)+((?'-Open'\))[^\(\)]*)+)*(?('Open')(?!))\)")   
       # re.sub('([\(\s]+?)([^\(\s].*?$)' , lambda m: '('  + re.sub('(\)\s*){'+str(m.group(1).count('(')) + '}$' , ')' , m.group(2)) , test)
        """
        re.sub("(\$\w+)([\(\w, '=]+?)([^\(].+?$)" , lambda m:  (m.group(2).count('(')>1 and '(' or
                            "")  + re.sub('(\$\w+)(\)\s*){' +str(m.group(2).count('(')) + '}$' ,
                                (m.group(2).count('(')>1 and  ')' or "") , m.group(3)) , a1)
        """
    def process(self, sql):
        while True:
                repr_str = self.getFunc_str(sql)
                if repr_str==-1: return sql
                sql = self.dispatch(repr_str, sql)
    def dispatch(self, func_str,sql):
        processOp = self.getFuncAndParam(func_str)
        processOp.process(sql, func_str, self.cursor)
        return processOp.postProcess()        

    def getFuncAndParam(self, func_str):
        func_name = re.findall("\$\w+", func_str)[0].strip("$")
        argslist = self.getParamTuple(func_str)
        return eval(func_name+argslist)
    
    def getParamTuple(self, func_str):
        idx=func_str.find("(")
        raw = func_str[idx+1:-1]
        heap=[]
        count=0
        al=[]
        for s in raw.split(","):
            count+=s.count("(")
            count-=s.count(")")
            heap.append(s)
            if count==0:
                al.append(",".join(heap))
                heap=[]
        return "(\""+"\",\"".join(al)+"\")"

    def getFunc_str(self, sql):
        raw = sql
        if not re.findall("\$[A-Z]+\([\S ]+\)", raw): return -1
        idx = raw.rfind("$")
        if idx==-1: return -1
        count = 0
        func_str=""
        for u in raw[idx:]:
            func_str+=u
            count+=u.count("(")
            if count==0: continue
            count-=u.count(")")
            if count==0:
                return func_str
        
        
