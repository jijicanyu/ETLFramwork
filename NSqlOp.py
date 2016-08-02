#!/usr/bin/env python
# coding=gbk
import os
import re
import subprocess
import pipes

from NOp import NOp
from NOp import NOpChild
from DBConf import DBConf
from NSqlDB import NSqlDB


class NSqlSelectOp(NOp):
    def __init__(self, fields, asField):
        NOp.__init__(self)
        self.fields = list(fields)
        self.asField = asField

    def process(self, data):
        sqlWhereOp = None
        sqlGroupOp = None
        sqlHavingOp = None
        sqlLimitOp = None
        sqlOrderOp = None
        for op in self.child:
            if isinstance(op, NSqlGroupOp):
                sqlGroupOp = op
            elif isinstance(op, NSqlWhereOp):
                sqlWhereOp = op
            elif isinstance(op, NSqlHavingOp):
                sqlHavingOp = op
            elif isinstance(op, NSqlOrderOp):
                sqlOrderOp = op
            elif isinstance(op, NSqlLimitOp):
                sqlLimitOp = op
            else:
                self.fields.append(op.process(data))
        sqlStat = "SELECT " + ','.join(self.fields)
        sqlStat += " FROM " + data
        if sqlWhereOp:
            sqlStat += " " + sqlWhereOp.process(data)
        if sqlGroupOp:
            sqlStat += " " + sqlGroupOp.process(data)
        if sqlHavingOp:
            sqlStat += " " + sqlHavingOp.process(data)
        if sqlOrderOp:
            sqlStat += " " + sqlOrderOp.process(data)
        if sqlLimitOp:
            sqlStat += " " + sqlLimitOp.process(data)
        if self.asField:
            sqlStat = "(" + sqlStat + ") AS " + self.asField 
        if self.next:
            self.next.process(sqlStat)

class NSqlWhereOp(NOpChild):
    def __init__(self, whereStat):
        NOpChild.__init__(self)
        self.whereStat = whereStat

    def process(self, data):
        return "WHERE " + self.whereStat

class NSqlGroupOp(NOpChild):
    def __init__(self, fields):
        NOpChild.__init__(self)
        self.fields = list(fields)

    def process(self, data):
        return "GROUP BY " + ','.join(self.fields)
       
class NSqlEachOp(NOpChild):
    def __init__(self, statement, asField):
        NOpChild.__init__(self, asField)
        self.statement = statement

    def process(self, data):
        if self.asField:
            asField = "`" + self.asField.strip("`") +"`"
        else:
            asField = ""
            
        return self.statement + (self.asField and " AS " + asField or "")
        
class NSqlHavingOp(NOpChild):
    def __init__(self, havingStat):
        NOpChild.__init__(self)
        self.havingStat = havingStat

    def process(self, data):
        return "HAVING " + self.havingStat

    
class NSqlOrderOp(NOpChild):
    def __init__(self, statment):
        NOpChild.__init__(self)
        self.orderStat = statment

    def process(self, data):
        return "ORDER BY " + ','.join(self.orderStat)

class NSqlLimitOp(NOpChild):
    def __init__(self, statment):
        NOpChild.__init__(self)
        self.LimitStat = statment

    def process(self, data):
        return "LIMIT " + self.LimitStat

class NSqlCreateFileOp(NOp):
    def __init__(self, db, filename, charset):
        NOp.__init__(self)
        from DBConf import DBConf
        self.db = db
        self.filename = os.path.abspath(filename)
        host = DBConf[self.db]['host']
        user = DBConf[self.db]['user']
        passwd = DBConf[self.db]['passwd']
        port = DBConf[self.db].get("port",3306)
        db = DBConf[self.db]['db']
        local_infile = DBConf[self.db].get('local_infile',0)
        ## where host field is cmd mode, get return of cmd as host
        host_cmd_rule = re.compile("^\$\{.*\}$")
        find_str = host_cmd_rule.findall(host.strip())
        ## found the cmd
        if len(find_str) == 1:
            host_cmd = find_str[0].strip("$").strip("{").strip("}")
            p = subprocess.Popen(host_cmd,shell=True,stdout = subprocess.PIPE)
            (stdout,stderr) = p.communicate()
            returncode = p.returncode
            if 0 != returncode:
                raise Exception("EXEC host cmd Failed")
            host = stdout.strip()

        # process MACRO in sql
        from NSqlDB import NSqlDB 
        from NSqlUtils import NSqlUtils
        self.connect = NSqlDB(self.db)
        self.cc = NSqlUtils(self.connect.cursor)
        self.cmd = "mysql  --default-character-set=%s --quick --skip-column  -h%s -u%s -p%s -P%s %s --local-infile=%s -N"%(charset, host,user, passwd, port, db, local_infile)
    def process(self, data):
        if (self.connect.schemaDetect):
            data = self.cc.process(data)
        cmd = '%s -e %s > %s' %(self.cmd, pipes.quote(data), pipes.quote(self.filename))
        #print cmd
        res = os.system(cmd)
        if res:
            raise Exception('dump table failed: %s' %(cmd))
   
    def value(self):
        return self.filename

class NSqlCreateDBTableOp(NOp):
    def __init__(self, fromDB, toDB, tableName, fields, fieldType,
            keyFields=None, partition=None, indexList=None, 
            overwrite=True, createCols=False, fieldComment=None, charset='gbk'):
        NOp.__init__(self)
        self.fromDB = fromDB
        self.toDB = toDB
        self.tableName = tableName
        self.fields = fields
        self.fieldType = fieldType
        self.keyFields = keyFields
        self.partition=partition
        self.indexList = indexList
        self.overwrite = overwrite
        self.createCols = createCols
        self.fieldComment = fieldComment
        self.charset = charset
        
        if(self.createCols and self.isTableExists()):
            self.autoCreateCols()
    
    def isTableExists(self):
        toSqlDB = NSqlDB(self.toDB)
        toSqlDB.execute("SHOW TABLES LIKE '%s'" %(self.tableName))
        tb = toSqlDB.fetchone()
        return tb and tb[0]

    def autoCreateCols(self):
        # auto add colums in program, 
        if self.createCols:
            toSqlDB = NSqlDB(self.toDB)
            toSqlDB.useDictCursor()
            toSqlDB.execute("SHOW FULL COLUMNS FROM %s" %(self.tableName))
            columns_attr = toSqlDB.fetchall()
            for field in self.fields:
                if field in [ca['Field'] for ca in columns_attr]:
                    continue
                else:
                    SQL="ALTER TABLE %s ADD COLUMN %s %s" %(self.tableName, field,
                            self.fieldType[field])
                    toSqlDB.execute(SQL)
            toSqlDB.close()
    def process(self, data):
        sqlDB = NSqlDB(self.fromDB)
        #if DBConf[self.fromDB]['host'] == DBConf[self.toDB]['host']:
        if False:
            dbFields = []
            for field in self.fields:
                dbFields.append(field + " " + self.fieldType[field])
            if self.keyFields:
                dbFields.append("PRIMARY KEY(" + ','.join(self.keyFields) + ")")
            if self.indexList:
                for index in self.indexList:
                    dbFields.append("INDEX(" + ','.join(index) + ")")
            if self.overwrite:
                sqlDB.execute("DROP TABLE IF EXISTS %s.%s"%(DBConf[self.toDB]['db'], self.tableName))
            statement = "CREATE TABLE IF NOT EXISTS %s.%s (%s) AS %s" % (DBConf[self.toDB]['db'], self.tableName, ','.join(dbFields), data)
            sqlDB.execute(statement)
        else:
            toSqlDB = NSqlDB(self.toDB)
            toSqlDB.createTable(self.tableName, self.fields, self.fieldType, self.overwrite,
                    self.keyFields, partition=self.partition, indexList=self.indexList,
                    fieldComment = self.fieldComment, charset = self.charset)
            sqlDB.useDictCursor()
            sqlDB.execute(data)
            while True:
                row = sqlDB.fetchone()
                if not row:
                    break
                toSqlDB.insertCache(self.tableName, row, self.fields)
            toSqlDB.insertFlush(self.tableName, self.fields)
    
class NSqlUpdateDBTableOp(NOp):
    def __init__(self, fromDB, toDB, tableName, fields, fieldType, updateFields,
            keyFields=None, createDB=True, createCols=False, partition=None, indexList=None,
            fieldComment=None, charset='gbk'):
        NOp.__init__(self)
        self.fromDB = fromDB
        self.toDB = toDB
        self.tableName = tableName
        self.fields = list(fields)
        self.fieldType = fieldType
        self.updateFields = list(updateFields)
        self.keyFields = keyFields
        self.createDB = createDB
        self.createCols = createCols
        self.partition = partition
        self.indexList = indexList
        self.fieldComment = fieldComment
        self.charset = charset
 
        if(self.createCols and self.isTableExists()):
            self.autoCreateCols()
    
    def isTableExists(self):
        toSqlDB = NSqlDB(self.toDB)
        toSqlDB.execute("SHOW TABLES LIKE '%s'" %(self.tableName))
        tb = toSqlDB.fetchone()
        return tb and tb[0]
    
    def autoCreateCols(self):
        # auto add colums in program, 
        if self.createCols:
            toSqlDB = NSqlDB(self.toDB)
            toSqlDB.useDictCursor()
            toSqlDB.execute("SHOW FULL COLUMNS FROM %s" %(self.tableName))
            columns_attr = toSqlDB.fetchall()
            for field in self.updateFields + self.keyFields:
                if field in [ca['Field'] for ca in columns_attr]:
                    continue
                else:
                    if field in self.fieldComment.keys():
                        comment = self.fieldComment[field]
                    else:
                        comment = ""
                    SQL="ALTER TABLE %s ADD COLUMN %s %s COMMENT '%s'" % (self.tableName, field,
                            self.fieldType[field], toSqlDB.escape_string(comment))
                    toSqlDB.execute(SQL)
            toSqlDB.close()

    def process(self, data):
        fromSqlDB = NSqlDB(self.fromDB)
        toSqlDB = NSqlDB(self.toDB)
        if self.createDB:
            toSqlDB.createTable(self.tableName, self.fields, self.fieldType, False, self.keyFields,
                    partition=self.partition, indexList=self.indexList, 
                    fieldComment=self.fieldComment, charset = self.charset)
        fromSqlDB.useDictCursor()
        fromSqlDB.execute(data)
        while True:
            row = fromSqlDB.fetchone()
            if not row:
                break
            toSqlDB.insertUpdateCache(self.tableName, row, self.fields, self.updateFields)
        toSqlDB.insertUpdateFlush(self.tableName, self.updateFields, self.fields)

                    
class NSqlCreateDBViewOp(NOp):
    def __init__(self, db, viewName):
        NOp.__init__(self)
        self.db = db
        self.viewName = viewName

    def process(self, data):
        statement = "CREATE OR REPLACE VIEW " + self.viewName + " AS " + data
        NSqlDB(self.db).execute(statement)
 
class NSqlCreateDataTableOp(NOp):
    def __init__(self, db):
        NOp.__init__(self)
        self.db = db
        self.table = []
    def process(self, data):
        sqlDB = NSqlDB(self.db)
        sqlDB.useDictCursor()
        sqlDB.execute(data)
        self.table = sqlDB.fetchall()
        
    def value(self):
        return self.table

class NSqlCreateSQLOp(NOp):
    def __init__(self):
        NOp.__init__(self)
        self.sql = ""
    def process(self, data):
        self.sql = data
    def value(self):
        return self.sql

class NSqlCreateDictOp(NOp):
    def __init__(self, db, keyField, valueField):
        NOp.__init__(self)
        self.db = db
        self.keyField = keyField
        self.valueField = valueField
        self.dict = {}
    
    def process(self, data):
        sqlDB = NSqlDB(self.db)
        sqlDB.useDictCursor()
        sqlDB.execute(data)
        while True:
            row = sqlDB.fetchone()
            if not row:
                break
            self.dict[row[self.keyField]] = row[self.valueField]

    def value(self):
        return self.dict
 
class NSqlCreateMultiDictOp(NOp):
    def __init__(self, db, keyField, valueFields):
        NOp.__init__(self)
        self.db = db
        self.keyField = keyField
        self.valueFields = valueFields
        self.dict = {}

    def process(self, data):
        sqlDB = NSqlDB(self.db)
        sqlDB.useDictCursor()
        sqlDB.execute(data)
        while True:
            row = sqlDB.fetchone()
            if not row:
                break
            valueData = {}
            for field in self.valueFields:
                valueData[field] = row[field]
            self.dict[row[self.keyField]] = valueData

    def value(self):
        return self.dict

class NSqlDumpOp(NOp):
    def __init__(self, DBConf, filename, charset):
        NOp.__init__(self)
        self.filename = os.path.abspath(filename)
        host = DBConf['host']
        user = DBConf['user']
        passwd = DBConf['passwd']
        port = DBConf.get("port",3306)
        db = DBConf['db']
        local_infile = DBConf.get('local_infile',0)

        ## where host field is cmd mode, get return of cmd as host
        host_cmd_rule = re.compile("^\$\{.*\}$")
        find_str = host_cmd_rule.findall(host.strip())
        ## found the cmd
        if len(find_str) == 1:
            host_cmd = find_str[0].strip("$").strip("{").strip("}")
            p = subprocess.Popen(host_cmd,shell=True,stdout = subprocess.PIPE)
            (stdout,stderr) = p.communicate()
            returncode = p.returncode
            if 0 != returncode:
                raise Exception("EXEC host cmd Failed")
            host = stdout.strip()

        self.cmd = "mysql --default-character-set=%s --quick --skip-column  -h%s -u%s -p%s -P%s %s --local-infile=%s -N"%(charset, host,user, passwd, port, db, local_infile)

    def process(self, data):
        #data = MySQLdb.escape_string(data)
        cmd = '%s -e %s > %s' %(self.cmd, pipes.quote(data), pipes.quote(self.filename))
        res = os.system(cmd)
        if res:
            raise Exception('dump table failed: %s' %(cmd))

    def value(self):
        return self.filename


class NSqlDropTableOp(NOp):
    def __init__(self, db, tablename):
        self.db = db
        self.tablename = tablename

    def process(self, data):
        sqlDB = NSqlDB(self.db)
        sql = "DROP TABLE IF EXISTS %s" %(self.tablename)
        sqlDB.execute(sql)
        sqlDB.close()

class NSqlExecSQLOp(NOp):
    def __init__(self, db, sql):
        self.db = db
        self.sql = sql

    def process(self, data):
        sqlDB = NSqlDB(self.db)
        affectedNum = sqlDB.execute(self.sql)
        sqlDB.close()

