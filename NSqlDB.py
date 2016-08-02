#!/usr/bin/env python
# coding=gbk

import MySQLdb
from DBConf import DBConf
from NLog import NLogger
from NConfParser import NConfParser
import re
from NSqlUtils import NSqlUtils
import subprocess

class NSqlDB:
    def __init__(self, dbName):
        self.host = DBConf[dbName]["host"]
        self.user = DBConf[dbName]["user"]
        self.passwd = DBConf[dbName]["passwd"]
        self.db = DBConf[dbName]["db"]
        self.local_infile = DBConf[dbName]["local_infile"]
        self.high_performance = DBConf[dbName].get("high_performance", 0)
        self.port = DBConf[dbName].get("port", 0)
        self.charset = DBConf[dbName]["charset"] or 'gbk'
        np = NConfParser()
        self.schemaDetect = np.get("NSqlDB","schemaDetect","BOOLEAN")

        ## where host field is cmd mode, get return of cmd as host
        host_cmd_rule = re.compile("^\$\{.*\}$")
        find_str = host_cmd_rule.findall(self.host.strip())
        ## found the cmd
        if len(find_str) == 1:
            host_cmd = find_str[0].strip("$").strip("{").strip("}")
            p = subprocess.Popen(host_cmd,shell=True,stdout = subprocess.PIPE)
            (stdout,stderr) = p.communicate()
            returncode = p.returncode
            if 0 != returncode:
                raise Exception("EXEC host cmd Failed")
            self.host = stdout.strip()
            
        if self.port:
            self.conn = MySQLdb.connect(
                host = self.host,
                user = self.user,
                passwd = self.passwd,
                db = self.db,
                port = self.port,
                use_unicode = False,
                charset = self.charset,
                local_infile = self.local_infile)
        else:
            self.conn = MySQLdb.connect(
                host = self.host,
                user = self.user,
                passwd = self.passwd,
                db = self.db,
                use_unicode = False,
                charset = self.charset,
                local_infile = self.local_infile)
        self.conn.autocommit(1)
        self.cursor = self.conn.cursor()
        if self.high_performance:
            self.execute("set session max_heap_table_size = %d"%(3 * 1024 * 1024 * 1024))
            self.execute("set session sort_buffer_size = %d"%(3 * 1024 * 1024 * 1024))
            self.execute("set session tmp_table_size = %d"%(3 * 1024 * 1024 * 1024))
 
        self.selectFields = []
        self.groupFields = []
        self.eachFields = []
        self.insertCacheArr = []
    
    def close(self):
        self.cursor.close()
        self.conn.close()
    
    def select(self, fields):
        self.selectFields = fields

    def group(self, fields):
        self.groupFields = fields

    def each(self, fields):
        self.eachFields = fields
 
    def createTable(self, tableName, fields, fieldsType, overwrite = True, keyFields=None,
            engine='MyIsam', charset='gbk', partition=None, indexList=None, fieldComment=None):
        fieldType = []
        for field in fields:
            comment = ""
            if(fieldComment):
                if fieldComment.has_key(field) and fieldComment[field]!=None:
                    comment = self.escape_string(fieldComment[field])
                else:
                    comment = "NULL"
            fieldType.append(field + " " + fieldsType[field] + " COMMENT '%s'" %(comment) )
        if partition:
            for p_k in partition.keys():
                if p_k not in fields:
                    # partition key 的类型 默认用varchar(50),非最优方法
                    fieldType.append(p_k + " "+ "varchar(50)")
        if keyFields:
            fieldType.append("PRIMARY KEY(" + ','.join(keyFields) + ")")
        if indexList:
            for index in indexList:
                fieldType.append("INDEX(" + ','.join(index) + ")")
        if overwrite:
            self.execute("DROP TABLE IF EXISTS %s" % (tableName))
        #print "CREATE TABLE IF NOT EXISTS %s(%s) ENGINE=%s DEFAULT CHARSET=%s" % (tableName, ','.join(fieldType),engine, charset)
        if not self.isTableExists(tableName):
            self.execute("CREATE TABLE IF NOT EXISTS %s(%s) ENGINE=%s DEFAULT CHARSET=%s" % \
                    (tableName, ','.join(fieldType), engine, charset))
        if  partition:
            partition_sql = " and ".join([ str(k)+"='"+str(v)+"'"   for k, v in partition.items() ])
            sql = "DELETE FROM %s WHERE %s" % (tableName, partition_sql)
            NLogger.info(sql)
            self.execute(sql)
    
    def isTableExists(self, tableName):
        """
        @Desc: check table if exist
        @Author: xiangyuanfei
        """
        sql = "SHOW TABLES LIKE '%s'" % (tableName)
        self.execute(sql)
        data = self.fetchall()
        if data and data[0]:
            return True
        return False
        
    def loadData(self, tableName, fields, fieldsType, filepath, overwrite = True, local = False,
            keyFields=None, engine='MyIsam', charset='gbk', delim="\t", partition=None,
            indexList=None, fieldComment=None):
         
        self.createTable(tableName, fields, fieldsType, overwrite, keyFields, engine, charset,
                 partition, indexList=indexList, fieldComment=fieldComment )
        partition_sql=""
        if partition:
            partition_list = []
            for p_k in partition.keys():
                    if p_k not in fields:
                        partition_list.append(p_k+ "=\'"+ partition[p_k]+"\'")
            if  partition_list:
                partition_sql= "SET %s" %(",".join(partition_list))
        if local:
             local = 'LOCAL'
        else:
             local = ''
        order = "LOAD DATA %s INFILE '%s' REPLACE INTO TABLE %s CHARACTER SET %s \
            FIELDS TERMINATED BY '%s' ENCLOSED BY '\r' escaped by '' %s %s" \
            % (local, filepath, tableName, charset, delim, \
                    fields and ("(" + ",".join(fields)+")") or "", partition_sql)
        NLogger.info(order)
        self.execute(order)

    def loadDataIB(self, tableName, fields, fieldsType, filepath, overwrite = True, local=False,
            keyFields=None, engine='MyIsam', charset='gbk', delim="\t", partition=None,
            indexList=None, fieldComment=None):
         if not local:
                order = "LOAD DATA  INFILE '%s' INTO TABLE %s %s CHARACTER SET %s fields terminated by '%s' enclosed by 'NULL' escaped by ''" % (filepath, tableName, fields and ( ",".join(fields)) or "", charset,delim)
         else:
                order = "LOAD DATA local  INFILE '%s' INTO TABLE %s %s  CHARACTER SET %s fields terminated by '%s' enclosed by 'NULL' escaped by ''" % (filepath, tableName, fields and  ",".join(fields) or "", charset, delim)
         NLogger.info(order)
         self.execute(order)
         self.execute("commit")


    def insert(self, tableName, items, fields):
        insertArr = []
        for field in fields:
            insertArr.append("'" + self.escape_string(str(items[field])) + "'")
        sql = "INSERT INTO %s(%s) VALUES(%s)" % (tableName, ','.join(fields), ','.join(insertArr))
        self.execute(sql)

    def insertCache(self, tableName, items, fields):
        insertArr = []
        for field in fields:
            insertArr.append("'" + self.escape_string(str(items[field])) + "'")
        self.insertCacheArr.append("(%s)"%(','.join(insertArr)))

        if len(self.insertCacheArr) > 1000:
            self.insertFlush(tableName, fields)

    def insertFlush(self, tableName, fields=None):
        if self.insertCacheArr:
            fieldsStr = ""
            if fields:
                fieldsStr = "(%s)"%(','.join(fields))
            sql = "INSERT INTO %s %s VALUES %s" % (tableName, fieldsStr,','.join(self.insertCacheArr))
            self.execute(sql)
            self.insertCacheArr = []
    
    def insertUpdateCache(self, tableName, items, fields, updateFields):
        insertArr = []
        for field in fields:
            if items[field]!=None:
                insertArr.append("'" + self.escape_string(str(items[field])) + "'")
            else:
                insertArr.append('NULL')
        self.insertCacheArr.append("(%s)"%(','.join(insertArr)))

        if len(self.insertCacheArr) > 1000:
            self.insertUpdateFlush(tableName, updateFields, fields)

    def insertUpdateFlush(self, tableName, updateFields, fields=None):
        updateStmtArr = []
        for field in updateFields:
            updateStmtArr.append("`" + field + "`=VALUES(`" + field + "`)")
        fieldsStr = ""
        if fields:
            fieldsStr = "(`%s`)"%('`,`'.join(fields))
        if self.insertCacheArr:
            sql="""
                INSERT INTO %s %s VALUES %s ON DUPLICATE KEY UPDATE %s
            """%(tableName, fieldsStr, ','.join(self.insertCacheArr), ','.join(updateStmtArr))
            self.execute(sql)
            self.insertCacheArr = []
            
    def execute(self, statement):
        NLogger.debug(statement)
        if self.schemaDetect:
            cc = NSqlUtils(self.cursor)
            statement = cc.process(statement)
        #print statement
        return self.cursor.execute(statement)
    
    def dropTable(self, tableName):
        self.execute("DROP TABLE IF EXISTS %s"%(tableName))
    
    def useDictCursor(self):
        self.cursor.close()
        self.cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)
        if self.high_performance:
            self.execute("set session max_heap_table_size = %d"%(3 * 1024 * 1024 * 1024))
            self.execute("set session sort_buffer_size = %d"%(3 * 1024 * 1024 * 1024))
            self.execute("set session tmp_table_size = %d"%(3 * 1024 * 1024 * 1024))

    def fetchone(self):
        return self.cursor.fetchone()
     
    def fetchall(self):
        return self.cursor.fetchall()

    def fetchmany(self, n):
        return self.cursor.fetchmany(n)

    # we use conn.escape_string instead of MySQLdb.escape_string
    # because of conn.escape_string knowns the coding of string
    def escape_string(self, val_str):
        return self.conn.escape_string(val_str)

    def insertUpdateItems(self, tableName, items, keyFields=None):
        updateFields = (keyFields==None and   items.keys() or keyFields)
        updateStmtArr = []
        valuesFields = []
        for field in updateFields:
            updateStmtArr.append(field + "=VALUES(" + field + ")")
            if items[field]!=None:
                value = "'" + self.escape_string(str(items[field]))+"'"
            else:
                value = "NULL"
            valuesFields.append(value)
        valuesStr = "(%s)" %(",".join(valuesFields))
        fieldsStr = "(%s)"%(','.join(updateFields))
        sql="""
            INSERT INTO %s %s VALUES %s ON DUPLICATE KEY UPDATE %s
        """%(tableName, fieldsStr, valuesStr, ','.join(updateStmtArr))
        self.execute(sql)

    def description(self):
        return self.cursor.description
