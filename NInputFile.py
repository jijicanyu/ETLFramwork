#!/usr/bin/env python
# coding=gbk

from NInputData import NInputData
from NSqlDB import NSqlDB
from NDataOp import *
from NHqlOp import *
import os
import re
import time
import datetime
import random
from NLog import NLogger

class NInputFile(NInputData):
    def __init__(self, filename):
        self.filename = filename
        self.paloFieldType = {}

        super(NInputFile, self).__init__(filename)

    def registerTask(self):
        # register task input
        self.taskEnv.registerTask(self.taskEnv.getRuntimeHost(), self.taskEnv.getRuntimeDir()+"/"+self.filename, self.__class__.__name__, self.getTaskIOType()) 
        self.setTaskIOType(None)       
    def iterator(self, opMgr, begin=0, debug=0):
        inputStream = file(self.filename, "r")
        cnt = begin
        start = time.time()
        while True:
            line = inputStream.readline().strip()
            if len(line) == 0:
                break

            cnt += 1

            # if the format of line is incorrect, we ignore this line
            itemArr = line.split("\t")
            try:
                data = {}
                for field in self.fieldIdx:
                    data[field] = itemArr[self.fieldIdx[field]]
            except:
                continue

            opMgr.process(data)
            if cnt % 10000 == 0:
                #print (time.time() - start) / 10000
                #print cnt
                start = time.time()
        return cnt

    def assign(self, fields, overwrite=False):
        '''
        overrides assign in NInput to support Palo
        '''
        for idx in fields:
            name = fields[idx][0]
            if not overwrite or name not in self.fields:
                self.fields.append(name)
            self.fieldIdx[name] = idx
            type = fields[idx][1]
            type, paloType = self.split_type(type)
            self.fieldType[name] = type
            if paloType:
                self.paloFieldType[name] = type
            # fieldComment
            if len(fields[idx]) >2:
                self.fieldComment[fields[idx][0]] = fields[idx][2]
            else:
                self.fieldComment[fields[idx][0]] = ''
        return self

    def createDBTable(self, db, tableName, overwrite = True, remote = False, keyFields=None, engine='MyIsam', charset='gbk', delim = "\t", partition=None, indexList=None):
        sqlDB = NSqlDB(db)
        if not self.opMgr.root:
            sqlDB.loadData(tableName, self.fields, self.fieldType, os.path.abspath(self.filename),
                    overwrite, remote, keyFields, engine, charset, delim, partition,
                    indexList=indexList, fieldComment=self.fieldComment)
            self.setTaskIOType("output")
            import NInputDB
            return NInputDB.NInputDB(db, tableName)
        else:
            return NInputData.createDBTable(self, db, tableName, overwrite, keyFields,
                    indexList=indexList, fieldComment=self.fieldComment)

    def createDBTableIB(self, db, tableName, overwrite = True, remote = False, keyFields=None,engine='brighthouse', charset='gbk', delim="\t", partition=None,indexList=None):
        if indexList:
            NLogger.warn("BRITHTHOUSE ENGININE DO NOT SUPPORT INDEX")
        if partition:
            NLogger.warn("BRITHTHOUSE ENGININE DO NOT SUPPORT PARTITION")
        sqlDB = NSqlDB(db)
        sqlDB.createTable(tableName,self.fields, self.fieldType, overwrite, keyFields,
                engine,charset, None, None, fieldComment= self.fieldComment)
        sqlDB.loadDataIB(tableName, [], [], os.path.abspath(self.filename), True,
                remote,keyFields,engine, charset,delim, None,None)
        self.setTaskIOType("output")
        import NInputDB
        return NInputDB.NInputDB(db, tableName)

    def removeFile(self):
        os.remove(self.filename)
    
    def execCMD(self, cmd):
        rule = re.compile("\$\{\w+.?\w+\}")
        vars = rule.findall(cmd)
        for var in vars:
            cmd = cmd.replace(var, eval(var.strip("$").strip("{").strip("}")))
        print cmd
        status = os.system(cmd)
        if status!=0:
            raise Exception("exec cmd:%s failed" %(cmd)) 
        return self
     
    def assignFile(self, filename):
        self.filename = filename
        return self

    def createMd5File(self, md5filename=None):
        dirname = os.path.dirname(self.filename) or "."
        filename = os.path.basename(self.filename)
        if md5filename:
            md5file = md5filename
        else:
            md5file = filename+".md5"
        cmd = "cd %s && md5sum %s >%s" %(dirname, filename, md5file)
        n = os.system(cmd)
        if(n!=0): raise Exception( "cmd:%s failed"%(cmd) )
        return self

    def loadToHiveTable(self, db, tableName=None, partition=None, overwrite=False, local=True,
            hiveInit=None, fileformat="TEXTFILE", createCols=False):
        '''
        load file data into hive table
        '''
        if not tableName:
            tableName = "tmp_table_%s_%s_%s" %(datetime.datetime.now().strftime("%Y%m%d%H%M%S"),\
                    random.randrange(1,10000), self.taskEnv.getRuntimePid())
        self.alterSchemaForHive()
        op = NHqlLoadToHiveTableOp(db, tableName, partition, overwrite, local,hiveInit,
                self.fields,self.fieldType,fileformat, autoCols = createCols, 
                fieldComment = self.fieldComment)
        self.opMgr.appendOp(op)
        self.opMgr.process(self.filename)
        self.setTaskIOType("output")
        import NInputHive
        return NInputHive.NInputHive(db, tableName, hiveInit)

    def loadToPaloTable(self, db, tableName, label, hiveInit=None,
                        local=True, fileformat='TEXTFILE',
                        overwrite=False, keyFields=None,
                        hash_mod=61, hash_method='full_key',
                        partition_columns=None, data_file_type='row',
                        max_filter_ratio=0, timeout=3600,
                        is_negative=False, partition=None):
        tempdb = 'exchange_db'

        # decide temp table name
        timestamp = time.strftime('%Y%m%d%H%M%S')
        temp_table_name = 'tmp_%s_%d_%d' % (timestamp,
                                           int(random.random() * 10000),
                                           os.getpid())
        step1_table_name = temp_table_name + '_1'
        step2_table_name = temp_table_name + '_2'

        # load to hive
        step1 = self.loadToHiveTable(tempdb, step1_table_name,
                                     overwrite=True,
                                     local=local,
                                     hiveInit=hiveInit,
                                     partition={},
                                     fileformat=fileformat)

        # load to palo
        step2, fields, schema_dict = step1.clone().select(['*'])\
            .prepareTmpTableForPalo(tempdb, step2_table_name)
        
        NLogger.info('hive schema : ' + repr(schema_dict))
        NLogger.info('palo schema : ' + repr(self.paloFieldType))
        NLogger.info('keyFields : ' + repr(keyFields))
        
        op = NHqlCreatePaloTableFamilyOp(
            db,
            tableName,
            overwrite=overwrite,
            srcHiveDB=tempdb,
            srcHiveTable=step2_table_name,
            fields=fields,
            fieldType=schema_dict,
            overrideField=self.paloFieldType,
            keyFields=keyFields,
            hash_mod=hash_mod,
            hash_method=hash_method,
            partition_columns=partition_columns,
            data_file_type=data_file_type,
            label=label,
            max_filter_ratio=max_filter_ratio,
            timeout=timeout,
            is_negative=is_negative,
            partition=partition)
        op.process('%s.%s' % (tempdb, step2_table_name))

        # remove temp tables
        import NInputHive
        NInputHive.NInputHive(tempdb, step1_table_name).dropTable()
        step2.dropTable()

        self.setTaskIOType('output')
        import NInputPalo
        return NInputPalo.NInputPalo(db, tableName)

    # overwriting renameField
    def renameField(self, field, asField, type=None, comment=''):
        """
        @Desc: rename field type
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
            self.fieldComment[asField] =  field in self.fieldComment.keys() \
                    and self.fieldComment[field] or "NULL"
        self.fields = copy.deepcopy(newField)
        return self

    def createDataTable(self,delim="\t"):
        inputStream = file(self.filename, "r")
        self.dataSet=[]
        while True:
            line = inputStream.readline().strip('\n')
            if len(line) == 0:
                break
            itemArr = line.split(delim)
            data = {}
            for field in self.fields:
                data[field] = itemArr[self.fields.index(field)]
            self.dataSet.append(copy.deepcopy(data))
        inputData = NInputData(self.dataSet)
        inputData.fields = copy.deepcopy(self.fields)
        inputData.fieldType = copy.deepcopy(self.fieldType)
        inputData.fieldComment = copy.deepcopy(self.fieldComment)
        return inputData

    def setLogFilename(self, filename):
        if self.taskEnv.enable_mfs and (filename.find("/") == -1):
            mfs_path = self.taskEnv.mfs_path
            self.filename = mfs_path + "/" + filename
        else:
            self.filename = filename
        dir = os.path.dirname(os.path.abspath(self.filename))
        if not os.path.exists(dir):
            os.makedirs(dir)

    def toTempFile(self):
        """
        convert to a NInputTempFile object for auto removal
        """
        return NInputTempFile(self)

class NInputTempFile(NInputFile):
    def __init__(self, inputfile):
        if isinstance(inputfile, NInputFile):
            super(NInputTempFile, self).__init__(inputfile.filename)
            self.fields = copy.deepcopy(inputfile.fields)
            self.fieldType = copy.deepcopy(inputfile.fieldType)
            self.fieldComment = copy.deepcopy(inputfile.fieldComment)
        else:
            super(NInputTempFile, self).__init__(inputfile)
    
    def __del__(self):
# super(NInputTempFile, self).__del__()
        if(os.path.exists(self.filename)):
            if(os.path.isfile(self.filename)):
                os.remove(self.filename)
    def createTempDBTable(self, db, overwrite = True, remote = False, keyFields=None, engine='MyIsam', charset='gbk', delim = "\t"):
        tableName = "tmp_%s_%s" %(datetime.datetime.now().strftime("%Y%m%d%H%M%S"), random.randrange(1,10000)) 
        sqlDB = NSqlDB(db)
        sqlDB.loadData(tableName, self.fields, self.fieldType, os.path.abspath(self.filename), overwrite, remote, keyFields,engine, charset, delim)
        import NInputDB
        return NInputDB.NInputTempDB(db, tableName)
