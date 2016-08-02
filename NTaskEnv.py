#!/usr/bin/env python
# coding=gbk
import copy
import os
import sys
import time
import socket
from NLog import NLogger
from NConfParser import NConfParser
from NSqlDB import NSqlDB

class NTaskEnv(object):
    def __init__(self):
        super(NTaskEnv, self).__init__()
        np = NConfParser()
        self.db= np.get("NTaskEnv","database")
        self.taskInput = np.get("NTaskEnv","taskInput")
        self.taskOutput = np.get("NTaskEnv","taskOutput")
        self.task_info_db = np.get("NTaskEnv","task_info_db")
        self.task_info_table = np.get("NTaskEnv","task_info_table")
        self.instance_info_table = np.get("NTaskEnv","instance_info_table")
        self.register_taskIO = np.get("NTaskEnv","register_taskIO","BOOLEAN")
        self.enable_taskEnv = np.get("NTaskEnv","enable_taskEnv","BOOLEAN")
        self.enable_mfs = np.get("NTaskEnv","enable_mfs","BOOLEAN")
        self.mfs_path = np.get("NTaskEnv","mfs_path")
        # init task Evn
        self.initTaskEnv()
        self.taskId=None
        self.taskIO=[]
        
    def getRuntimeDir(self):
        self.workdir = os.getcwd()
        return self.workdir

    def getRuntimePid(self):
        self.pid = os.getpid()
        return self.pid
        
    def getRuntimeHost(self):
        self.host = socket.gethostname().strip(".baidu.com")
        return self.host
    
    def getRuntimeArg(self):
        self.argv = sys.argv
        return self.argv
    
    def parseArg(self):
        if not self.argv:
            self.getRuntimeArg()
        else:
            # split args from sys.argv
            self.scriptName = self.argv[0]
            if len(self.argv) > 1:
                self.execParam = self.argv[-1]
                self.scriptArgs = self.argv[1:-1]
            else:
                self.execParam = None
                self.scriptArgs = []
            
    def getTaskId(self):
        cursor = NSqlDB(self.task_info_db)
        cursor.useDictCursor()
        sql= "select * from %s where userAthost like '%%%s%%' and command_path like '%%%s%%' and command like '%%%s%%%s%%'" %(self.task_info_table, self.host, self.workdir, self.scriptName, " ".join(self.scriptArgs))
        cursor.execute(sql)
        task_info = cursor.fetchall()
        cursor.close()
        if len(task_info)<1:
            NLogger.debug("can't find taskid by params, sql: %s"%(sql))
            self.taskId = None
        elif len(task_info)>1:
            NLogger.debug("find too many taskids %s, sql: %s"%(task_info, sql))
            self.taskId = None
        else:
            self.taskId=task_info[0]['task_id']
            NLogger.debug("find taskId %s, sql:%s" %(task_info, sql))
        return self.taskId

    def getInstanceId(self):
        if not self.taskId:
            self.getTaskId()
        sql = "select * from %s where task_id =%s and param like '%s'" %(self.instance_info_table, self.taskId, self.execParam)
        cursor = NSqlDB(self.task_info_db)
        cursor.useDictCursor()
        cursor.execute(sql)
        instance_info = cursor.fetchall()
        cursor.close()
        if (len(instance_info)<1):
            NLogger.debug("can't find instance by param, sql:%s" %(sql))
            self.instanceId = None
        elif len(instance_info)>1:
            NLogger.info("find too many instance_ids:%s, sql: %s " %(instance_info,sql))
            self.InstanceId = None
        else:
            self.instanceId = instance_info[0]['instance_id']
            NLogger.debug("find instanceid %s" %(self.instanceId))
        return self.instanceId

    def registerTaskInput(self, index, value, taskType):
        if not self.enable_taskEnv: return 
        if not self.taskId:
            self.getTaskId()
        data={}
        data['taskType']=taskType
        data["item"] = index
        data['value'] = self.processTaskIO(value)
        data["taskId"] = self.taskId
        self.__registerTaskIO(data, "input")

    def registerTaskOutput(self, index, value, taskType):
        if not self.enable_taskEnv: return 
        if not self.taskId:
            self.getTaskId()
        data={}
        data['taskType']=taskType
        data["item"] = index
        data['value'] = self.processTaskIO(value)
        data["taskId"] = self.taskId
        self.__registerTaskIO(data, "output")

    def registerTask(self, index, value, taskType, IOType):
        if not self.enable_taskEnv: return 
        if not self.taskId:
            self.getTaskId()
        data={}
        data['taskType']=taskType
        data["item"] = index
        data['value'] = self.processTaskIO(value)
        data["taskId"] = self.taskId
        self.__registerTaskIO(data, IOType)
        self.__flushTaskIO()

    def __registerTaskIO(self, data, IOType="input"):
        # register task input output
        ref_keys = data.keys()
        tmp_dict = copy.deepcopy(data)
        tmp_dict["in_count"] = (IOType=="input" and 1 or 0)
        tmp_dict['out_count'] = (IOType == 'output' and 1 or 0)
        if not self.taskIO:
            self.taskIO.append(copy.deepcopy(tmp_dict))
            NLogger.debug(self.taskIO)
            return 
        else:
            for t_io in self.taskIO:
                found_flag=True
                for key in ref_keys:
                    if(tmp_dict[key]!=t_io[key]):
                        found_flag=False
                        break
                if(found_flag):
                    t_io["in_count"]+= tmp_dict["in_count"]
                    t_io["out_count"]+= tmp_dict["out_count"]
                    break
            if(not found_flag):
                self.taskIO.append(copy.deepcopy(tmp_dict))
        NLogger.debug(self.taskIO)
        return 

    def __flushTaskIO(self):
        try:
            if self.register_taskIO:
                cursor = NSqlDB(self.db)
                for t_io in self.taskIO:
                    if t_io["in_count"] > 0 and  t_io['out_count']==0:
                        cursor.insertUpdateItems(self.taskInput, t_io,["taskId","taskType","item","value"])
                        NLogger.debug("update task input %s, items:%s" %(self.taskInput, t_io))
                    elif t_io['out_count']>0:
                        cursor.insertUpdateItems(self.taskOutput, t_io,["taskId","taskType","item","value"])
                        NLogger.debug("update task input %s, items:%s" %(self.taskOutput, t_io))
                    else:
                        NLogger.info("ignore taskIO, data:%s"%(t_io))
                cursor.close()
            else:
                NLogger.info("ingore register task IO")
        except Exception as e:
            NLogger.debug("flush task IO failed, %s ,%s" %(self.taskIO, e))
        finally:
            self.taskIO=[]

    def __del__(self):
        try:
            self.__flushTaskIO()
            NLogger.debug("finish update taskIO")
        except:
            NLogger.critical("flush task IO failed, %s" %(self.taskIO))
    def initTaskEnv(self):
        self.getRuntimeDir()
        self.getRuntimeHost()
        self.getRuntimeArg()
        self.parseArg()

    def processTaskIO(self, values):
        import re
        p=re.compile("\d")
        return p.sub("X",values)
if __name__=="__main__":

    b =  NTaskEnv()
    
