#!/usr/bin/env python
# coding=gbk

from NInputLog import NInputLog
from NInputLogOnline import NInputLogOnline
from NInputDB import NInputDB
from NInputFileRemote import NInputFileRemote
from NInputMulti import NInputMulti
from NInputFile import NInputFile
from NInputSocket import NInputSocket
from NInputFileThread import NInputFileThread
#from NInputFileDist import NInputFileDist, NInputMultiFileDist
from NNotify import alarm
from NSqlDB import NSqlDB
import datetime
from NLog import NLogger
import socket

class NQuery:
    @classmethod
    def NInputMulti(cls):
        return NInputMulti()

    @classmethod
    def NInputLog(cls, datetime, product, item, retry =  1, sleepTime =0, idx=-1):
        return NInputLog(datetime, product, item, retry, sleepTime, idx)

    @classmethod
    def NInputLogOnline(cls, datetime, product, item, retry =  1, sleepTime =0, idx=-1):
        return NInputLogOnline(datetime, product, item, retry, sleepTime, idx)

    @classmethod
    def NInputDB(cls, db, tableName):
        return NInputDB(db, tableName)

    @classmethod
    def NInputFile(cls, fileName):
        return NInputFile(fileName)

    @classmethod
    def NInputFileThread(cls, fileName, threadCnt=1):
        return NInputFileThread(fileName, threadCnt)

    #@classmethod
    #def NInputFileDist(cls, fileName, producerCnt, consumerCnt, ppservers, portBase, modules=()):
    #    return NInputFileDist(fileName, producerCnt, consumerCnt, ppservers, portBase, modules)

    #@classmethod
    #def NInputMultiFileDist(cls, producerCnt, consumerCnt, ppservers, portBase, modules=()):
    #    return NInputMultiFileDist(producerCnt, consumerCnt, ppservers, portBase, modules)

    @classmethod
    def NInputFileRemote(cls, host, pathname):
        return NInputFileRemote(host, pathname)

    @classmethod
    def NInputSocket(cls, socket):
        return NInputSocket(socket)

    @classmethod
    def NSqlDB(self, db):
        return NSqlDB(db)

    @classmethod
    def DoTask(cls, taskName, taskFunc, userData=None, notifyId=None):
        host = socket.gethostname().strip(".baidu.com")


        try:
            # db = NSqlDB('task_manager_db')
            # from HiveConfig import hiveConfig
            # db.execute("INSERT INTO qe_hive_log VALUES('%s', '%s', '%s', '%s');" % (
            #            datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), taskName,
            #            '', hiveConfig.get_engine()))
            # db.close()

            taskFunc(userData)
        except Exception as e:
            errMsg = str(e)
            alarm(notifyId, taskName, errMsg)
            import traceback
            NLogger.critical("[host: %s] task %s FAILED: %s, \n%s" % (host, taskName, errMsg, traceback.format_exc()))
            exit(1)
        else:
            NLogger.info("[host: %s] task %s SUCCESS" % (host, taskName))

    @classmethod
    def WriteLog(cls, logStr):
        NLogger.info(logStr)

if __name__ == "__main__":
    def errorfunc(userData):
        date_time = datetime.datetime(2011, 4, 21)
        NQuery.NInputLog(date_time, "ecom_cpro", "test")

    NQuery.DoTask("testTask", errorfunc, None, ["zhangqi"])

