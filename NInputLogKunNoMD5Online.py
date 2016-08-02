#!/usr/bin/env python
# coding=gbk

from NQuery import NQuery
from NInputFile import NInputFile
import datetime
import time
import os

class NInputLogKunNoMD5Online(NInputFile):
    def __init__(self, datetime, product, item, tryTimes = 1, interval = 0, idx=-1):
        self.product = product
        self.item = item
        NInputFile.__init__(self, None)
        self.tryTimes = max(1, tryTimes)
        self.interval = interval
        self.idx = idx
        filename = item + "_" + datetime.strftime("%Y%m%d%H%M") + "%s"%(idx >= 0 and "_%d"%(idx) or "")
        self.setLogFilename(filename)
        self.wGetDownload(datetime, product, item, self.filename)
    def registerTask(self):
        # register task input/output
        self.taskEnv.registerTask(self.product, self.item, self.__class__.__name__, self.getTaskIOType())
        self.setTaskIOType(None)
    def wait_for_log_ready(self,date_time,item):
        product = "ecom_nova"
        token = "ecom_nova_yd6sv6fjjnewg0dnc3me87"
        oFile = item + date_time.strftime("%Y%M%d%H%M%S") + datetime.datetime.now().strftime("%Y%m%d%H%M%S")        

        for i in range(self.tryTimes):
            cmd = "wget"
            site ="http://online.logdata.baidu.com/?m=Data&a=GetData&token=%s"%(token)
            dateTime = date_time.strftime("%Y-%m-%d %H:%M:%S")
            statusFile = oFile+".status"
            statusCmd = cmd + " \"" +\
                        site +\
                        "&product=%s" % (product) +\
                        "&date=%s" %(dateTime)+\
                        "&item=%s"% (item) +\
                        "&type=%s" %("status")+\
                        "\" -O %s" % (statusFile)+\
                        " 1>/dev/null 2>/dev/null"
            os.system(statusCmd)
            status = os.popen("awk '{print $2}' " + statusFile).readline().strip()
            if (int(status)==1):
                os.remove(statusFile)
                NQuery.WriteLog("%s: %s readey" %(item, date_time))
                return 0
            else:
                os.remove(statusFile)
                NQuery.WriteLog("%s: %s not ready" %(item,date_time))
                time.sleep(self.interval)
        raise Exception("try %d times, but data not ready!" % self.tryTimes)
         
    def wGetDownload(self, dateTime, product, item, oFile):
        cmd = "wget"
        site = "http://online.logdata.baidu.com/?m=Data&a=GetData&token=ecom_nova_yd6sv6fjjnewg0dnc3me87"
        date = dateTime.strftime("%Y-%m-%d %H:%M:00")
        
        self.wait_for_log_ready(dateTime,item)
        
        res = os.system(cmd + " \"" +\
                        site +\
                        "&type=%s" % (self.idx < 0 and "normal" or "midoutfile") +\
                        "&product=%s" % (product) +\
                        "&date=%s" % (date) +\
                        "&item=%s" % (item) +\
                        "%s" % (self.idx >= 0 and "&file=%.6d"%(self.idx) or "") +\
                        "\" -O %s " % (oFile) +\
                        "1>down.std 2>down.err")
        if res==0:
            NQuery.WriteLog("wget file success!")
        else:
            NQuery.WriteLog("wget file: %s  Failed! %s  ERROR:%d" %(item, date,res))
            raise Exception("wget file: %s  Failed! %s  ERROR:%d" %(item, date,res))
            
