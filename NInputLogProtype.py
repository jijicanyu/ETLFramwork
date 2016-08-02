#!/usr/bin/env python
# coding=gbk

from NInputFile import NInputFile
import datetime
import time
import os
import sys

class NInputLogProtype(NInputFile):
    def __init__(self, datetime, host="log.baidu.com", token="ecom_cpro_2ig4tj1tsbai41tunq5rvi",product="ecom_cpro", item=None, tryTimes = 1, interval = 0, idx=-1):

        self.product = product
        self.item = item
        NInputFile.__init__(self, None)
        self.tryTimes = max(1, tryTimes)
        self.interval = interval
        self.idx = idx
        filename = item + "_" + datetime.strftime("%Y%m%d%H%M") + "%s"%(idx >= 0 and "_%d"%(idx) or "")
        self.setLogFilename(filename)
        self.wGetDownload(datetime, product, item, self.filename, host, token)

    def registerTask(self):
        # register task input/output
        self.taskEnv.registerTask(self.product, self.item, self.__class__.__name__, self.getTaskIOType())
        self.setTaskIOType(None)
    def wGetDownload(self, dateTime, product, item, oFile, host, token):
        cmd = "wget"
        site = "http://%s/?m=Data&a=GetData&token=%s" %(host, token)
        date = dateTime.strftime("%Y-%m-%d %H:00:00")
        oFileMd5 = oFile + ".md5"
        success = False
  
        for i in range(self.tryTimes):
            res = os.system(cmd + " \"" +\
                            site +\
                            "&type=%s" % (self.idx < 0 and "normal" or "midoutfile") +\
                            "&product=%s" % (product) +\
                            "&date=%s" % (date) +\
                            "&item=%s" % (item) +\
                            "%s" % (self.idx >= 0 and "&file=%.6d"%(self.idx) or "") +\
                            "\" -O %s " % (oFile) +\
                            "1>/dev/null 2>/dev/null")
            res = os.system(cmd + " \"" +\
                            site +\
                            "&product=%s" % (product) +\
                            "&date=%s" % (date) +\
                            "&item=%s" % (item) +\
                            "&type=md5" +\
                            "%s" % (self.idx >= 0 and "&file=%.6d"%(self.idx) or "") +\
                            "\" -O %s " % (oFileMd5) +\
                            "1>/dev/null 2>/dev/null")

            onlineMd5 = os.popen("awk '{print $2}' " + oFileMd5).readline().strip()
            md5 = os.popen("md5sum " + oFile + " | awk '{print $1}'").readline().strip()
            # remove md5 file
            os.remove(oFileMd5)
            if onlineMd5 != md5:
                os.remove(oFile)
                print "[%s] wait for %s" % (datetime.datetime.now(), oFile)
                sys.stdout.flush()

                time.sleep(self.interval)
            else:
                success = True
                break

        if not success:
            self.filename = None
            raise Exception("Log data download Failed (product=%s, item=%s, datetime=%s). Tried %d times" % (product, item, dateTime.strftime("%Y%m%d%H"), self.tryTimes))

