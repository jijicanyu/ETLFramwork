#!/usr/bin/env python
# coding=gb2312

from NInputFile import NInputFile
import datetime
import time
import os
import copy
import decimal
class NInputLogItem(NInputFile):
    def __init__(self, datetime, product ,item, tryTimes =15,
            interval=15,host='logdata.baidu.com',token="ecom_nova_yd6sv6fjjnewg0dnc3me87",
            isDebug=False):
        self.tryTimes = max(1, tryTimes)
        self.interval = interval
        self.filename = item + "_" + datetime.strftime("%Y%m%d%H%M")
        self.isDebug = isDebug
        self.product = product
        self.item = item
        super(NInputLogItem, self).__init__(self.filename)
        self.wGetDownload(datetime, product, item, self.filename, host, token)
        self.fields=[item]
        # 默认double
        self.fieldType = {item:"double"}
    def registerTask(self):
        # register task input/output
        self.taskEnv.registerTask(self.product, self.item, self.__class__.__name__, self.getTaskIOType())
        self.setTaskIOType(None)
    def wGetDownload(self, dateTime, product, item, oFile, host, token):
        cmd = "wget"
        site = "http://%s/?m=Data&a=GetData&token=%s" %(host, token)
        date = dateTime.strftime("%Y-%m-%d %H:%M:00")
        oFileMd5 = oFile + ".md5"
        success = False
            
        for i in range(self.tryTimes):
            
            statusFile=oFile+".status"
            status_url = cmd + " \"" +\
                            site +\
                            "&type=%s" % ("status") +\
                            "&product=%s" % (product) +\
                            "&date=%s" % (date) +\
                            "&item=%s" % (item) +\
                            "\" -O %s " % (statusFile) +\
                            "1>/dev/null 2>/dev/null"
            res = os.system(status_url)
            status = os.popen("awk '{print $2}' " + statusFile).readline().strip()
            os.remove(statusFile)
            if (int(status)!=1):
                print "[%s] wait for %s" % (datetime.datetime.now(), oFile)
                time.sleep(self.interval)
            else:
                item_url = cmd + " \"" +\
                                site +\
                                "&type=%s" % ("normal") +\
                                "&product=%s" % (product) +\
                                "&date=%s" % (date) +\
                                "&item=%s" % (item) +\
                                "\" -O %s " % (oFile) +\
                                "1>/dev/null 2>/dev/null"
                res = os.system(item_url)
                success = True
                break

            if(self.isDebug):
                print item_url
        if not success:
            self.filename = None
            raise Exception("Log data download Failed (product=%s, item=%s, datetime=%s). Tried %d times" % (product, item, dateTime.strftime("%Y%m%d%H"), self.tryTimes))

    def createDataTable(self):
        inputStream = file(self.filename, "r")
        t1 = datetime.datetime.now()
        start = time.time()
        self.dataSet=[]
        while True:
            line = inputStream.readline().strip()
            if len(line) == 0:
                break
            # if the format of line is incorrect, we ignore this line
            itemArr = line.split("\t")
            data = {}
            for field in self.fields:
                data[field] = itemArr[self.fields.index(field)]
            self.dataSet.append(copy.deepcopy(data))
        import NInputData
        inputData = NInputData.NInputData(self.dataSet)
        inputData.fields = copy.deepcopy(self.fields)
        inputData.fieldType = copy.deepcopy(self.fieldType)
        return inputData
                    
    def __del__(self):
        super(NInputLogItem, self).__del__()
        if os.path.isfile(self.filename) and (not self.isDebug):
            if os.path.exists(self.filename):
                os.remove(self.filename)
