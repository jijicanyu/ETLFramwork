#!/usr/bin/env python
# coding=gbk

from NInputFile import NInputFile
import os
from datetime import datetime
import time
import random
import sys
class NInputFileDTS(NInputFile):
    def __init__(self, item, index, item_md5, tryTimes=15, interval = 60*15, isDebug=False):
        self.filename = os.path.basename(item) +"_"+datetime.now().strftime("%Y%M%d%H%M%S") +"_"+ str(random.randrange(1,100))
        self.tryTimes = tryTimes
        self.interval = interval
        self.ismd5sum = item_md5 and True
        self.index = index
        self.item=item
        self.isDebug = isDebug

        super(NInputFileDTS, self).__init__(self.filename)
        self.dtsDownload(item, item_md5)

    def registerTask(self):
        self.taskEnv.registerTask(self.item, self.index, self.__class__.__name__, self.getTaskIOType())
        self.setTaskIOType(None)

    def dtsDownload(self, item, item_md5):
        cmd1 = "noahdt download  %s -i %s %s " %(item, self.index, self.filename )
        md5file = self.filename+"_md5"
        cmd2 = "noahdt download  %s -i %s %s " %(item_md5, self.index, md5file)
        
        success = False
        for i in range(self.tryTimes):
            
            success = False
            res = os.system(cmd1)
            if res!=0:
                print "[%s] wait for %s" %(datetime.now(),self.filename)
                sys.stdout.flush()
                time.sleep(self.interval)
                continue
            if (self.isDebug): print cmd1
            if(self.ismd5sum):
                res = os.system(cmd2)
                if(self.isDebug): print cmd2
                onlineMd5 = os.popen("awk '{print $1}' " + md5file).readline().strip()
                md5 = os.popen("md5sum " + self.filename + " | awk '{print $1}'").readline().strip()
                if(self.isDebug): print onlineMd5,":", md5
                # remove md5 file
                os.remove(md5file)
                if(onlineMd5!=md5):
                    success = False
                    os.remove(self.filename)
                    print "[%s] wait for %s" %(datetime.now(),self.filename)
                    sys.stdout.flush()
                    time.sleep(self.interval)
                else:
                    success = True
                    break
            else:
                if (os.path.getsize(self.filename)!=0):
                    success = True
                    break
                else:
                    os.remove(self.filename)
                    print "[%s] wait for %s" %(datetime.now(),self.filename)
                    sys.stdout.flush()
                    time.sleep(self.interval)
            
        if not success:
            self.filename = None
            raise Exception("[%s] download Failed: Tried %d times" % (datetime.now(), self.tryTimes))
    
