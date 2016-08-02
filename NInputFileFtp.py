#!/usr/bin/env python
# coding=gbk

from NInputFile import NInputFile
import os
from datetime import datetime
import time
import random
import sys
class NInputFileFtp(NInputFile):
    def __init__(self, host, pathname, tryTimes=15, interval = 60*15, ismd5sum=True, speedLimit=30,
            isDebug=False, md5path=None, callback=None, filename=None):
        if not filename:
            filename = os.path.basename(pathname) + "_" + datetime.now().strftime("%Y%M%d%H%M%S")\
                + "_" + str(random.randrange(1, 100))
        self.setLogFilename(filename)
        self.tryTimes = tryTimes
        self.interval = interval
        self.ismd5sum = ismd5sum
        self.speedLimit = speedLimit
        self.isDebug = isDebug
        self.md5path =  md5path 
        self.host = host
        self.pathname = pathname
        self.callback = callback
        super(NInputFileFtp, self).__init__(self.filename)
        self.wGetDownload(host, pathname)

    def registerTask(self):
        self.taskEnv.registerTask(self.host, self.pathname, self.__class__.__name__, self.getTaskIOType())
        self.setTaskIOType(None)
    def wGetDownload(self, host, pathname):
        cmd = "wget "
        cmd = cmd  +" -q -t 1 -T 180 -nd --limit-rate=%sM " %(self.speedLimit) 
        success = False
        for i in range(self.tryTimes):
            success = True
            filecmd = cmd +\
                " %s "%(host+pathname) +\
                "-O %s"%(self.filename)
            res = os.system(filecmd)
            if (self.isDebug): print filecmd
            if(self.ismd5sum):
                md5file = self.filename + ".md5"
                md5cmd = cmd +\
                    " %s "%(host+ ((self.md5path) and (self.md5path) or  (pathname+'.md5')) ) +\
                    "-O %s"%(md5file)
                res = os.system(md5cmd)
                if(self.isDebug): print md5cmd
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
                    if (self.callback) and self.callback(self)!=1: #Return of callback equals 1 indicates OK
                        os.remove(self.filename)
                        sys.stdout.flush()
                        time.sleep(self.interval)
                        success = False
                        print "The return of callback function is not 1, wait for file..."
                    else:
                        success = True
                        break
                else:
                    os.remove(self.filename)
                    success = False
                    print "[%s] wait for %s" %(datetime.now(),self.filename)
                    sys.stdout.flush()
                    time.sleep(self.interval)
            
        if not success:
            self.filename = None
            raise Exception("[%s] download Failed: Tried %d times" % (datetime.now(), self.tryTimes))
    
