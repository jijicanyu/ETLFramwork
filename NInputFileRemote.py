#!/usr/bin/env python
# coding=gbk

from NInputFile import NInputFile
import os

class NInputFileRemote(NInputFile):
    def __init__(self, host, pathname):
        NInputFile.__init__(self, None)
        self.filename = os.path.basename(pathname)
        self.wGetDownload(host, pathname)

    def wGetDownload(self, host, pathname):
        res = os.system("wget ftp://%s%s -O %s" % (host, pathname, self.filename))
