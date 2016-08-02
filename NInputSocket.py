#!/usr/bin/env python
# coding=gbk

from NInputData import NInputData
import select

class NInputSocket(NInputData):
    def __init__(self, sockets):
        NInputData.__init__(self, None)
        self.fds = []
        for socket in sockets:
            self.fds.append(socket.makefile())
        self.cnt = len(self.fds)

    def readFd(self, fd, opMgr):
        line = fd.readline().strip()
        if not line:
            return False
        
        itemArr = line.split("\t")
        data = {}
        for field in self.fieldIdx:
            data[field] = itemArr[self.fieldIdx[field]]
        opMgr.process(data)
        return True
    
    def iterator(self, opMgr):
        if self.cnt == 1:
            while True:
                if not self.readFd(self.fds[0], opMgr):
                    break
        else:
            while True:
                inputready,outputready,exceptready = select.select(self.fds,[],[]) 
                for fd in inputready:
                    if not self.readFd(fd, opMgr):
                        self.fds.remove(fd)
                        self.cnt -= 1
                
                if self.cnt == 0:
                    break
                    
        
     

        
