#!/usr/bin/env python
# coding=gbk

import select
import socket
import threading
import os
import sys
import operator

class NDispatchServer: 
    def __init__(self, port, taskCnt, dispatcher): 
        self.host = '' 
        self.port = port
        self.backlog = 64
        self.size = 1024 
        self.server = None 
        self.taskCnt = taskCnt
        self.dispatcher = dispatcher
        self.socket = [None] * taskCnt

    def open_socket(self): 
        try: 
            self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
            self.server.bind((self.host, self.port)) 
            self.server.listen(self.backlog) 
        except socket.error, (value, message): 
            if self.server: 
                self.server.close() 
            print "Could not open socket: " + message 
            sys.exit(1) 
        print "Server open SUCCESS %d"%(self.port)

    def run(self): 
        self.open_socket() 
        input = [self.server] 
        running = 1 
        taskCnt = 0
        while running: 
            inputready,outputready,exceptready = select.select(input,[],[]) 
            for s in inputready: 
                if s == self.server: 
                    # handle the server socket 
                    clientSocket, address = self.server.accept()
                    clientSocket.settimeout(None)
                    idx = clientSocket.recv(self.size)
                    self.socket[int(idx)] = clientSocket
                    taskCnt += 1
                    
                    print "%d: client %s connect %d"%(self.port, address, taskCnt)
                    
                    if taskCnt == self.taskCnt:
                        self.preDispatch()
                        self.dispatch()
                        self.postDispatch()
                        running = 0
                        break

        for socket in self.socket:
            socket.close()
        self.server.close() 
        print "Server close"

    def dispatch(self):
        print "Dispatch %d"%(self.port)
        while True:
            line = self.getData()
            if not line:
                break
            item = line.split("\t")
            if self.dispatcher:
                i = self.dispatcher(item)
            else:
                i = int(item[0])
            if i < 0:
                continue
            self.socket[i % len(self.socket)].send(line)

    def preDispatch(self):
        pass

    def postDispatch(self):
        pass

    def getData(self):
        return None

class NFileDispatchServer(NDispatchServer):
    def __init__(self, filename, idx, total, port, taskCnt, dispatcher):
        NDispatchServer.__init__(self, port, taskCnt, dispatcher)
        self.filename = filename
        self.idx = idx
        self.total = total
        self.fileStream = None
        self.endPos = 0
        self.cnt = 0
    
    def preDispatch(self):
        fileSize = os.path.getsize(self.filename)
        self.fileStream = file(self.filename, 'r')
        self.endPos = int(float(self.idx + 1) / self.total * fileSize)
        self.fileStream.seek(int(float(self.idx) / self.total * fileSize))
        #丢弃非首块的第一行
        if self.idx:
            self.fileStream.readline()

    def getData(self):
        if self.idx == 0 and self.cnt % 10000 == 0:
            print self.cnt
        self.cnt += 1

        pos = self.fileStream.tell()
        if pos > self.endPos:
            return None
        return self.fileStream.readline()
        
    def postDispatch(self):
        self.fileStream.close()

class Test:
    def __init__(self, pos, adid):
        self.pos = pos
        self.adid = adid

class NFileIndexDispatchServer(NDispatchServer):
    def __init__(self, filename, idx, total, port, taskCnt, dispatcher):
        NDispatchServer.__init__(self, port, taskCnt, dispatcher)
        self.filename = filename
        self.idx = idx
        self.total = total
        self.fileStream = None
        self.endPos = 0
        self.cnt = 0
        self.index = []
        for i in xrange(taskCnt):
            self.index.append([])
    
    def preDispatch(self):
        fileSize = os.path.getsize(self.filename)
        self.fileStream = file(self.filename, 'r')
        self.endPos = int(float(self.idx + 1) / self.total * fileSize)
        self.fileStream.seek(int(float(self.idx) / self.total * fileSize))
        #丢弃非首块的第一行
        if self.idx:
            self.fileStream.readline()
        
        self.createIndex()

    def dispatch(self):
        print "Dispatch %d"%(self.port)
        posArr = [0] * len(self.index)

        self.cnt = 0
        running = 1
        while running:
            inputready,outputready,exceptready = select.select([], self.socket, [])
            for s in outputready:
                if self.idx == 0 and self.cnt % 10000 == 0:
                    print self.cnt
                self.cnt += 1

                idx = self.socket.index(s)
                pos = self.index[idx][posArr[idx]]
                posArr[idx] += 1
                self.fileStream.seek(pos)
                s.send(self.fileStream.readline())
                if posArr[idx] == len(self.index[idx]):
                    self.index.pop(idx)
                    self.socket.pop(idx)
                    posArr.pop(idx)
                    s.close()
                    if not self.socket:
                        running = 0
                    print "close cliend %d"%(idx)


    def getData(self):
        if self.idx == 0 and self.cnt % 10000 == 0:
            print self.cnt
        self.cnt += 1

        pos = self.fileStream.tell()
        if pos > self.endPos:
            return None
        return self.fileStream.readline()
        
    def postDispatch(self):
        self.fileStream.close()

    def createIndex(self):
        startPos = self.fileStream.tell()
        curPos = startPos
        while True:
            line = self.getData()
            if not line:
                break
            item = line.split("\t")
            if self.dispatcher:
                i = self.dispatcher(item)
            else:
                i = int(item[0])
            if i < 0:
                continue
            self.index[i % len(self.index)].append(curPos)
            curPos = self.fileStream.tell()
        self.fileStream.seek(startPos)
        temp = []
        for arr in self.index:
            temp.append(len(arr))
        print temp
 
    def createIndex_1(self):
        startPos = self.fileStream.tell()
        curPos = startPos
        while True:
            line = self.getData()
            if not line:
                break
            item = line.split("\t")
            if self.dispatcher:
                i = self.dispatcher(item)
            else:
                i = int(item[0])
            if i < 0:
                continue
            self.index[i % len(self.index)].append(Test(curPos, int(item[2])))
            curPos = self.fileStream.tell()
        self.fileStream.seek(startPos)

        temp = []
        for arr in self.index:
            sorted(arr, key=operator.itemgetter(1))
            temp.append(len(arr))
        print temp
     
class NMultiFileDispatchServer(NDispatchServer):
    def __init__(self, port, taskCnt, dispatcher, debug=0):
        NDispatchServer.__init__(self, port, taskCnt, dispatcher)
        self.filenameArr = []
        self.fileStreamArr = []
        self.cur = 0
        self.cnt = 0
        self.debug = debug

    def appendFile(self, filename):
        self.filenameArr.append(filename)

    def preDispatch(self):
        for filename in self.filenameArr:
            self.fileStreamArr.append(file(filename, 'r'))

    def getData(self):
        if self.debug and self.cnt % 10000 == 0:
            print self.cnt
        self.cnt += 1
    
        if self.cur >= len(self.fileStreamArr):
            return None
        data = self.fileStreamArr[self.cur].readline()
        if data:
            return data
        else:
            self.cur += 1
            return self.getData()

    def postDispatch(self):
        for fileStream in self.fileStreamArr:
            fileStream.close()



