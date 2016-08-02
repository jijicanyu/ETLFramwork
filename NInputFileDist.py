#!/usr/bin/env python
# coding=gbk

from NInputData import NInputData
from NServer import *
import datetime
import copy
import pp
from multiprocessing import Process
import threading
import select
import socket
import os
import time
import sys

class NInputFileDist(NInputData):
    def __init__(self, filename, producerCnt, consumerCnt, ppservers, portBase, modules=()):
        NInputData.__init__(self, None)
        self.filename = filename
        self.producerCnt = producerCnt
        self.consumerCnt = consumerCnt
        self.producers = []
        self.consumers = []
        self.ports = []
        self.ppservers = ppservers
        self.dispatcher = None
        self.host = socket.gethostbyname(socket.gethostname())
        self.portBase = portBase
        self.modules = modules
        self.processor = None

    def preProcess(self):
        producers = []
        for i in xrange(self.producerCnt):
            self.ports.append(i + self.portBase)
        for i in xrange(self.producerCnt):
            p = Process(target=self.startProducer, args=(i,))
            p.start()
            producers.append(p)

        self.dispatcher = None
        userFunc = []
        op = self.opMgr.root
        while op:
            func = op.getUserFunc()
            if func:
                userFunc.append(func)
            for child in op.child:
                childFunc = child.getUserFunc()
                if childFunc:
                    userFunc.append(childFunc)
            op = op.next
    
        ppServer = pp.Server(4, ppservers=self.ppservers)
        self.consumers = [ppServer.submit(self.startConsumer, (i,), tuple(userFunc), tuple(self.modules)) for i in xrange(self.consumerCnt)]
        
        self.producers = producers

    def iterator(data, opMgr):
        pass

    def postProcess(self):
        for consumer in self.consumers:
            print consumer()

        for producer in self.producers:
            producer.join()

        print "END"

    def startProducer(self, idx):
        print "io process %d"%(os.getpid())
        server = NFileDispatchServer(self.filename, idx, self.producerCnt, self.ports[idx], self.consumerCnt, self.dispatcher)
        server.run()
 
    def startConsumer(self, idx):
        sockets = []
        for port in self.ports:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
            s.settimeout(None)

            # 每隔1秒尝试重新连接一次
            retry = 1
            while True:
                try:
                    s.connect((self.host, port))
                    break
                except:
                    if retry == 3:
                        print "Connect to server FAILED: had tried %d times"%(retry) 
                        return
                    retry += 1
                    time.sleep(1)
                
            s.send(str(idx))
            sockets.append(s)

        inputSocket = NQuery.NInputSocket(sockets)
        inputSocket.fieldIdx = self.fieldIdx
        inputSocket.fieldType = self.fieldType
        inputSocket.fields = self.fields
        inputSocket.opMgr = self.opMgr
        inputSocket.doProcess()
        return "Client SUCCESS"

    def dispatch(self, dispatcher):
        self.dispatcher = dispatcher
        return self

    def removeFile(self):
        os.remove(self.filename)

    def processOrig(self, processor):
        self.processor = processor
        return self

class NInputMultiFileDist(NInputFileDist):
    def __init__(self, producerCnt, consumerCnt, ppservers, portBase, modules=()):
        NInputFileDist.__init__(self, [], producerCnt, consumerCnt, ppservers, portBase, modules)
    
    def appendFile(self, filename):
        self.filename.append(filename)

    def startProducer(self, idx):
        server = NMultiFileDispatchServer(self.ports[idx], self.consumerCnt, self.dispatcher, idx==0)
        begin = int(float(idx) / self.producerCnt * len(self.filename))
        end = int(float(idx + 1) / self.producerCnt * len(self.filename))
        for i in xrange(begin, end, 1):
            server.appendFile(self.filename[i])
        server.run()
    
    def removeFile(self):
        for filename in self.filename:
            os.remove(filename)



