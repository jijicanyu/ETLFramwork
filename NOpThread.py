#!/usr/bin/env python
# coding=gbk

from multiprocessing import Process, Queue

##########################################
## class:   NOpMgr
## desc:

def func(op, q):
    print x * x

            
class NOpMgrThread:
    def __init__(self):
        self.root = None
        self.tail = None
        self.q = Queue()

    def appendOp(self, op):
        if not self.root:
            self.root = op
            self.tail = op
        else:
            self.tail = op.appendTo(self.tail)

    def preProcess(self):
        if self.root:
            for i in range(8):
                p = Process(traget=func, args=(self.root, self.q,))
                p.start()
                p.join()

    def process(self, data):
        if self.root:
            q.put(data)
            p = Process(target=func, args=(2,))
            p.start()
            p.join()
            #return self.root.process(data)

    def processEnd(self):
        if self.root:
            q.close()
            return self.root.processEnd()
        

