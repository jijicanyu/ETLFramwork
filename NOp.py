#!/usr/bin/env python
# coding=gbk

from multiprocessing import Process

##########################################
## class:   NOpMgr
## desc:
class NOpMgr:
    def __init__(self):
        self.root = None
        self.tail = None

    def appendOp(self, op):
        if not self.root:
            self.root = op
            self.tail = op
        else:
            self.tail = op.appendTo(self.tail)
    
    def preProcess(self):
        if self.root:
            self.root.preProcess()
    
    def process(self, data):
        if self.root:
            return self.root.process(data)

    def processEnd(self):
        if self.root:
            return self.root.processEnd()

    def postProcess(self):
        if self.root:
            self.root.postProcess()

    def clear(self):
        self.root = None
        self.tail = None
        
##########################################
## class:   NOp
## desc:
class NOp:
    def __init__(self):
        self.next = None
        self.child = []

    def getAssigner(self):
        return None
        
    def appendTo(self, op):
        op.next = self
        return self
    
    def preProcess(self):
        if self.next:
            self.next.preProcess()
        
    def process(self, data):
        pass

    def processEnd(self):
        if self.next:
            self.next.processEnd()

    def postProcess(self):
        if self.next:
            self.next.postProcess()
            
    def childPostProcess(self, childOp, idx, dataSet):
        if not dataSet:
            return
        if not isinstance(dataSet, list):
            dataSet = [dataSet]
        if len(childOp) == idx:
            while dataSet:
                self.next.process(dataSet.pop(0))
        else:
            while dataSet:
                self.childPostProcess(childOp, idx + 1, childOp[idx].postProcess(dataSet.pop(0)))

    def getUserFunc(self):
        return None
             
##########################################
## class:   NOp
## desc:
class NOpThread(NOp):
    def __init__(self, threadCnt):
        NOp.__init__(self)
        self.threadCnt = threadCnt
        self.queues = []
        self.workers = []

    def run(self, queue):
        while True:
            data = queue.get()
            if not data:
                break
            self.process(data)
        self.processEnd()
        
    def preProcess(self):
        import multiprocessing.queues
        if self.next:
            self.next.preProcess()

        for i in range(self.threadCnt):
            q = multiprocessing.queues.SimpleQueue()
            p = Process(target=self.run, args=(q,))
            self.workers.append(p)
            self.queues.append(q)
            p.start()

    def postProcess(self):
        for p in self.workers:
            p.join()
        
        if self.next:
            self.next.dispatch(0)
            self.next.postProcess()

    def dispatch(self, data):
        if not data:
            for q in self.queues:
                q.put(0)
            return

        if self.threadCnt == 0:
            self.process(data)
        else:
            self.doDispatch(data)

    def doDispatch(self, data):
        pass
            
    def childPostProcessThread(self, childOp, idx, dataSet):
        if not dataSet:
            return
        for op in childOp:
            dataSet = op.postProcess(dataSet)
        self.next.dispatch(dataSet)
            

            
##########################################
## class:   NOpChild
## desc:
class NOpChild(NOp):
    def __init__(self, asField=None):
        NOp.__init__(self)
        self.asField = asField
    
    def appendTo(self, op):
        op.child.append(self)
        return op
 
    def postProcess(self, data):
        pass

##########################################
## class:   NProcessor
## desc:
class NProcessor:
    pass
