#!/usr/bin/env python
# coding=gbk

import threading
import os
import Queue
import sys

class NThreadFile:
    def __init__(self, filename, split_line, num_thread, func, args):
        self.timeout = 5
        self.filename = filename
        self.split_line = split_line
        self.num_thread = num_thread
        self.func = func
        self.args = args

        self.fileQueue = Queue.Queue()
        self.workers = []

        self.init_work()

    def init_work(self):
        self.split()
        filename = "split_%s_"%(self.filename)
        files = os.listdir(".")
        for f in files:
            if filename == f[0:len(filename)]:
                self.fileQueue.put(f)
        
        for i in range(self.num_thread):
            self.workers.append(WorkThread(self.fileQueue, self.timeout, self.func, self.args))
    

    def split(self):
        cmd = "split -l %s %s split_%s_"%(self.split_line, self.filename, self.filename)
        os.system(cmd)

    def start(self):
        for w in self.workers:
            w.start()

    def join(self):
        while len(self.workers):  
            worker = self.workers.pop()  
            worker.join( )  
            if worker.isAlive() and not self.workQueue.empty():  
                self.workers.append( worker )  
        print "All jobs are are completed." 


        
       

class WorkThread(threading.Thread):
    work_count = 0
    def __init__(self, fileQueue, timeout, func, args):
        threading.Thread.__init__(self)
        self.id = WorkThread.work_count
        WorkThread.work_count += 1
        self.setDaemon(True)
        self.args = args
        self.timeout = timeout
        self.fileQueue = fileQueue
        self.func = func

    def run(self):
        while True:
            try:
                filename = self.fileQueue.get(timeout = self.timeout)
                self.func(filename, self.args)
                print "work_%s run once [%s]"%(self.id, filename)
            except Queue.Empty:  
                break
    
