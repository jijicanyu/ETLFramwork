#!/usr/bin/env python
# coding=gbk

from collections import defaultdict

class NLiteralMap:
    def __init__(self):
        self.dataPos = 128
        self.data = [None]*(self.dataPos + 1)

    #overload []
    def __getitem__(self, key):
        res = self.data
        for c in key:
            res = res[ord(c)]
            if not res:
                return None
        return res[self.dataPos]

    #overload set []
    def __setitem__(self, key, value):
        res = self.data
        for c in key:
            if not res[ord(c)]:
                res[ord(c)] = [None]*(self.dataPos + 1)
            res = res[ord(c)]
        res[self.dataPos] = value
    
    def dfs(self, data, res):
        if data[self.dataPos]:
            res.append(data[self.dataPos])
        for i in xrange(self.dataPos):
            if data[i]:
                self.dfs(data[i], res)

    def __iter__(self):
        res = []
        self.dfs(self.data, res)
        for d in res:
            yield d
# define multi-dimension  hash for dict
class Mdict(defaultdict,dict):
    def __init__(self):
        defaultdict.__init__(self,Mdict)
    def __repr__(self):
        return dict.__repr__(self)

def isset(v):
    try:
        type(eval(v))
    except:
        return 0
    else:
        return 1
if __name__ == "__main__":
    m = NLiteralMap()
    m["thisis"] = 14
    for i in m:
        print i
        
