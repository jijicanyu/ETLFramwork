#!/usr/bin/env python
# coding=gbk

from NSqlDB import NSqlDB
from NOp import NOp
from NOp import NOpChild
from heapq import heappush, heappop
import copy
import os
import json
import time
import random
from NUtils import Mdict
##########################################
## class:   NSumOp
## desc:
class NSumOp(NOpChild):
    def __init__(self, field, asField):
        NOpChild.__init__(self, asField)
        self.field = field
        self.sum = 0
        
    def process(self, data):
        # I think there is better way !
        try:
            self.sum += data[self.field]
        except:
            try:
                self.sum += int(data[self.field])
            except:
                #print self.field
                #print data[self.field]
                self.sum += float(data[self.field])

    def postProcess(self, data):
        data[self.asField] = self.sum
        return data

##########################################
## class:   NBitOrOp
## desc:
class NBitOrOp(NOpChild):
    def __init__(self, field, asField):
        NOpChild.__init__(self, asField)
        self.field = field
        self.val = 0
        
    def process(self, data):
        self.val = self.val | data[self.field]

    def postProcess(self, data):
        data[self.asField] = self.val
        return data


##########################################
## class:   NMinOp
## desc:
class NMinOp(NOpChild):
    def __init__(self, field, asField):
        NOpChild.__init__(self, asField)
        self.field = field
        self.minValue = None

    def process(self, data):
        if self.minValue:
            self.minValue = min(self.minValue, data[self.field])
        else:
            self.minValue = data[self.field]

    def postProcess(self, data):
        data[self.asField] = self.minValue
        return data

##########################################
## class:   NMaxOp
## desc:
class NMaxOp(NOpChild):
    def __init__(self, field, asField):
        NOpChild.__init__(self, asField)
        self.field = field
        self.maxValue = None

    def process(self, data):
        if self.maxValue:
            self.maxValue = max(self.maxValue, data[self.field])
        else:
            self.maxValue = data[self.field]

    def postProcess(self, data):
        data[self.asField] = self.maxValue
        return data

##########################################
## class:   NAverageOp
## desc:
class NAverageOp(NOpChild):
    def __init__(self, field, asField):
        NOpChild.__init__(self, asField)
        self.field = field
        self.sum = 0
        self.cnt = 0

    def process(self, data):
        try:
            self.sum += data[self.field]
        except:
            try:
                self.sum += int(data[self.field])
            except:
                self.sum += float(data[self.field])
        self.cnt += 1

    def postProcess(self, data):
        data[self.asField] = float(self.sum) / self.cnt
        return data


##########################################
## class:   NSelectOp
## desc:
class NSelectOp(NOp):
    def __init__(self, fields):
        NOp.__init__(self)
        self.fields = copy.deepcopy(fields)
        self.saveData = []
 
    def doProcess(self, data):
        selectData = {}
        for field in self.fields:
            selectData[field] = data[field]
        return selectData

        
    def process(self, data):
        selectData = self.doProcess(data)
        if not selectData:
            return
        if not isinstance(selectData, list):
            selectData = [selectData]
        if self.child:
            for d in selectData:
                self.saveData.append(d)
                for op in self.child:
                    op.process(d)
        elif self.next:
            for d in selectData:
                self.next.process(d)

    def processEnd(self):
        if self.next:
            self.childPostProcess(self.child, 0, self.saveData)
            self.saveData = []
            self.next.processEnd()
            

##########################################
## class:   NDBInsertOp
## desc:
class NDBInsertOp(NOp):
    def __init__(self, db, tableName, fields):
        NOp.__init__(self)
        self.fields = list(fields)
        self.db = db
        self.tableName = tableName
        self.sqlDB = None

    def preProcess(self):
        self.sqlDB = NSqlDB(self.db)

    def process(self, data):
        self.sqlDB.insertCache(self.tableName, data, self.fields)

    def processEnd(self):
        self.sqlDB.insertFlush(self.tableName, self.fields)
        NOp.processEnd(self)
            
##########################################
## class:   NDBUpdateOp
## desc:
class NDBUpdateOp(NOp):
    def __init__(self, db, tableName, fields, updateFields):
        NOp.__init__(self)
        self.fields = list(fields)
        self.db = db
        self.tableName = tableName
        self.updateFields = list(updateFields)
        self.sqlDB = None

    def preProcess(self):
        self.sqlDB = NSqlDB(self.db)

    def process(self, data):
        self.sqlDB.insertUpdateCache(self.tableName, data, self.fields, self.updateFields)
        
    def processEnd(self):
        self.sqlDB.insertUpdateFlush(self.tableName, self.updateFields)
        NOp.processEnd(self)

##########################################
## class:   NDBUpdateWithFieldsOp
## desc:
class NDBUpdateWithFieldsOp(NOp):
    def __init__(self, db, tableName, fields, updateFields, fieldType=None, createCols=False):
        NOp.__init__(self)
        self.fields = list(fields)
        self.db = db
        self.tableName = tableName
        self.updateFields = list(updateFields)
        self.sqlDB = None
        self.createCols = createCols
        self.fieldType = fieldType
        if(self.createCols):
            self.autoCreateCols()

    def autoCreateCols(self):
        # auto add colums in program, 
        if self.createCols:
            toSqlDB = NSqlDB(self.db)
            toSqlDB.useDictCursor()
            toSqlDB.execute("SHOW FULL COLUMNS FROM %s" %(self.tableName))
            columns_attr = toSqlDB.fetchall()
            add_colmns=[]
            for field in self.fields:
                if field in [ca['Field'] for ca in columns_attr]:
                    continue
                else:
                    SQL="ALTER TABLE %s ADD COLUMN %s %s" %(self.tableName, field,
                            self.fieldType[field])
                    toSqlDB.execute(SQL)
            toSqlDB.close()

    def preProcess(self):
        self.sqlDB = NSqlDB(self.db)

    def process(self, data):
        self.sqlDB.insertUpdateCache(self.tableName, data, self.fields, self.updateFields)
        
    def processEnd(self):
        self.sqlDB.insertUpdateFlush(self.tableName, self.updateFields, self.fields)
        NOp.processEnd(self)
            
##########################################
## class:   NGroupOp
## desc:
class NGroupOp(NOp):
    def __init__(self, fields):
        NOp.__init__(self)
        self.fields = list(fields)
        self.groupData = {}
    
    def process(self, data):
        groupData = self.groupData
        for field in self.fields:
            if data[field] not in groupData:
                groupData[data[field]] = {}
            groupData = groupData[data[field]]
        if '__data__' not in groupData:
            groupData['__data__'] = data
            groupData['__child__'] = copy.deepcopy(self.child)
        for op in groupData['__child__']:
            op.process(data)

    
    def processEnd(self):
        if self.next and self.groupData:
            self.processRecurse(self.groupData, 0)
            del self.groupData
            self.next.processEnd()

    def processRecurse(self, groupData, idx):
        if idx == len(self.fields):
            self.childPostProcess(groupData['__child__'], 0, groupData['__data__'])
        else:
            for field in groupData:
                self.processRecurse(groupData[field], idx + 1)
             
##########################################
## class:   NCloneGroupOp
## desc:
class NCloneGroupOp(NOp):
    def __init__(self, fields):
        NOp.__init__(self)
        self.fields = list(fields)
        self.groupData = {}
#self.dictArr = [{} for i in xrange(1000000)]
        self.dictCnt = 0
        self.c = None
    
    def preProcess(self):
        self.c = self.child[0].processor
        self.adview = 0
        self.click = 0
        self.gain = 0
        self.pv = 0.0
        self.pctr = 0


    def test(self, data, v):
        v.adview = int(data["adview"])
        v.click = int(data["adview"])
        v.gain = int(data["adview"])
        v.pv = float(data["adview"])
        v.pctr = int(data["pctr"])
        return
        self.adview += int(data["adview"])
        self.click += int(data["click"])
        self.gain += int(data["gain"])
        self.pv += float(data["pv"])
        self.pctr += int(data["pctr"])
        
    def process(self, data):
        groupData = self.groupData
        k = data["md5"]
#v = groupData.setdefault(k, (data, self.c.clone()))
        groupData[k] = data
        return
        v = groupData.get(k, None)
        if not v:
            v = (data["pos"], self.c.clone())
            groupData[k] = v
        v[1].process(data)
        return
#self.test(data, v)
        if 1:
            v.adview = int(data["adview"])
            v.click = int(data["adview"])
            v.gain = int(data["adview"])
            v.pv = float(data["adview"])
            v.pctr += int(data["pctr"])
#v.process(data)
        return
#groupData = groupData.setdefault(int(random.uniform(1, 100)), dict.fromkeys(range(100), {}))
#k = data["md5"]
        
#k = int(random.uniform(1, 10000000))
#if k not in groupData:
#groupData[k] = {}
#groupData = groupData[k]
        if '__data__' not in groupData:
            groupData['__data__'] = data
            opArr = []
            for op in self.child:
                opArr.append(op.clone())
            groupData['__child__'] = opArr

        for op in groupData['__child__']:
            op.process(data)

        return
        if '__data__' not in groupData:
            groupData['__data__'] = data
            groupData['__child__'] = self.child
        if 0:
            for op in self.child:
                ## OP itself must have clone function
                groupData['__child__'].append(op.clone())
        if 0:
            for op in groupData['__child__']:
                op.process(data)
    
    def processEnd(self):
        if self.next and self.groupData:
            self.processRecurse(self.groupData, 0)
            del self.groupData
            self.next.processEnd()

    def processRecurse(self, groupData, idx):
        if idx == len(self.fields):
#self.childPostProcess(groupData['__child__'], 0, groupData['__data__'])
            data = groupData['__data__']
            for child in groupData['__child__']:
                data = child.postProcess(data)
            self.next.process(data)
        else:
            for field in groupData:
                self.processRecurse(groupData[field], idx + 1)
             
##########################################
## class:   NSwapGroupOp
## desc:
class NSwapGroupOp(NOp):
    def __init__(self, fields):
        NOp.__init__(self)
        self.fields = list(fields)
        self.groupData = {}
        self.filename = None
        self.fileStream = None
        self.cur = 0
    
    def preProcess(self):
        self.filename = "swap_group_cache_%d"%(os.getpid())
        self.fileStream = open(self.filename, 'w+')
        NOp.preProcess(self)

    def postProcess(self):
        self.fileStream.close()
        os.remove(self.filename)
        NOp.postProcess(self)
        
    def process(self, data):
        groupData = self.groupData
        for field in self.fields:
            if data[field] not in groupData:
                groupData[data[field]] = {}
            groupData = groupData[data[field]]
        if '__data__' not in groupData:
            groupData['__data__'] = self.cur
            groupData['__child__'] = copy.deepcopy(self.child)
            
            dataStr = json.dumps(data) + "\n"
            self.fileStream.write(dataStr)
            self.cur += len(dataStr)
            
        for op in groupData['__child__']:
            op.process(data)

    
    def processEnd(self):
        if self.next and self.groupData:
            self.processRecurse(self.groupData, 0)
            del self.groupData
            self.next.processEnd()

    def processRecurse(self, groupData, idx):
        if idx == len(self.fields):
            pos = groupData['__data__']
            self.fileStream.seek(pos)
            data = json.loads(self.fileStream.readline().strip())
            
            self.childPostProcess(groupData['__child__'], 0, data)
        else:
            for field in groupData:
                self.processRecurse(groupData[field], idx + 1)
                


##########################################
## class:   NCreateDictOp
## desc:
class NCreateDictOp(NOp):
    def __init__(self, keyField, valueField):
        NOp.__init__(self)
        self.keyField = keyField
        self.valueField = valueField
        self.dict = {}

    def process(self, data):
        self.dict[data[self.keyField]] = data[self.valueField]

    def value(self):
        return self.dict
   
##########################################
## class:   NCreateMultiDictOp
## desc:
class NCreateMultiDictOp(NOp):
    def __init__(self, keyField, valueFields):
        NOp.__init__(self)
        self.keyField = keyField
        self.valueFields = valueFields
        self.dict = {}

    def process(self, data):
        valueData = {}
        for field in self.valueFields:
            valueData[field] = data[field]
        self.dict[data[self.keyField]] = valueData

    def value(self):
        return self.dict


#########################################
## class:   NCreateMultiListOp
## desc:
class NCreateMultiListOp(NOp):
    def __init__(self, keyFields):
        NOp.__init__(self)
        self.keyFields = keyFields
        self.dict = {}

    def process(self, data):
        for field in self.keyFields:
            if field not in self.dict.keys():
                self.dict[field]=[] 
            self.dict[field].append(data[field])

    def value(self):
        return self.dict
##########################################
## class:   NProcessOp
## desc:
class NProcessOp(NSelectOp):
    def __init__(self, callback, userData):
        NSelectOp.__init__(self, None)
        self.callback = callback
        self.userData = userData

    def doProcess(self, data):
        processData = self.callback(data, self.userData)
        return processData

    def getUserFunc(self):
        return self.callback

##########################################
## class:   NProcessFastOp
## desc:
class NProcessFastOp(NSelectOp):
    def __init__(self, callback, userData):
        NSelectOp.__init__(self, None)
        self.callback = callback
        self.userData = userData

    def process(self, data):
#processData = self.callback(data, self.userData)
        processData = self.callback.process(data)
#return self.next.process(processData)

    def getUserFunc(self):
        return self.callback


##########################################
## class:   NProcessorOp
## desc:
class NProcessorOp(NSelectOp):
    def __init__(self, processor):
        NSelectOp.__init__(self, None)
        self.processor = processor

    def preProcess(self):
        self.processor.preProcess()
        NSelectOp.preProcess(self)

    def doProcess(self, data):
        return self.processor.process(data)

    def postProcess(self):
        self.processor.postProcess()
        NSelectOp.postProcess(self)

    def getUserFunc(self):
        return self.processor.__class__

##########################################
## class:   NProcessEachOp
## desc:
class NProcessEachOp(NOpChild):
    def __init__(self, processor):
        NOpChild.__init__(self)
        self.processor = processor

    def process(self, data):
        self.processor.process(data)

    def postProcess(self, data):
        return self.processor.postProcess(data)

    def getUserFunc(self):
        return self.processor.__class__

    def clone(self):
        return NProcessEachOp(self.processor.clone())

##########################################
## class:   NFakeGroupOp
## desc:
class NFakeGroupOp(NOp):
    def __init__(self, fields):
        NOp.__init__(self)
        self.fields = list(fields)
        self.groupData = {}
        
    def process(self, data):
        groupData = self.groupData
        for field in self.fields:
            if data[field] not in groupData:
                groupData[data[field]] = {}
            groupData = groupData[data[field]]
        if '__data__' not in groupData:
            groupData['__data__'] = []
            groupData['__child__'] = copy.deepcopy(self.child)
        groupData['__data__'].append(data)
        for op in groupData['__child__']:
            op.process(data)
    
    def processEnd(self):
        if self.next:
            self.processRecurse(self.groupData, 0)
            del self.groupData
            self.next.processEnd()

    def processRecurse(self, groupData, idx):
        if idx == len(self.fields):
            self.childPostProcess(groupData['__child__'], 0, groupData['__data__'])
        else:
            for field in groupData:
                self.processRecurse(groupData[field], idx + 1)
     
##########################################
## class:   NCreataDataTableOp
## desc:
class NCreateDataTableOp(NOp):
    def __init__(self):
        NOp.__init__(self)
        self.table = []

    def process(self, data):
        self.table.append(data)

    def value(self):
        return self.table

##########################################
## class:   NCreataFileOp
## desc:
class NCreateFileOp(NOp):
    def __init__(self, filename, fields):
        NOp.__init__(self)
        self.fields = list(fields)
        self.file = open(filename, 'w')

    def process(self, data):
        itemArr = []
        for field in self.fields:
            itemArr.append(str(data[field]))
        self.file.write("%s\n" % ("\t".join(itemArr)))

    def processEnd(self):
        self.file.close()

##########################################
## class:   NCountOp
## desc:
class NCountOp(NOpChild):
    def __init__(self, field, asField, distinct):
        NOpChild.__init__(self, asField)
        self.field = field
        self.distinct = distinct
        if self.distinct:
            self.count_set = set()
        else:
            self.count = 0

    def process(self, data):
        """
            process each row in one group
        """
        if self.distinct:
            self.count_set.add(data[self.field])
        else:
            self.count += 1

    def postProcess(self, data):
        """
            compute count
        """
        if self.distinct:
            data[self.asField] = len(self.count_set)
        else:
            data[self.asField] = self.count
        return data

    def clone(self):
        return NCountOp(self.asField)

##########################################
## class:   NTopOp
## desc:    利用最小堆实现的TOP排序操作
class NTopOp(NOpChild):
    def __init__(self, field, topCnt):
        NOpChild.__init__(self, None)
        self.heap = []
        self.saveData = {}
        self.topCnt = topCnt
        self.field = field
        
    def pushData(self, data):
        heappush(self.heap, data[self.field])
        if data[self.field] not in self.saveData:
            self.saveData[data[self.field]] = []
        self.saveData[data[self.field]].append(data)

    def popData(self):
        val = heappop(self.heap)
        if val in self.saveData:
            self.saveData[val].pop()
        
    def process(self, data):
        if len(self.heap) < self.topCnt:
            self.pushData(data)
        elif data[self.field] > self.heap[0]:
            self.popData()
            self.pushData(data)

    def postProcess(self, data):
        dataSet = []
        while self.heap:
            dataSet.extend(self.saveData[heappop(self.heap)])
        return dataSet
 

##########################################
## class:   NJoinOp
## desc:    
class NJoinOp(NOpChild):
    def __init__(self, inputData, joinFields, linkFields, defaultValue=None):
        NOpChild.__init__(self)
        self.joinFields = joinFields
        self.linkFields = linkFields
        self.groupData = Mdict()
        self.default = defaultValue
        # reassign inputdata
        for unitData in inputData:
            unit_key = "-".join([str(unitData[field]) for field in self.linkFields])
            for field in joinFields:
                self.groupData[unit_key][field] = unitData[field]
    def process(self, data):
        groupData = self.groupData
        unit_key = "-".join([str(data[field]) for field in self.linkFields])
        for field in self.joinFields:
            if unit_key not in groupData:
                data[field] = self.default
            else:
                data[field] = groupData[unit_key][field]
        return data

    def postProcess(self, data):
        return data



##########################################
## class:   NEachOp
## desc:
class NEachOp(NOpChild):
    def __init__(self, statment, asField, type=None):
        NOpChild.__init__(self, asField)
        self.asField = asField
        self.statment = statment
        
    def process(self, data):
        if hasattr(self.statment,"__call__"):
            data[self.asField] = self.statment(data)
        else:
            data[self.asField] = self.statment
        return data
        
    def postProcess(self, data):
        return data


##########################################
## class:   NUnionAllOp
## desc:    
class NUnionAllOp(NOp):
    def __init__(self, inputData):
        NOp.__init__(self)
        self.uniondata = inputData
        self.result = []
    def process(self, data): 
        self.result.append(data)
        return data
    def value(self):
        return  self.result + list(self.uniondata)

##########################################
## class:   NUnionOp
## desc:   union method, uniq by uniqkey
class NUnionOp(NOp):
    def __init__(self, inputData, uniqKey, overwrite = True):
        NOp.__init__(self)
        self.right = inputData
        self.uniqKey = uniqKey
        self.result = []
        self.overwrite = overwrite
        # deal with right data
        self.rightData={}
        for unitdata in self.right.dataSet:
            uk = "-".join([str(unitdata[field]) for field in self.uniqKey])
            self.rightData[uk]={}
            for field in self.right.fields:
                self.rightData[uk][field] = unitdata[field] 
        
    def process(self, data): 
        uk = "-".join([str(data[field]) for field in self.uniqKey])
        tmp_data = data
        if (self.rightData.has_key(uk)):
            if (self.overwrite):
                tmp_data = self.rightData[uk]
            self.rightData.pop(uk)
        self.result.append(tmp_data)
        return tmp_data
    def value(self):
        for uk in self.rightData:
           self.result.append(self.rightData[uk])
        return  self.result


##########################################
## class:   NFullJoinOp
## desc:   linkFields must be uniq for both side of full join 
class NFullJoinOp(NOp):
    def __init__(self, inputData, joinFields, linkFields, defaultValue=0):
        NOp.__init__(self)
        self.joinFields = joinFields
        self.linkFields = linkFields
        self.groupData = Mdict()
        self.leftField=[]
        self.firstEntrance = True
        self.defaultValue = defaultValue
        self.data = []
        # reassign inputdata
        for unitData in inputData:
            unit_key = "-".join([str(unitData[field]) for field in self.linkFields])
            for field in joinFields+linkFields :
                self.groupData[unit_key][field] = unitData[field]

    def process(self, data):
        # record the data schema
        if(self.firstEntrance):
            self.leftField = [field for field in data.keys()]
            self.firstEntrance = False
        
        groupData = self.groupData
        unit_key = "-".join([str(data[field]) for field in self.linkFields])
        is_found = False
        for field in self.joinFields:
            if unit_key not in groupData:
                data[field] = self.defaultValue
            else:
                data[field] = groupData[unit_key][field]
                is_found = True
        # if we found the key in group data, we pop the data 
        if(is_found):
            groupData.pop(unit_key)
        self.data.append(data)
        return data

    def value(self):
        # if groupData not empty, we just combine group data with data, default 0
        remain_data = []
        remain_field = [field for field in self.leftField if field not in self.joinFields+ self.linkFields]
        if(self.groupData):
            for key in self.groupData:
                tmp_data={}
                for field in self.joinFields+self.linkFields:
                    tmp_data[field] = self.groupData[key][field] 
                    for field in remain_field:
                        tmp_data[field]= self.defaultValue
                remain_data.append(copy.deepcopy(tmp_data))       
        return self.data + remain_data


##########################################
## class:   NUpdateDataTableOp
## desc: implement of sql like 'insert *** on depulicate key update'    
class NUpdateDataTableOp(NOp):
    def __init__(self, inputData, updateFields, keyFields, updateCallback, defaultvalue=0):
        NOp.__init__(self)
        self.updateFields = updateFields
        self.keyFields = keyFields
        self.updateCallback = updateCallback
        self.groupData = Mdict()
        self.result=[]
        self.firstEntrance = True
        self.leftFields = []
        # default value
        self.default=defaultvalue 
        # reassign inputdata, with hashkey
        for unitData in inputData:
            unit_key = "-".join([str(unitData[field]) for field in self.keyFields])
            self.groupData[unit_key] = unitData
    def process(self, data):
        # record the data schema
        if(self.firstEntrance):
            self.leftFields = [field for field in data.keys()]
            self.firstEntrance = False
        groupData = self.groupData
        unit_key = "-".join([str(data[field]) for field in self.keyFields])
        is_found = True
        if unit_key not in groupData: 
            is_found = False
        else: 
            is_found = True
        for field in self.updateFields:
            if not data.has_key(field):
                data.update({field:self.default})
            if hasattr(self.updateCallback,"__call__"):
                if(is_found):
                    data[field] = self.updateCallback(data, groupData[unit_key], field)
                else:
                    tmp_data={}
                    tmp_data[field]=self.default
                    data[field] = self.updateCallback(data, tmp_data, field)
            else:
                if(is_found):
                    data[field] = groupData[unit_key][field]
                else:
                    data[field] = data[field]
        self.result.append(data)
        if(is_found):
            self.groupData.pop(unit_key)
        return data
    def value(self):
        # if groupData not empty, we just combine group data with data, default 0
        remain_data=[]
        if(self.groupData):
            for key in self.groupData:
                tmp_data={}
                for field in self.leftFields:
                    if field in self.updateFields + self.keyFields:
                        tmp_data[field] = self.groupData[key][field] 
                    else:
                        tmp_data[field]=self.default
                remain_data.append(copy.deepcopy(tmp_data))       
        return  self.result+ remain_data


##########################################
## class:   NfilterOp
## desc: if true then pass else delete
class NFilterOp(NOpChild):
    def __init__(self, callback):
        NOpChild.__init__(self)
        self.callback = callback

    def preProcess(self):
        pass
    def process(self, data):
        pass
    def postProcess(self, data):
        if self.callback(data):
            return data
        else:
            del data   

##########################################
## class:   NRenameOp
## desc: rnameField 
class NRenameOp(NOp):
    def __init__(self, field, asField):
        NOp.__init__(self)
        self.asField = asField
        self.field = field
        self.data=[]
    def process(self, data):
        data[self.asField] = data[self.field]
        self.data.append(data)
    
    def value(self):
        return self.data
