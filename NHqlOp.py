#!/usr/bin/env python
# coding=gb2312

from NOp import NOp
from NOp import NOpChild
import MySQLdb
import copy
import os
from Hive import Hive
import time
import datetime
import ordereddict
import random
import re

class NHqlSelectOp(NOp):
    def __init__(self, fields, asField):
        NOp.__init__(self)
        self.fields = list(fields)
        self.asField = asField

    def process(self, data):
        hqlWhereOp = None
        hqlGroupOp = None
        hqlHavingOp = None
        hqlLimitOp = None
        hqlOrderOp = None
        hqlLateralviewOp = None
        hqlFromOp = None
        hqlDistributeOp = None
        hqlSortOp = None
        hqlJoinOpList = []
        for op in self.child:
            if isinstance(op, NHqlGroupOp):
                hqlGroupOp = op
            elif isinstance(op, NHqlWhereOp):
                hqlWhereOp = op
            elif isinstance(op, NHqlDistributeOp):
                hqlDistributeOp = op
            elif isinstance(op, NHqlSortOp):
                hqlSortOp = op
            elif isinstance(op, NHqlHavingOp):
                hqlHavingOp = op
            elif isinstance(op, NHqlOrderOp):
                hqlOrderOp = op
            elif isinstance(op, NHqlLimitOp):
                hqlLimitOp = op
            elif isinstance(op, NHqlLateralviewOp):
                hqlLateralviewOp = op
            elif isinstance(op, NHqlFromOp):
                hqlFromOp = op
            elif isinstance(op, NHqlJoinOp):
                hqlJoinOpList.append(op)
            elif isinstance(op, NHqlEachOp):
                # replace duplicate columns
                if op.asField in self.fields:
                    pos = self.fields.index(op.asField)
                    self.fields[pos] = op.process(data)
                else:
                    self.fields.append(op.process(data))
            else:
                self.fields.append(op.process(data))
        hqlStat = "SELECT %s\n" % (join_fields(self.fields))
        if hqlFromOp:
            hqlStat += " FROM " + hqlFromOp.process(data) + "\n"
        else:
            hqlStat += " FROM " + data + "\n"
        if hqlJoinOpList:
            for op in hqlJoinOpList:
                hqlStat += " " + op.process(data) + "\n"
        if hqlLateralviewOp:
            hqlStat += " " + hqlLateralviewOp.process(data) + "\n"
        if hqlWhereOp:
            hqlStat += " " + hqlWhereOp.process(data) + "\n"
        if hqlGroupOp:
            hqlStat += " " + hqlGroupOp.process(data) + "\n"
        if hqlDistributeOp:
            hqlStat += " " + hqlDistributeOp.process(data) + "\n"
        if hqlSortOp:
            hqlStat += " " + hqlSortOp.process(data) + "\n"
        if hqlHavingOp:
            hqlStat += " " + hqlHavingOp.process(data) + "\n"
        if hqlOrderOp:
            hqlStat += " " + hqlOrderOp.process(data) + "\n"
        if hqlLimitOp:
            hqlStat += " " + hqlLimitOp.process(data) + "\n"
        if self.asField:
            hqlStat = "(" + hqlStat + ") AS `" + self.asField + "`\n"
        if self.next:
            self.next.process(hqlStat)


class NHqlWhereOp(NOpChild):
    def __init__(self, whereStat):
        NOpChild.__init__(self)
        self.whereStat = whereStat

    def process(self, data):
        return "WHERE " + self.whereStat


class NHqlGroupOp(NOpChild):
    def __init__(self, fields):
        NOpChild.__init__(self)
        self.fields = list(fields)

    def process(self, data):
        return "GROUP BY %s" % (join_fields(self.fields))


class NHqlEachOp(NOpChild):
    def __init__(self, statement, asField):
        NOpChild.__init__(self, asField)
        self.statement = statement

    def process(self, data):
        return self.statement + \
            (self.asField and " AS `%s`" % (self.asField) or "")


class NHqlJoinOp(NOpChild):
    def __init__(self, inputhive, alias, joinType, cond):
        NOpChild.__init__(self, None)
        # indicate subquery
        if inputhive.hql:
            self.tableName = "(" + inputhive.hql + ")"
        else:
            self.tableName = inputhive.db + "." + inputhive.tableName
        self.tbAlias = alias
        self.joinType = joinType
        self.cond = cond

    def process(self, data):
        return " %s JOIN %s `%s` ON (%s) " % (self.joinType, self.tableName,
                                              self.tbAlias, self.cond)


class NHqlFromOp(NOpChild):
    def __init__(self, statment, alias):
        NOpChild.__init__(self)
        self.statment = statment
        self.alias = alias

    def process(self, data):
        return " (%s) `%s` " % (self.statment, self.alias)


class NHqlLateralviewOp(NOpChild):
    def __init__(self, statement, alias, asField):
        NOpChild.__init__(self, asField)
        self.statement = statement
        self.alias = alias
        self.asField = asField

    def process(self, data):
        return "LATERAL VIEW " + self.statement + " " + self.alias + " " + \
            (self.asField and " AS `%s`" % (self.asField) or "")


class NHqlHavingOp(NOpChild):
    def __init__(self, havingStat):
        NOpChild.__init__(self)
        self.havingStat = havingStat

    def process(self, data):
        return "HAVING " + self.havingStat


class NHqlOrderOp(NOpChild):
    def __init__(self, statment):
        NOpChild.__init__(self)
        self.orderStat = statment

    def process(self, data):
        return "ORDER BY " + ','.join(self.orderStat)


class NHqlDistributeOp(NOpChild):
    def __init__(self, fields):
        NOpChild.__init__(self)
        self.fields = fields

    def process(self, data):
        return "DISTRIBUTE BY %s" % (join_fields(self.fields))


class NHqlSortOp(NOpChild):
    def __init__(self, statment):
        NOpChild.__init__(self)
        self.orderStat = statment

    def process(self, data):
        return "SORT BY " + ','.join(self.orderStat)


class NHqlLimitOp(NOpChild):
    def __init__(self, statment):
        NOpChild.__init__(self)
        self.LimitStat = statment

    def process(self, data):
        return "LIMIT " + self.LimitStat


class NHqlCreateFileOp(NOp):
    def __init__(self, filename, singleFile, hiveInit):
        NOp.__init__(self)
        self.filename = filename
        self.singleFile = singleFile
        self.hv = Hive()
        self.hv.Add(hiveInit)

    def process(self, data):
        if os.path.isfile(os.path.abspath(self.filename)):
            os.remove(os.path.abspath(self.filename))
        elif os.path.isdir(os.path.abspath(self.filename)):
            import shutil
            shutil.rmtree(os.path.abspath(self.filename))
        temp_dir = os.path.abspath(self.filename + "_tmp")
        abs_filename = os.path.abspath(self.filename)
        statement = "INSERT OVERWRITE LOCAL DIRECTORY '%s' %s;" \
            % (temp_dir, data)
        self.hv.Add(statement)
        self.hv.ExecuteAll(True)
        if self.singleFile:
            cmd = "cat %s/* > %s  && sync && sleep 10 && rm -r %s" \
                % (temp_dir, abs_filename, temp_dir)
        else:
            cmd = "mv %s %s" % (temp_dir, self.filename)
        self.hv.ExecuteCMD(cmd, True)

    def getFile(self):
        return self.filename


class NHqlWriteToFileOp(NOp):
    def __init__(self, filename, hql, singleFile, hiveInit):
        NOp.__init__(self)
        self.filename = filename
        self.singleFile = singleFile
        self.hv = Hive()
        self.hv.Add(hiveInit)
        self.hql = hql

    def process(self, data):
        if os.path.isfile(os.path.abspath(self.filename)):
            os.remove(os.path.abspath(self.filename))
        elif os.path.isdir(os.path.abspath(self.filename)):
            import shutil
            shutil.rmtree(os.path.abspath(self.filename))
        temp_dir = os.path.abspath(self.filename+"_tmp")
        abs_filename = os.path.abspath(self.filename)
        statement = "INSERT OVERWRITE LOCAL DIRECTORY '%s' %s ;"\
            % (temp_dir, self.hql)
        self.hv.Add(statement)
        self.hv.ExecuteAll(True)
        if self.singleFile:
            cmd = "cat %s/* > %s && sync && sleep 10 && rm -r %s"\
                % (temp_dir, abs_filename, temp_dir)
        else:
            cmd = "mv %s %s" % (temp_dir, self.filename)
        self.hv.ExecuteCMD(cmd)

    def getFile(self):
        return self.filename


class NHqlCreateHiveTableOp(NOp):
    def __init__(self, db, tablename, partition=None, hiveInit="",
                 overwrite=False, fields=None, fieldType=None,
                 fileformat="TEXTFILE", createCols=False, fieldComment=None):

        NOp.__init__(self)
        self.db = db
        self.tablename = tablename
        self.partition = partition
        self.hv = Hive()
        self.hv.Add(hiveInit)
        self.overwrite = overwrite
        self.fields = fields
        self.fieldType = fieldType
        # fileformat: EXTFILE, RCFILE,ORC, INPUTFORMAT, SEQUENCEFILE
        self.fileformat = fileformat
        self.autoCols = createCols
        self.fieldComment = fieldComment
        self.tmpHiveTable = "tmp_%s_%s_%s"\
            % (datetime.datetime.now().strftime("%Y%m%d%H%M%S"),
               random.randrange(1, 10000), os.getpid())

    def process(self, data):
        self.hv.Add("USE %s;" % (self.db))

        if self.isTableExist():
            # get fields definetion from hive
            hive_table_schema = self.getSchema()
            data_table_schema = ordereddict.OrderedDict()
            for field in self.fields:
                # all fields in hive schema are lower chars,
                # trans fields in self.fields to lower
                data_table_schema.update(
                    {field.lower(): self.fieldType[field]}
                )
            # pop hive partition key
            if self.partition:
                for partition in self.partition.keys():
                    hive_table_schema.pop(partition)
                    if partition in data_table_schema:
                        data_table_schema.pop(partition)

            hive_table_fields = copy.deepcopy(hive_table_schema)
            data_table_fields = copy.deepcopy(data_table_schema)
            # 对于表覆盖和 不存在的情况， 不需要考虑schema 不一致问题
        if self.overwrite or (not self.isTableExist())\
                or (hive_table_fields.keys() == data_table_fields.keys()):
            if(self.overwrite):
                self.hv.Add("DROP TABLE IF EXISTS %s;" % (self.tablename))
            str_field = []
            for field in self.fields:
                # skip partition key, there maybe a better way
                if self.partition and field in self.partition.keys():
                    continue
                if self.fieldComment and (field in self.fieldComment.keys()):
                    #';' in hive comment will lead to exception
                    comment = MySQLdb.escape_string(self.fieldComment[field] or "").replace(';', '\;')
                else:
                    comment = ""
                str_field.append("%s %s COMMENT '%s'"
                    % (escape_field(field), self.fieldType[field], comment))
            if self.partition:
                partition_column_hql = '\nPARTITIONED BY (%s)'\
                    % (",".join([
                        "`%s` STRING" % (k) for k, v in self.partition.items()
                    ]))
            else:
                partition_column_hql = ''
            hql = """CREATE TABLE IF NOT EXISTS %s(%s)%s
                ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
                COLLECTION ITEMS TERMINATED BY ','
                LINES TERMINATED BY '\n'
                STORED AS %s;
                """ % (self.tablename, ",".join(str_field),
                       partition_column_hql, self.fileformat)
            self.hv.Add(hql)
            partition_hql = ""
            if self.partition:
                partition_hql = "PARTITION (%s)" % (",".join([
                    '`%s`=%s' % (k, v) for k, v in self.partition.items()
                ]))
            statement = "INSERT OVERWRITE TABLE %s %s %s; " % (
                self.tablename, self.partition and partition_hql or "", data
            )
            self.hv.Add(statement)
        else:
            str_field = []
            if data_table_fields.keys() != hive_table_fields.keys():
                str_field = ["`%s` %s" % (field, data_table_fields[field])
                             for field in data_table_fields.keys()]
                hql = """CREATE TABLE IF NOT EXISTS %s(%s)
                    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
                    COLLECTION ITEMS TERMINATED BY ','
                    LINES TERMINATED BY '\n'
                    STORED AS %s;
                    """ % (self.tmpHiveTable, ",".join(str_field), "RCFILE")
                self.hv.Add(hql)
                hql = "INSERT OVERWRITE TABLE %s %s;" % (self.tmpHiveTable,
                                                         data)
                self.hv.Add(hql)
                select_fields = []
                for tmp_field in hive_table_fields.keys():
                    if tmp_field in data_table_fields.keys():
                        select_fields.append(tmp_field)
                        data_table_fields.pop(tmp_field)
                    else:
                        select_fields.append("NULL")

                # deal with remaining data_table_fields, in the situation of
                # data_table containing more columns than hive table
                for tmp_field in data_table_fields.keys():
                    select_fields.append(tmp_field)
                    if self.fieldComment and (tmp_field in self.fieldComment.keys()):
                        comment = MySQLdb.escape_string(self.fieldComment[tmp_field] or "").replace(';', '\;')
                    else:
                        comment = ""
                    hql = "ALTER TABLE %s ADD COLUMNS(`%s` %s COMMENT '%s');" % (
                        self.tablename,
                        tmp_field,
                        data_table_fields[tmp_field],
                        comment
                    )
                    self.hv.Add(hql)
                # insert into ${self.tableName}
            partition_hql = ""
            if self.partition:
                partition_hql = "PARTITION (%s)" % (",".join([
                    "`%s`=%s" % (k, v) for k, v in self.partition.items()
                ]))
            select_hql = "SELECT %s FROM %s; " % (join_fields(select_fields),
                                                  self.tmpHiveTable)
            statement = "INSERT OVERWRITE TABLE %s %s %s; " % (
                self.tablename,
                self.partition and partition_hql or "",
                select_hql
            )
            self.hv.Add(statement)
            drop_hql = "DROP TABLE IF EXISTS %s;" % (self.tmpHiveTable)
            self.hv.Add(drop_hql)
        self.hv.ExecuteAll(True)

    def isTableExist(self):
        hql = "USE %s; SHOW TABLES;" % (self.db)
        res = self.hv.fetchall_hql(hql)
        tables = res.strip("\n").split("\n")
        for tb in tables:
            if tb.strip(" ") == self.tablename.strip(" "):
                return True
        return False

    def getSchema(self):
        hv = Hive()
        hv.Add("use %s;" % (self.db))
        hv.Add("desc %s;" % (self.tablename))
        schema_str = hv.fetchall()

        import Schema
        schema = Schema.get_schema_list(schema_str)

        return ordereddict.OrderedDict().appendList(schema)


class NHqlExecHqlOp(NOp):
    def __init__(self, hql, hiveInit):
        NOp.__init__(self)
        self.hv = Hive()
        self.hv.Add(hiveInit)
        self.hql = hql

    def process(self, data):
        self.hv.Add(self.hql)
        self.hv.ExecuteAll(True)


class NHqlCreateHqlOp(NOp):
    def __init__(self, hiveInit):
        NOp.__init__(self)
        self.hiveInit = hiveInit
        self.hql = None

    def process(self, data):
        self.hql = (self.hiveInit or " ") + data

    def getHql(self):
        return self.hql


class NHqlLoadToHiveTableOp(NOp):
    def __init__(self, db, tablename, partition=None, overwrite=False,
                 local=True, hiveInit=None, fields=None, fieldType=None,
                 fileformat='TEXTFILE', autoCols=False, fieldComment=None):
        NOp.__init__(self)
        self.db = db
        self.tablename = tablename
        self.partition = partition
        self.hv = Hive()
        self.hv.Add(hiveInit)
        self.overwrite = overwrite
        self.fields = fields
        self.fieldType = fieldType
        self.local = local
        # fileformat: EXTFILE, RCFILE,ORC, INPUTFORMAT, SEQUENCEFILE
        self.fileformat = fileformat
        self.tmpHiveTable = "tmp_%s_%s_%s" % (
            datetime.datetime.now().strftime("%Y%m%d%H%M%S"),
            random.randrange(1, 10000),
            os.getpid()
        )
        self.autoCols = autoCols
        self.fieldComment = fieldComment

    def process(self, data):
        self.hv.Add("USE %s;" % (self.db))
        if self.isTableExist():
            # get fields definetion from hive
            hive_table_schema = self.getSchema()
            data_table_schema = ordereddict.OrderedDict()
            for field in self.fields:
                # all fields in hive schema are lower chars, trans fields in
                # self.fields to lower
                data_table_schema.update(
                    {field.lower(): self.fieldType[field]}
                )
            # pop hive partition key
            if self.partition:
                for partition in self.partition.keys():
                    hive_table_schema.pop(partition)
                    if partition in data_table_schema:
                        data_table_schema.pop(partition)

            hive_table_fields = copy.deepcopy(hive_table_schema)
            data_table_fields = copy.deepcopy(data_table_schema)

        if self.overwrite or (not self.isTableExist())\
                or (hive_table_fields.keys() == data_table_fields.keys()):
            if self.overwrite:
                self.hv.Add("DROP TABLE IF EXISTS %s;" % (self.tablename))
            str_field = []
            for field in self.fields:
                # skip partition key, there maybe a better way
                if self.partition:
                    if field in self.partition.keys():
                        continue
                if self.fieldComment and (field in self.fieldComment.keys()):
                    comment = MySQLdb.escape_string(self.fieldComment[field]).replace(';', '\;')
                else:
                    comment = ""
                str_field.append("%s %s COMMENT '%s'"
                    % (escape_field(field), self.fieldType[field], comment))
            partitioned_hql = ''
            if self.partition:
                partitioned_hql = 'PARTITIONED BY (%s) ' % (",".join([
                    '`%s` STRING' % (k) for k, v in self.partition.items()
                ]))
            hql = """CREATE TABLE IF NOT EXISTS %s(%s )
                %s
                ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
                COLLECTION ITEMS TERMINATED BY ','
                LINES TERMINATED BY '\n'
                STORED AS %s;
                """ % (self.tablename, ",".join(str_field),
                       partitioned_hql, self.fileformat)
            self.hv.Add(hql)
            partition_hql = ""
            if self.partition:
                partition_hql = "PARTITION (%s)" % (",".join([
                    '`%s`=%s' % (k, v) for k, v in self.partition.items()
                ]))
            if self.local:
                statement = "LOAD DATA LOCAL INPATH '%s' OVERWRITE INTO TABLE %s %s ;"\
                    % (os.path.abspath(data),
                       self.tablename,
                       self.partition and partition_hql or "")
            else:
                statement = "LOAD DATA INPATH '%s' OVERWRITE INTO TABLE %s %s ;"\
                    % (data,
                       self.tablename,
                       self.partition and partition_hql or "")
            self.hv.Add(statement)
        else:
            str_field = []
            if data_table_fields.keys() != hive_table_fields.keys():
                for field in data_table_fields.keys():
                    str_field.append("`%s` %s" % (field,
                                                  data_table_fields[field]))
                hql = """CREATE TABLE IF NOT EXISTS %s(%s)
                    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
                    COLLECTION ITEMS TERMINATED BY ','
                    LINES TERMINATED BY '\n'
                    STORED AS %s;
                    """ % (self.tmpHiveTable, ",".join(str_field), "TEXTFILE")
                self.hv.Add(hql)
                if self.local:
                    # data is local file
                    statement = "LOAD DATA LOCAL INPATH '%s' OVERWRITE INTO TABLE %s;"\
                        % (os.path.abspath(data), self.tmpHiveTable)
                else:
                    # data is hdfs path
                    statement = "LOAD DATA INPATH '%s' OVERWRITE INTO TABLE %s;"\
                        % (data, self.tmpHIveTable)
                self.hv.Add(statement)
                select_fields = []
                for tmp_field in hive_table_fields.keys():
                    if tmp_field in data_table_fields.keys():
                        select_fields.append(tmp_field)
                        data_table_fields.pop(tmp_field)
                    else:
                        select_fields.append("NULL")

                # deal with remaining data_table_fields, in the situation of
                # data_table containing more columns than hive table
                for tmp_field in data_table_fields.keys():
                    select_fields.append(tmp_field)
                    if self.fieldComment and (tmp_field in self.fieldComment.keys()):
                        comment = MySQLdb.escape_string(self.fieldComment[field] or "").replace(';', '\;')
                    else:
                        comment = ""
                    hql = "ALTER TABLE %s ADD COLUMNS(`%s` %s COMMENT '%s');" % (
                        self.tablename, tmp_field, data_table_fields[tmp_field],
                        comment
                    )
                    self.hv.Add(hql)
                # insert into ${self.tableName}
            partition_hql = ""
            if self.partition:
                partition_hql = "PARTITION (%s)" % (",".join([
                    "`%s`=%s" % (k, v) for k, v in self.partition.items()
                ]))
            select_hql = "SELECT %s FROM %s; " % (
                join_fields(select_fields),
                self.tmpHiveTable
            )
            statement = "INSERT OVERWRITE TABLE %s %s %s;" % (
                self.tablename,
                self.partition and partition_hql or "",
                select_hql)
            self.hv.Add(statement)
            drop_hql = "DROP TABLE IF EXISTS %s;" % (self.tmpHiveTable)
            self.hv.Add(drop_hql)
        self.hv.ExecuteAll(True)

    def isTableExist(self):
        hql = "USE %s; SHOW TABLES;" % (self.db)
        res = self.hv.fetchall_hql(hql)
        tables = res.strip("\n").split("\n")
        for tb in tables:
            if tb.strip(" ") == self.tablename.strip(" "):
                return True
        return False

    def getSchema(self):
        hv = Hive()
        hv.Add("use %s;" % (self.db))
        hv.Add("desc %s;" % (self.tablename))
        schema_str = hv.fetchall()

        import Schema
        schema = Schema.get_schema_list(schema_str)

        return ordereddict.OrderedDict().appendList(schema)


class NHqlCreateExternalHiveTableOp(NOp):
    def __init__(self, db, tablename, location, partition, fieldType,
                 hiveInit="", overwrite=False, tryTimes=15, interval=5*60,
                 charset="gbk", fieldDelimiter="\t", storedAs="TEXTFILE"):
        NOp.__init__(self)
        self.db = db
        self.tablename = tablename
        self.partition = partition
        self.hv = Hive()
        self.hv.Add(hiveInit)
        self.overwrite = overwrite
        self.fields = [idx.keys()[0] for idx in fieldType]
        self.fieldType = {}
        for idx in fieldType:
            self.fieldType.update(idx)
        self.location = location
        self.tryTimes = tryTimes
        self.interval = interval
        self.charset = 'utf-8' if charset == 'utf8' else charset
        self.fieldDelimiter = fieldDelimiter
        self.storedAs = storedAs

    def process(self, data):
        self.wait_for_data_ready()
        self.hv.Add("USE %s;" % (self.db))
        if self.partition:
            partition_hql = "PARTITIONED BY (%s)" % (",".join([
                '`%s` STRING' % (k) for k, v in self.partition.items()
            ]))
            location_hql = ""
        else:
            partition_hql = ""
            location_hql = "LOCATION '%s'" % (self.location)
        if self.overwrite or (not self.isTableExist()):
            if self.overwrite:
                self.hv.Add("DROP TABLE IF EXISTS %s;" % (self.tablename))
            str_field = []
            for field in self.fields:
                # skip partition key, there maybe a better way
                if self.partition:
                    if field in self.partition.keys():
                        continue
                str_field.append('`%s` %s' % (field, self.fieldType[field]))

            hql = """CREATE EXTERNAL TABLE IF NOT EXISTS %s(%s )
                %s
                ROW FORMAT CHARSET '%s' DELIMITED FIELDS TERMINATED BY '%s'
                COLLECTION ITEMS TERMINATED BY ','
                LINES TERMINATED BY '\n'
                STORED AS %s
                %s;
                """ % (self.tablename, ",".join(str_field), partition_hql,
                       self.charset, self.fieldDelimiter, self.storedAs, location_hql)
            self.hv.Add(hql)

        # add partition
        if self.partition and self.isTableExist():
            partition = ",".join([
                '`%s`=%s' % (k, v) for k, v in self.partition.items()
            ])
            drop_partition_hql = "ALTER TABLE %s DROP IF EXISTS PARTITION(%s);"\
                % (self.tablename, partition)
            add_partition_hql = " ALTER TABLE %s ADD PARTITION(%s) LOCATION '%s';"\
                % (self.tablename, partition, self.location)
            self.hv.Add(drop_partition_hql)
            self.hv.Add(add_partition_hql)
        self.hv.ExecuteAll(True)

    def isTableExist(self):
        hql = "USE %s; SHOW TABLES;" % (self.db)
        res = self.hv.fetchall_hql(hql)
        tables = res.strip("\n").split("\n")
        for tb in tables:
            if tb.strip(" ") == self.tablename.strip(" "):
                return True
        return False

    def wait_for_data_ready(self):
        success = True
        for tryTimes in range(self.tryTimes):
            try:
                hv = Hive()
                # is director exists
                cmd1 = "hadoop fs -test -d %s " % (self.location)
                hv.ExecuteCMD(cmd1, True)
                success = True
                # is length not zero
                break
            except Exception:
                print "[%s] wait for %s ready" % (
                    datetime.datetime.now(),
                    self.location
                )
                time.sleep(self.interval)
                success = False
        if not success:
            raise Exception("TIME OUT, %s not ready, waiting for %s seconds" %
                            (self.location, self.tryTimes*self.interval))


class NHqlDropTableOp(NOp):
    def __init__(self, db, table):
        NOp.__init__(self)
        self.hv = Hive()
        self.db = db
        self.table = table

    def process(self, data):
        hql = "USE %s; DROP TABLE IF EXISTS %s" % (self.db, self.table)
        self.hv.Add(hql)
        self.hv.ExecuteAll(True)


class NHqlTransformSelectOp(NOp):
    def __init__(self, fields, scriptName=None):
        NOp.__init__(self)
        self.fields = list(fields)
        self.eachlist = []
        self.scriptName = scriptName

    def process(self, data):
        hqlDistributeOp = None
        hqlSortOp = None
        eachStrList = []
        for op in self.child:
            if isinstance(op, NHqlDistributeOp):
                hqlDistributeOp = op
            elif isinstance(op, NHqlSortOp):
                hqlSortOp = op
            else:
                eachStrList.append(op.process(data))
        # split 'each' statment
        self.processEachList(eachStrList)
        hqlStat = " FROM %s \n" % (data)
        transformlist = self.fields +\
            [i.keys()[0] for i in self.eachlist if len(self.eachlist) > 0]
        uselist = self.fields +\
            [i.values()[0] for i in self.eachlist if len(self.eachlist) > 0]
        hqlStat += " SELECT TRANSFORM( %s )\n" % (join_fields(transformlist))
        hqlStat += " USING '%s' \n" % (self.scriptName)
        hqlStat += " AS %s \n" % (join_fields(uselist))
        if hqlDistributeOp:
            hqlStat += " " + hqlDistributeOp.process(data) + "\n"
        if hqlSortOp:
            hqlStat += " " + hqlSortOp.process(data) + "\n"
        if self.next:
            self.next.process(hqlStat)

    def processEachList(self, eachList):
        # split 'each' statment
        for statment in eachList:
            data = statment.strip(" ").split("AS")
            if len(data) > 1:
                self.eachlist.append({data[0]: data[1]})
            else:
                self.eachlist.append({data[0]: [data[0]]})


class NHqlPrepareTmpTableForPaloOp(NOp):
    def __init__(self, hql, fields, schema_dict, alias_name='t'):
        self.hql = hql
        self.fields = list(fields)
        self.schema_dict = dict(schema_dict)
        self.alias_name = alias_name

    @classmethod
    def create_statement(cls, fields, schema_dict, hql, alias_name):
        # set values for null value
        # hive_type : [null_conditions, null_value, map_function]
        type_map = {
            'tinyint': [['%s is null'], '0', None],
            'smallint': [['%s is null'], '0', None],
            'int': [['%s is null'], '0', None],
            'bigint': [['%s is null'], '0', None],
            'boolean': [['%s is null'], '0', 'cast(%s as tiny)'],
            'float': [['%s is null'], '0.0', None],
            'double': [['%s is null'], '0.0', None],
            'decimal': [['%s is null'], '0', None],
            'string': [['%s is null', '%s = "NULL"'], "''", None],
            'varchar': [['%s is null', '%s = "NULL"'], "''", None]
        }

        each_statements = []
        for column_name in fields:
            column_type = schema_dict[column_name].lower()
            default_value = type_map[column_type][1]
            src_column_name = '`%s`.`%s`' % (alias_name, column_name)
            conditions = ' or '.join([
                condition % escape_field(src_column_name)
                for condition in type_map[column_type][0]
            ])
            if type_map[column_type][2]:
                map_func = type_map[column_type][2] % src_column_name
            else:
                map_func = src_column_name
            statement = "if(%s, %s, %s)" % (
                conditions, default_value, map_func
            )
            each_statements.append('%s AS `%s`' % (statement, column_name))

        fields_hql = ', '.join(each_statements)

        return 'SELECT %s FROM ( %s ) `%s`'\
            % (fields_hql, hql, alias_name)

    def process(self, data):
        statement = NHqlPrepareTmpTableForPaloOp.create_statement(
            self.fields, self.schema_dict,
            self.hql, self.alias_name
        )
        self.next.process(statement)


class NHqlCreatePaloTableFamilyOp(NOp):
    def __init__(self, db, tbName, overwrite=False,
                 srcHiveDB=None, srcHiveTable=None, hiveInit='',
                 fields=None, fieldType=None,
                 overrideField=None, keyFields=None,
                 hash_mod=61, hash_method='full_key',
                 partition_columns=None, data_file_type='row',
                 label=None, is_negative=False, max_filter_ratio=0,
                 timeout=3600, partition=None, rollup_tables=None):
        NOp.__init__(self)
        self.db = db
        self.tbName = tbName
        self.overwrite = overwrite

        import PaloClient
        palo = PaloClient.PaloClient(self.db)

        self.fields = fields

        modified_schema = palo.create_schema(
            fields,
            fieldType,
            overrideField,
            keyFields=keyFields,
            type_map_type='hive')

        self.tbInfo = PaloClient.TableFamilyInfo(
            self.tbName,
            modified_schema,
            partition_method='hash',
            partition_columns=partition_columns,
            data_file_type=data_file_type,
            hash_mod=hash_mod,
            hash_method=hash_method)
        if rollup_tables:
            for table in rollup_tables:
                self.tbInfo.add_rollup_dict(table)

        # batch_load
        self.max_filter_ratio = max_filter_ratio
        self.timeout = timeout
        self.is_negative = is_negative
        self.partition = partition

        self.hiveDB = srcHiveDB
        self.hiveTable = srcHiveTable
        self.hiveInit = hiveInit
        self.loadData = self.hiveDB is not None and self.hiveTable is not None

        if label:
            self.label = label
        else:
            # random label always generated in batch_load_sync
            self.label = '%s_%s' % (self.tbName, self.hiveTable)

    def process(self, data):
        import PaloClient
        palo = PaloClient.PaloClient(self.db)

        if self.overwrite and palo.is_table_exist(self.tbName):
            palo.drop_table_family(self.tbName)

        palo.create_table_family(self.tbName, self.tbInfo, overwrite=False)

        if self.loadData:
            _, option = parse_schema_and_options(self.hiveDB,
                                                 self.hiveTable,
                                                 self.hiveInit)
            if not self.is_negative:
                locations = ['%s/*' % (loc) for loc in option['location']]
                recover_locations = []
            else:
                locations = []
                recover_locations = ['%s/*' % (loc)
                                     for loc in option['location']]
            print 'load from %s' % (locations)
            succ, detail = palo.batch_load_sync(
                self.label,
                self.tbName,
                urls=locations,
                negative_urls=recover_locations,
                fields=self.fields,
                separator='\t',
                timeout=self.timeout,
                max_filter_ratio=self.max_filter_ratio,
                partition=self.partition)

        return '%s.%s' % (self.db, self.tbName)

# a list of hive keywords, only "NULL" is removed.
keywords = ['TRUE', 'FALSE', 'ALL', 'AND', 'OR', 'NOT', 'LIKE', 'ASC', 'DESC',
            'ORDER', 'BY', 'GROUP', 'WHERE', 'FROM', 'AS', 'SELECT',
            'DISTINCT', 'INSERT', 'OVERWRITE', 'OUTER', 'JOIN', 'LEFT',
            'RIGHT', 'FULL', 'ON', 'PARTITION', 'PARTITIONS', 'TABLE',
            'TABLES', 'TBLPROPERTIES', 'SHOW', 'MSCK', 'DIRECTORY', 'LOCAL',
            'LOCKS', 'TRANSFORM', 'USING', 'CLUSTER', 'DISTRIBUTE', 'SORT',
            'UNION', 'LOAD', 'DATA', 'INPATH', 'IS', 'CREATE',
            'EXTERNAL', 'ALTER', 'DESCRIBE', 'DROP', 'TO',
            'COMMENT', 'BOOLEAN', 'TINYINT', 'SMALLINT', 'INT', 'BIGINT',
            'FLOAT', 'DOUBLE', 'DATE', 'DATETIME', 'TIMESTAMP', 'STRING',
            'BINARY', 'ARRAY', 'MAP', 'REDUCE', 'PARTITIONED', 'CLUSTERED',
            'SORTED', 'INTO', 'BUCKETS', 'ROW', 'FORMAT', 'DELIMITED',
            'FIELDS', 'TERMINATED', 'COLLECTION', 'ITEMS', 'KEYS', 'LINES',
            'STORED', 'INPUTFORMAT', 'SEQUENCEFILE', 'TEXTFILE', 'RCFILE',
            'OUTPUTFORMAT', 'LOCATION', 'TABLESAMPLE', 'BUCKET', 'OUT', 'OF',
            'CAST', 'ADD', 'REPLACE', 'COLUMNS', 'RLIKE', 'REGEXP',
            'TEMPORARY', 'FUNCTION', 'EXPLAIN', 'EXTENDED', 'SERDE', 'WITH',
            'SERDEPROPERTIES', 'LIMIT', 'SET',
            'AFTER', 'ANALYZE', 'ARCHIVE', 'BEFORE', 'BETWEEN', 'BOTH',
            'CASCADE', 'CASE', 'CLUSTERSTATUS', 'COLUMN', 'COMPUTE',
            'CONCATENATE', 'CONTINUE', 'CROSS', 'CURSOR', 'DATABASE',
            'DATABASES', 'DBPROPERTIES', 'DEFERRED', 'DELETE', 'DISABLE',
            'ELSE', 'ENABLE', 'END', 'ESCAPED', 'EXCLUSIVE', 'EXISTS',
            'EXPORT', 'FETCH', 'FILEFORMAT', 'FIRST', 'FORMATTED',
            'FUNCTIONS', 'GRANT', 'HAVING', 'HOLD_DDLTIME', 'IDXPROPERTIES',
            'IF', 'IMPORT', 'IN', 'INDEX', 'INDEXES', 'INPUTDRIVER',
            'INTERSECT', 'LATERAL', 'LOCK', 'LONG', 'MACRO', 'MAPJOIN',
            'MATERIALIZED', 'MINUS', 'NO_DROP', 'OFFLINE', 'OPTION',
            'OUTPUTDRIVER', 'PERCENT', 'PLUS', 'PRESERVE', 'PROCEDURE',
            'PURGE', 'RANGE', 'READ', 'READONLY', 'READS', 'REBUILD',
            'RECORDREADER', 'RECORDWRITER', 'RENAME', 'REPAIR',
            'RESTRICT', 'REVOKE', 'SCHEMA', 'SCHEMAS', 'SEMI', 'SHARED',
            'SHOW_DATABASE', 'SSL', 'STATISTICS', 'STREAMTABLE', 'STRUCT',
            'THEN', 'TOUCH', 'TRIGGER', 'UNARCHIVE', 'UNDO', 'UNIONTYPE',
            'UNIQUEJOIN', 'UNLOCK', 'UNSIGNED', 'UPDATE', 'USE', 'UTC',
            'VIEW', 'WHEN', 'WHILE']


def escape_field(field):
    return field.upper().strip() in keywords and "`%s`" % field.strip()\
        or field.strip()


def join_fields(fields):
    return ', '.join([escape_field(field) for field in fields])


def parse_schema_and_options(dbname, tbname, hiveInit):
    hv = Hive()
    hv.Add(hiveInit)
    statement = 'use %s; desc extended %s;' % (dbname, tbname)
    output = hv.fetchall_hql(statement)
    import Schema
    return Schema.parse_schema_and_options(output)
