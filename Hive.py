#!/usr/bin/env python
# coding=gbk
import subprocess
import datetime
import sys
import pipes


from HiveConfig import hiveConfig

class Hive:
    def __init__(self, path=None):
        if path is None:
            path = hiveConfig.get_command()
        else:
            hiveConfig.set_engine(path)
        self.path = path
        self.hql=""

    def WriteLog(self, logStr):
        print "[%s] %s"%(datetime.datetime.now(), logStr)
        sys.stdout.flush()

    def WriteLogToErr(self, logStr):
        sys.stderr.write("[%s] %s\n"%(datetime.datetime.now(), logStr))
        sys.stdout.flush()

    def Execute(self, hql, debug = False):
        hql = hiveConfig.get_namespace() + hql
        if debug:
            self.WriteLogToErr(hql)

        cmd = '%s -e %s' % (self.path, pipes.quote(hql))
        p = subprocess.Popen(cmd, shell = True, stdout = subprocess.PIPE)
        (stdoutdata, stderrdata) = p.communicate()
        process_status = p.returncode
        if process_status != 0:
            return_info = stdoutdata
            raise Exception("Hive Exec err: %s, cmd: %s"%(return_info, cmd))

    def Add(self, sql):
        if sql!=None:
            self.hql +="\n"+sql

    def ClearHql(self):
        self.hql = ""

    def ExecuteAll(self, debug=False):
        hql = hiveConfig.get_namespace() + self.hql
        if debug:
            self.WriteLogToErr(hql)
        cmd = '%s -e %s' % (self.path, pipes.quote(hql))
        p = subprocess.Popen(cmd, shell = True, stdout = subprocess.PIPE)
        (stdoutdata, stderrdata) = p.communicate()
        process_status = p.returncode
        self.ClearHql()
        if(process_status!=0):
            return_info = stdoutdata
            raise Exception("Hive Exec err: %s, cmd: %s, code:%s"%(return_info, cmd, process_status))

    def fetchall_hql(self, hql,debug=False):
        hql = hiveConfig.get_namespace() + hql
        if debug:
            self.WriteLogToErr(hql)
        cmd = self.path + " -e \"" + hql + "\""
        p = subprocess.Popen(cmd, shell = True, stdout = subprocess.PIPE)
        (stdoutdata, stderrdata) = p.communicate()
        process_status = p.returncode
        result = stdoutdata
        if process_status != 0:
            return_info = result
            raise Exception("Hive Exec err: %s, cmd: %s"%(return_info, cmd))

        return result

    def fetchall(self, debug=False):
        hql = hiveConfig.get_namespace() + self.hql
        if debug:
            self.WriteLogToErr(hql)
        cmd = self.path + " -e \"" + hql + "\""
        p = subprocess.Popen(cmd, shell = True, stdout = subprocess.PIPE)
        (stdoutdata, stderrdata) = p.communicate()
        process_status = p.returncode
        result = stdoutdata
        self.ClearHql()
        if process_status != 0:
            return_info = result
            raise Exception("Hive Exec err: %s, cmd: %s"%(return_info, cmd))

        return result

    def GetHql(self):
        return self.hql

    def ExecuteCMD(self, cmd, debug = False):
        if debug:
            self.WriteLogToErr(cmd)
        p = subprocess.Popen(cmd, shell = True, stdout = subprocess.PIPE)
        stdoutdata = p.communicate()[0]
        process_status = p.returncode
        if(process_status!=0):
            return_info = stdoutdata
            raise Exception("Hive Exec err: %s, cmd: %s"%(return_info, cmd))
