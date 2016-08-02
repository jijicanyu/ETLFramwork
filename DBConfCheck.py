#!/usr/bin/env python
# coding=gb2312

import MySQLdb
from DBConf import DBConf
import datetime
import sys
import re
import subprocess


def DBConfCheck(dbName):
    connect_terms = []
    while True: 
        host = DBConf[dbName]["host"]
        user = DBConf[dbName]["user"]
        passwd = DBConf[dbName]["passwd"]
        db = DBConf[dbName]["db"]
        local_infile = DBConf[dbName]["local_infile"]
        high_performance = DBConf[dbName].get("high_performance", 0)
        port = DBConf[dbName].get("port", 0)
        backup_term = DBConf[dbName]["backup_term"]

        ## where host field is cmd mode, get return of cmd as host
        host_cmd_rule = re.compile("^\$\{.*\}$")
        find_str = host_cmd_rule.findall(host.strip())
        ## found the cmd
        if len(find_str) == 1:
            host_cmd = find_str[0].strip("$").strip("{").strip("}")
            p = subprocess.Popen(host_cmd,shell=True,stdout = subprocess.PIPE)
            (stdout,stderr) = p.communicate()
            returncode = p.returncode
            if 0 != returncode:
                raise Exception("EXEC host cmd Failed")
            host = stdout.strip()

        try:
            if port:
                conn = MySQLdb.connect(
                    host = host,
                    user = user,
                    passwd = passwd,
                    db = db,
                    port = port,
                    local_infile = local_infile)
            else:
                conn = MySQLdb.connect(
                    host = host,
                    user = user,
                    passwd = passwd,
                    db = db,
                    local_infile = local_infile)
            break
        except Exception as e:
            error = "%s with error %s"%(dbName,e)
            sys.stdout.flush()
            connect_terms.append(error)
            if backup_term != None:
                dbName = backup_term
            else:
                raise Exception("MYSQL database Connection ERROR(S): %s"%(",".join(connect_terms)))
    cursor = conn.close()
    return dbName
 
