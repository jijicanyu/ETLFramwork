#!/usr/bin/env python
# coding=gbk

import os
import traceback
from NSqlDB import NSqlDB

gsmServerPort = [
    'emp01.baidu.com:15003',
    'emp02.baidu.com:15003']

#通知列表
NotifyConf = {}

## NotifyConf = "database" or "file"
NotifyConf_src = "database"

if NotifyConf_src == "database":
    dbName = 'task_manager_db'
    connect = NSqlDB(dbName)
    connect.useDictCursor()
    
    tableName = 'monitor'
    slct_sql  = "SELECT name, mobile, email FROM %s"%(tableName)
    connect.execute(slct_sql)
    rows = connect.fetchall()
    
    connect.close()

    for row in rows:
        name = row['name']
        del row['name']
        if row['mobile'] == '':
            del row['mobile']
        if row['email'] == '':
            del row['email']
        NotifyConf[name] = row

elif NotifyConf_src == "file":
    NotifyConf["zhangqi"] = {
        'email': 'zhangqi04@baidu.com',
        'mobile': '18621268706'
    } 
    NotifyConf["qincpp"] = {
        'email' : 'yangyuanqin@baidu.com',
        'mobile' : '13917252810'
    }
    NotifyConf["liumingshuo"] = {
        'email' : 'liumingshuo@baidu.com',
        'mobile' : '13816725673'
    }
    NotifyConf["xiangyuanfei"] = {
        'email' : 'xiangyuanfei@baidu.com',
        'mobile' : '18221014315'
    }
    NotifyConf["wangfei"] = {
        'email' : 'wangfei03@baidu.com',
        'mobile' : '13918556451'
    }
    NotifyConf["fangzhiwen"] = {
        'email' : 'fangzhewen01@baidu.com',
        'mobile' : '13811018335'
    }

    NotifyConf["caoying"] = {
        'email' : 'caoying@baidu.com',
        'mobile' : '18612291989'
    }
    NotifyConf["hexinghua"] = {
        'email': 'hexinghua@baidu.com',
        'mobile': '18616769936'
    }
    NotifyConf["wushengfeng"] = {
        'email': 'wushengfeng@baidu.com',
        'mobile': '13816818532'
    }
    NotifyConf["yishaobin"] = {
        'email': 'yishaobin@baidu.com',
        'mobile': '18721920747'
    }
    NotifyConf["hulele"] = {
        'email': 'hulele@baidu.com',
        'mobile': '15202163677'
    }
    NotifyConf["pengbo"] = {
        'email': 'pengbo06@baidu.com',
        'mobile': '13588132314'
    }
    NotifyConf["lijue"] = {
        'email': 'lijue@baidu.com',
        'mobile': '13671549303'
    }
    NotifyConf["wangzijian"] = {
        'email': 'wangzijian@baidu.com',
        'mobile': '18930083867'
    }

#print NotifyConf

def gsmsend(id, taskName, errMsg):
    if id not in NotifyConf:
        return
    if 'mobile' not in NotifyConf[id]:
        return
    
    gsmServerPortArg = ""
    for server in gsmServerPort:
        gsmServerPortArg += " -s " + server
    msg = "task %s FAILED: %s" % (taskName, errMsg)
    os.system('gsmsend %s %s@"%s"' % (gsmServerPortArg, NotifyConf[id]['mobile'], msg))
    
def email(id, taskName, errMsg):
    if id not in NotifyConf:
        return
    if 'email' not in NotifyConf[id]:
        return

    import smtplib
    from email.mime.text import MIMEText
    content = "task %s FAILED: %s\n%s" % (taskName, errMsg, traceback.format_exc())
    msg = MIMEText(content, _subtype='html',_charset='gb2312')
    msg['Subject'] = "task %s FAILED" % (taskName)
    msg['From'] = "ubi-monitor@baidu.com"
    msg['To'] = NotifyConf[id]['email']
    text = msg.as_string()
    if 0!=os.system("echo '%s' | /usr/sbin/sendmail '%s'" % (text, msg['To'])):
        raise Exception("Oops! something wrong has happened while sending email!")
    
def alarm(notifyId, taskName, errMsg):
    if not notifyId:
        return
    for id in notifyId:
        gsmsend(id, taskName, errMsg)
        email(id, taskName, errMsg)



