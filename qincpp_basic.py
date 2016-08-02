#
#
#
#function
#********
#name:  get_database()
#input: {'argv_name': value}
#output:    MySQLdb.connection




import sys
import time    
import datetime
import MySQLdb
import os

DATABASE_DEFAULT_PORT = 3306

kprq_adc = {
            0:[16],
            1:[64],
            2:[256],
            4:[1,2,3,4,6,8,25,33,34,129,130,132,136],
            5:[0]
            }
mt_mt = {
            0:[(2, 1)],
            1:[(1, 1)],
            2:[(1, 2)],
            3:[(1, 3), (1, 5)],
            
        }
fea_fea = {
            0:[0],
            1:[1],
            2:[2],
            4:[4],
            8:[8],
            }

def get_select_order_2(select_list, tablename_list):
    select = "select"

    flag = False
    for l in select_list:
        if flag:
            select += ", %s"%(l)
        else:
            select += " %s"%(l)
            flag = True

    flag = False
    for tablename in tablename_list:
        if flag:
            select += ", %s"%(tablename)
        else:
            select += " from %s"%(tablename)
            flag = True
    select += ";"
    return select

def get_select_order(select_list, tablename_list, where, group_list, order_list):
    select = get_select_order_2(select_list, tablename_list)
    select = select[0:len(select)-1]

    if len(where) > 0:
        select += " where %s"%(where)

    flag = False
    for group in group_list:
        if flag:
            select += ", %s"%(group)
        else:
            select += " group by %s"%(group)
            flag = True

    flag = False
    for order in order_list:
        if flag:
            select += ", %s"%(order)
        else:
            select += " order by %s"%(order)
            flag = True

    select += ";"
    return select

    
    

def translation(map, item):
    for key,value in map.items():
        for i in value:
            if (i == item):
                return key
    return -1

def adc_to_kprq(adc):
    return translation(kprq_adc, adc)

def adsrcmt_to_mt(adsrcmt):
    return translation(mt_mt, adsrcmt)


def get_date():
    if len(sys.argv) > 1:
        date_str = sys.argv[1]
        date = datetime.datetime(int(date_str[0:4]), int(date_str[4:6]), int(date_str[6:8]))
    else:
        tt = time.localtime()
        date = datetime.datetime(tt[0], tt[1], tt[2])

    return date

def get_database(db_info = {}):
        
    if "host" in db_info:
        host = db_info['host']
    else:
        host = "localhost"
          
    if "user" in db_info:
        user = db_info['user']
    else:
        user = ""

    if "passwd" in db_info:
        passwd = db_info['passwd']
    else:
        passwd = ""
        
    if "db" in db_info:
        db = db_info['db']
    else:
        db = ""
            
    if "port" in db_info:
        port = db_info['port']
    else:
        port = DATABASE_DEFAULT_PORT     


    if "local_infile" in db_info:
        local_infile = db_info['local_infile']
    else:
        local_infile = 0
        
    return database_connect(host, user, passwd, db, port, local_infile)

    
def database_connect(host, user, passwd, db, port, local_infile):
    db = MySQLdb.connect(host = host, user = user, passwd = passwd, db = db, port = port, local_infile = local_infile)
    return db

def get_insert_order(tablename, lines):
    if len(lines) == 0:
        return ""
    insert = "insert into %s values "%(tablename)
    flag1 = False
    for line in lines:
        if flag1:
            insert += ", "
        else:
            flag1 = True
        insert += "("
        flag2 = False
        for item in line:
            if flag2:
                insert += ", "
            else:
                flag2 = True
            insert += "%s"%item
        insert += ")"
    return insert

def is_table_exist(cursor, tablename):
    cursor.execute("show tables;")
    tables = cursor.fetchall()
    for line in tables:
        table = line[0]
        if table == tablename:
            return True

    return False
       
def get_load_local_file_order(filename, tablename):
    load_file = """LOAD DATA LOCAL INFILE '%s' REPLACE INTO TABLE %s; """%(filename, tablename)
    return load_file
    
def get_load_file_order(filename, tablename):
    load_file = """LOAD DATA INFILE '%s' REPLACE INTO TABLE %s; """%(filename, tablename)
    return load_file

def download(host, filename, log = "", output = ""):
    wget = "wget %s:%s"%(host, filename)
    if isinstance(log, str) and len(log) > 0:
        wget += " -o %s"%(log)
    if isinstance(output, str) and len(output) > 0:
        wget += " -O %s"%(output)
    os.system(wget)

def nslog_output_to_database(filename, tablename, cursor, create_order = ""):
    if not os.path.exists(filename) or not os.path.exists("mark."+filename):
        return  False
    cursor.execute("show tables;")
    tables = cursor.fetchall()
    flag = False
    for table in tables:
        if table[0] == tablename:
            flag = True
            break
    if not flag:
        cursor.execute(create_order)

    cursor.execute(get_load_file_order(os.getcwd()+"/"+filename, tablename))
    
    return True
