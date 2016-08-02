#!/usr/bin/env python
# coding=gb2312
import MySQLdb

DBConf={}
#### dbconf_src = "database" or "file"
dbconf_src = "database"

if dbconf_src == "database":
    host = 'yf-cm-dastat54.yf01'
    user = 'ubi_work_p'
    passwd = '1jlauo7952jlJF'
    db = 'dbconf'
    port = 9999
    local_infile = 1
    connect = MySQLdb.connect(
        host = host,
        user = user,
        passwd = passwd,
        db = db,
        port = port,
        local_infile = local_infile)
    cursor = connect.cursor(MySQLdb.cursors.DictCursor)
    table_name = "dbconf"
    select = "select * from %s"%(table_name)
    cursor.execute(select)
    rows = cursor.fetchall()
    cursor.close()
    connect.close()
    for row in rows:
        term = row['term']
        del row['term']
        row['ignore_scheme'] = (row['ignore_scheme'] == 'True')
        DBConf[term] = row

elif dbconf_src == "file":
    DBConf['test'] = {
        'host':     'yf-cm-dastat02.yf01',
        'user':     'ubi_work',
        'passwd':   'ubi@baidu496',
        'db':       'test',
        'local_infile': 1}
    
    DBConf['test_01'] = {
        'host':     'yf-cm-dastat01.yf01',
        'user':     'ubi_work',
        'passwd':   'ubi@baidu496',
        'db':       'test',
        'local_infile': 1}
        
    DBConf['remote'] = {
        'host':     'yf-cm-dastat03.yf01',
        'user':     'ubi_work',
        'passwd':   'ubi@baidu496',
        'db':       'test',
        'local_infile': 1}
    
    DBConf['source_adid_db'] = {
        'host':     'localhost',
        'user':     'work',
        'passwd':   'work',
        'db':       'adid_daily_db',
        'local_infile': 1}
        
    DBConf['source_adlib_db'] = {
        'host':     'localhost',
        'user':     'ubi_work',
        'passwd':   'ubi@baidu496',
        'db':       'adlib_db',
        'local_infile': 1}
     
    DBConf['remote_source_adlib_db'] = {
        'host':     'yf-cm-dastat02.yf01',
        'user':     'ubi_work',
        'passwd':   'ubi@baidu496',
        'db':       'adlib_db',
        'local_infile': 1}
    
    DBConf['remote_source_adlib_new_db'] = {
        'host':     'yf-cm-dastat02.yf01',
        'user':     'ubi_work',
        'passwd':   'ubi@baidu496',
        'db':       'adlib_new_db',
        'local_infile': 1}
        
    DBConf['plan_budget_db'] = {
        'host':     'localhost',
        'user':     'ubi_work',
        'passwd':   'ubi@baidu496',
        'db':       'plan_budget_daily_db',
        'high_performance': 1,
        'local_infile': 1}
     
    DBConf['plan_budget_02'] = {
        'host':     'yf-cm-dastat02.yf01',
        'user':     'ubi_work',
        'passwd':   'ubi@baidu496',
        'high_performance': 1,
        'db':       'plan_budget_daily_db',
        'local_infile': 1}
    
    
    DBConf['source_holmes_db'] = {
        'host':     'localhost',
        'user':     'ubi_work',
        'passwd':   'ubi@baidu496',
        'db':       'holmes_db',
        'local_infile': 1}
    
    DBConf['userstat'] = {
        'host':     'localhost',
        'user':     'ubi_work',
        'passwd':   'ubi@baidu496',
        'db':       'userstat_db',
        'local_infile': 1}
    
    DBConf['overall'] = {
        'host':     'localhost',
        'user':     'work',
        'passwd':   'work',
        'db':       'nova_data_analyse_source',
        'local_infile': 1}
    DBConf['overall_remote'] = {
        'host':     'yf-cm-dastat02.yf01',
        'user':     'ubi_work',
        'passwd':   'ubi@baidu496',
        'db':       'nova_data_analyse_source',
        'local_infile': 1}
        
    DBConf['site_pv'] = {
        'host':     'localhost',
        'user':     'work',
        'passwd':   'work',
        'db':       'site_pv_daily_db',
        'local_infile': 1}
    
    DBConf['source_xpid_db'] = {
        'host':     'localhost',
        'user':     'work',
        'passwd':   'work',
        'db':       'expid_db',
        'local_infile': 1}
    DBConf['source_expid_db_remote'] = {
        'host':     'yf-cm-dastat01.yf01',
        'user':     'ubi_work',
        'passwd':   'ubi@baidu496',
        'db':       'expid_db',
        'high_performance': 1,
        'local_infile': 1}
    DBConf['exhibit_expid_db'] = {
        'host':     'yf-cm-dastat03.yf01',
        'user':     'ubi_work',
        'passwd':   'ubi@baidu496',
        'db':       'expid_db',
        'local_infile': 1}
    DBConf['exhibit_expid_db_test'] = {
        'host':     'yf-cm-dastat03.yf01',
        'user':     'ubi_work',
        'passwd':   'ubi@baidu496',
        'db':       'test',
        'local_infile': 1}
    DBConf['exhibit_user_db'] = {
        'host':     'yf-cm-dastat03.yf01',
        'user':     'ubi_work',
        'passwd':   'ubi@baidu496',
        'db':       'user_db',
        'local_infile': 1}
    DBConf['exhibit_overall'] = {
        'host':     'yf-cm-dastat03.yf01',
        'user':     'ubi_work',
        'passwd':   'ubi@baidu496',
        'db':       'overall',
        'local_infile': 1} 
    
    DBConf['user_db'] = {
        'host':     'yf-cm-dastat03.yf01',
        'user':     'ubi_work',
        'passwd':   'ubi@baidu496',
        'db':       'user_db',
        'local_infile': 1} 
    
    DBConf['site_adtrade_db'] = {
        'host':     'localhost',
        'user':     'work',
        'passwd':   'work',
        'db':       'site_adtrade_daily_db',
        'local_infile': 1}
    
    DBConf['work'] = {
        'host':     'localhost',
        'user':     'work',
        'passwd':   'work',
        'db': 'expid_temp_db',
        'local_infile': 1
    }
    
    DBConf['expid_04'] = {
        'host':     'yf-cm-dastat04.yf01',
        'user':     'work',
        'passwd':   'work',
        'db': 'exp_db',
        'local_infile': 1
    }
    DBConf['expid_05'] = {
        'host':     'yf-cm-dastat05.yf01',
        'user':     'work',
        'passwd':   'work',
        'db': 'exp_db',
        'local_infile': 1
    }
    DBConf['expid_06'] = {
        'host':     'yf-cm-dastat06.yf01',
        'user':     'work',
        'passwd':   'work',
        'db': 'exp_db',
        'local_infile': 1
    }
    DBConf['expid_07'] = {
        'host':     'yf-cm-dastat07.yf01',
        'user':     'work',
        'passwd':   'work',
        'db': 'exp_db',
        'local_infile': 1
    }
    DBConf['expid_07_tmp'] = {
        'host':     'yf-cmda-stat00.yf01.baidu.com',
        'user':     'work',
        'passwd':   'work',
        'db': 'exp_db',
        'local_infile': 1
    }
    DBConf['expid_08'] = {
        'host':     'yf-cm-dastat08.yf01',
        'user':     'work',
        'passwd':   'work',
        'db': 'exp_db',
        'local_infile': 1
    }
    DBConf['bid_04'] = {
        'host':     'yf-cm-dastat04.yf01',
        'user':     'work',
        'passwd':   'work',
        'db': 'bid_db',
        'local_infile': 1
    }
    DBConf['bid_05'] = {
        'host':     'yf-cm-dastat05.yf01',
        'user':     'work',
        'passwd':   'work',
        'db': 'bid_db',
        'local_infile': 1
    }
    DBConf['bid_06'] = {
        'host':     'yf-cm-dastat06.yf01',
        'user':     'work',
        'passwd':   'work',
        'db': 'bid_db',
        'local_infile': 1
    }
    DBConf['bid_07'] = {
        'host':     'yf-cm-dastat07.yf01',
        'user':     'work',
        'passwd':   'work',
        'db': 'bid_db',
        'local_infile': 1
    }
    DBConf['bid_07_tmp'] = {
        'host':     'yf-cmda-stat00.yf01',
        'user':     'work',
        'passwd':   'work',
        'db': 'bid_db',
        'local_infile': 1
    }
    
    DBConf['bid_08'] = {
        'host':     'yf-cm-dastat08.yf01',
        'user':     'work',
        'passwd':   'work',
        'db': 'bid_db',
        'local_infile': 1
    }
    
    DBConf['source_site_db_remote'] = {
        'host':     'yf-cm-dastat17.yf01',
        'user':     'ubi_work',
        'passwd':   'ubi@baidu496',
        'db':  'site_db',
        'local_infile': 1,
        'port':8029
    }
    DBConf['source_adid_db_remote'] = {
        'host':     'yf-cm-dastat18.yf01',
        'user':     'ubi_work',
        'passwd':   'ubi@baidu496',
        'db': 'adid_db',
        'local_infile': 1
    }
    DBConf['source_adid_db_remote_pro'] = {
        'host':     'yf-cm-dastat18.yf01',
        'user':     'ubi_work',
        'passwd':   'ubi@baidu496',
        'db': 'adid_db',
        'local_infile': 1,
        'high_performance': 1
    }
    DBConf['source_site_db_remote_pro'] = {
        'host':     'yf-cm-dastat17.yf01',
        'user':     'ubi_work',
        'passwd':   'ubi@baidu496',
        'db': 'site_db',
        'local_infile': 1,
        'high_performance': 1,
        'port':8029,
    }
    DBConf['source_uit_db_remote'] = {
        'host':     'yf-cm-dastat01.yf01',
        'user':     'ubi_work',
        'passwd':   'ubi@baidu496',
        'db': 'uit_db',
        'local_infile': 1
    }
    
    DBConf['sub_user_overall_db'] = {
        'host':     'yf-cm-xbdbs156.yf01',
        'user':     'ubi_work',
        'passwd':   'ubi@baidu496',
        'db': 'cm_kpi_orig',
        'port': 8029,
        'local_infile': 1
    }
    
    DBConf['user_overall_db'] = {
        'host':     'yf-cm-dastat03.yf01',
        'user':     'ubi_work',
        'passwd':   'ubi@baidu496',
        'db': 'user_overall_db',
        'local_infile': 1
    }
    
    DBConf['nova_stat_source'] = {
        'host':     'yf-cm-dastat02.yf01',
        'user':     'ubi_work',
        'passwd':   'ubi@baidu496',
        'db':       'nova_data_analyse_source',
        'local_infile': 1
    }
    DBConf['site_adtrade_02'] = {
        'host':     'yf-cm-dastat02.yf01',
        'user':     'ubi_work',
        'passwd':   'ubi@baidu496',
        'db':       'site_adtrade_daily_db',
        'local_infile': 1
    }
    DBConf['site_pv_02'] = {
        'host':     'yf-cm-dastat02.yf01',
        'user':     'ubi_work',
        'passwd':   'ubi@baidu496',
        'db':       'site_pv_daily_db',
        'local_infile': 1
    }
    DBConf['holmes_remote'] = {
        'host':     'tc-cm-pricing00.tc.baidu.com',
        'user':     'rd',
        'passwd':   '123456',
        'db':       'holmes',
        'local_infile': 1
    }
    DBConf['holmes_remote_new'] = {
        'host' :    'yf-cm-dastat01.yf01',
        'user' :    'ubi_work',
        'passwd' :  'ubi@baidu496',
        'db' :      'holmes_db',
        'local_infile': 1
    }
    DBConf['holmes_ct'] = {
        'host':     'yf-cm-dastat02.yf01.baidu.com',
        'user':     'ubi_work',
        'passwd':   'ubi@baidu496',
        'db':       'holmes_ct_db',
        'local_infile': 1
    }
    DBConf['daily_winfo'] = {
        'host':     'yf-cm-dastat02.yf01.baidu.com',
        'user':     'ubi_work',
        'passwd':   'ubi@baidu496',
        'db':       'daily_winfo_db',
        'local_infile': 1
    }
    DBConf['userdb01'] = {
        'host':     'localhost',
        'user':     'work',
        'passwd':   'work',
        'db':       'user_db',
        'local_infile': 1
    }
    
    
    DBConf['source_holmes_db_remote'] = {
        'host':     'yf-cm-dastat01.yf01',
        'user':     'ubi_work',
        'passwd':   'ubi@baidu496',
        'db':       'holmes_db',
        'local_infile': 1}
    
    DBConf['cpro_stat'] = {
        'host':     '10.23.248.58',
        'user':     'cprostat',
        'passwd':   '123456',
        'db':       'Cpro_Stat',
        'port':     6678,
        'local_infile': 1,
        'ignore_scheme': True}
    
    # data from beidou    
    DBConf['source_beidou_db_remote'] = {
        'host':     'db-bd-stdb-03.db01',
        'user':     'yulu',
        'passwd':   'cpropm',
        'db':       'beidou',
        'local_infile': 1}
    DBConf['source_adtrade_sitetrade_db'] = {
        'host':     'yf-cm-dastat03.yf01',
        'user':     'ubi_work',
        'passwd':   'ubi@baidu496',
        'db':       'expid_db',
        'local_infile': 1}
        
    DBConf['source_cm_kpi_orig'] = {
        'host':     'yf-cm-dastat02.yf01',
        'user':     'ubi_work',
        'passwd':   'ubi@baidu496',
        'db':       'cm_kpi_orig',
        'high_performance': 1,
        'local_infile': 1}
    DBConf['pfs_stat_remote'] = {
        'host':     'yf-cm-dastat02.yf01',
        'user':     'ubi_work',
        'passwd':   'ubi@baidu496',
        'db':       'pfs_data_db',
        'local_infile': 1}
        
    DBConf['test_02'] = {
        'host':     'yf-cm-dastat02.yf01',
        'user':     'ubi_work',
        'passwd':   'ubi@baidu496',
        'db':       'test',
        'local_infile': 1}
    DBConf['cm_kpi_db'] = {
        'host':     'yf-cm-dastat03.yf01',
        'user':     'ubi_work',
        'passwd':   'ubi@baidu496',
        'db':       'cm_kpi',
        'local_infile': 1}
    
    DBConf['tmp_db_03'] = {
        'host':     'yf-cm-dastat03.yf01',
        'user':     'ubi_work',
        'passwd':   'ubi@baidu496',
        'db':       'tmp_db',
        'local_infile': 1}
        
    DBConf['union4'] = {
        'host':     '10.38.15.38',
        'user':     'caoying',
        'passwd':   'GeHMlwR0yI6T7Q96JYfc',
        'db':       'union4',
        'port':  3618,
        'local_infile': 0}
    DBConf['domain_db'] = {
        'host':     'yf-cm-dastat02.yf01',
        'user':     'ubi_work',
        'passwd':   'ubi@baidu496',
        'db':       'domain_db',
        'local_infile': 1}
    DBConf['fcquery_db'] = {
        'host':     'yf-cm-dastat02.yf01',
        'user':     'ubi_work',
        'passwd':   'ubi@baidu496',
        'db':       'fcquery_db',
        'local_infile': 1}
    
    DBConf['6600_db'] = {
        'host':     '10.23.240.129',
        'user':     'baiou',
        'passwd':   'baiou',
        'port':     6600,
        'db':       'SF_User',
        'local_infile': 1}
    
    DBConf['operation_db'] = {
        'host':     'yf-cm-dastat02.yf01',
        'user':     'ubi_work',
        'passwd':   'ubi@baidu496',
        'db':       'operation_db',
        'local_infile': 1}
    
    DBConf['adlib_new'] = {
        'host':     'yf-cm-dastat02.yf01',
        'user':     'ubi_work',
        'passwd':   'ubi@baidu496',
        'db':       'adlib_new_db',
        'local_infile': 1}
    
    DBConf['beidoustat_db'] = {
        'host':     'db-bd-stdb-03.db01.baidu.com',
        'user':     'yulu',
        'passwd':   'cpropm',
        'db':       'beidou',
        'local_infile': 1}
    DBConf['linkunit_db'] = {
        'host':     'yf-cm-dastat03.yf01',
        'user':     'ubi_work',
        'passwd':   'ubi@baidu496',
        'db':       'linkunit_db',
        'local_infile': 1}
        
    DBConf['tmp_02'] = {
        'host':     'yf-cm-dastat02.yf01',
        'user':     'ubi_work',
        'passwd':   'ubi@baidu496',
        'db':       'tmp_db',
        'local_infile': 1}
    
    DBConf['click_db'] = {
        'host':     'yf-cm-dastat05.yf01',
        'user':     'ubi_work',
        'passwd':   'ubi@baidu496',
        'db':       'click_db',
        'local_infile': 1}
    
    DBConf['domain_userid_db'] = {
        'host':  'yf-cm-ecompm03.yf01.baidu.com',
        'user':  'ubi_work',
        'passwd': 'ubi@baidu496',
        'port': 8749,
        'db':   'base_query',
        'local_infile': 1}
    DBConf['6601_db'] = {
        'host':  '10.38.65.59',
        'user':  'baiou',
        'passwd': 'baiou',
        'port': 6601,
        'db':   'SF_Click',
        'local_infile': 1,
        'ignore_scheme': True}
    DBConf['SF_click_db'] = {
        'host':     'yf-cm-dastat05.yf01',
        'user':     'ubi_work',
        'passwd':   'ubi@baidu496',
        'db':       'SF_click_db',
        'local_infile': 1}
    DBConf['user_plan_ign_db'] = {
        'host':     'yf-cm-dastat02.yf01',
        'user':     'ubi_work',
        'passwd':   'ubi@baidu496',
        'db':       'user_plan_ign_db',
        'local_infile': 1}
    
    DBConf['kpi_module_db'] = {
        'host':     'yf-cm-dastat03.yf01',
        'user':     'ubi_work',
        'passwd':   'ubi@baidu496',
        'db':       'kpi_module_db',
        'local_infile': 1}
    
    DBConf['task_manager_db'] = {
        'host':     'yf-cm-dastat02.yf01',
        'user':     'ubi_work',
        'passwd':   'ubi@baidu496',
        'db':       'task_manager_db',
        'local_infile': 1}
        
    DBConf['it_stat_data_db'] = {
        'host':     'yf-cm-dastat03.yf01',
        'user':     'ubi_work',
        'passwd':   'ubi@baidu496',
        'db':       'it_stat_data_db',
        'local_infile': 1}
    
    DBConf['beidou_db'] = {
        'host':  'yf-cm-ecompm03.yf01.baidu.com',
        'user':  'ubi_work',
        'passwd': 'ubi@baidu496',
        'port': 8749,
        'db':   'base_conf',
        'local_infile': 1}
        
    
    DBConf['beidou_detail'] = {
        'host':  'yf-cm-ecompm03.yf01.baidu.com',
        'user':  'ubi_work',
        'passwd': 'ubi@baidu496',
        'port': 8749,
        'db':   'base_detail',
        'local_infile': 1}
    
    DBConf['backup_adid_db'] = {
        'host':  'yf-cm-bdbs11045.yf01.baidu.com',
        'user':  'ubi_work',
        'passwd': 'ubi@baidu496',
        'port': 8029,
        'db':   'adid_db',
        'local_infile': 1}
    
    DBConf['backup_site_db'] = {
        'host':  'yf-cm-bdbs11045.yf01.baidu.com',
        'user':  'ubi_work',
        'passwd': 'ubi@baidu496',
        'port': 8029,
        'db':   'site_db',
        'local_infile': 1}
        
    DBConf['backup_holmes_db'] = {
        'host':  'yf-cm-bdbs11045.yf01.baidu.com',
        'user':  'ubi_work',
        'passwd': 'ubi@baidu496',
        'port': 8029,
        'db':   'holmes_db',
        'local_infile': 1}
        
    DBConf['backup_groupid_budget_db'] = {
        'host':  'yf-cm-bdbs11045.yf01.baidu.com',
        'user':  'ubi_work',
        'passwd': 'ubi@baidu496',
        'port': 8029,
        'db':   'groupid_budget_db',
        'local_infile': 1}
    
    DBConf['beidoustat_db_stat'] = {
        'host':  '10.42.46.43',
        'user':  'ubi_off_r',
        'passwd': 'jEHowHtz8d4Mvv9BEPgdQTHT6VIxqkRz',
        'port': 3306,
        'db':   'beidoustat',
        'local_infile': 1}
    
    DBConf['backup_groupid_db'] = {
        'host':  'yf-cm-bdbs11045.yf01.baidu.com',
        'user':  'ubi_work',
        'passwd': 'ubi@baidu496',
        'port': 8029,
        'db':   'groupid_db',
        'local_infile': 1}
    
    DBConf['backup_cm_kpi_orig'] = {
        'host':     'yf-cm-xbdbs156.yf01',
        'user':     'ubi_work',
        'passwd':   'ubi@baidu496',
        'port': 8029,
        'db':       'cm_kpi_orig',
        'local_infile': 1}
    
    DBConf['budget_detail_db'] = {
        'host':     'yf-cm-bdbs22031.yf01',
        'user':     'ubi_work',
        'passwd':   'ubi@baidu496',
        'db':       'budget_detail_db',
        'local_infile': 1,
        'port': 8029}
    
    
    DBConf['pm_holmes'] = {
        'host':     'yf-cm-ecompm02.yf01.baidu.com',
        'user':     'admin',
        'passwd':   'ecom1234',
        'db':       'holmes',
        'local_infile': 1,
        'port': 8665}
                          
    DBConf['beidou_db'] = {
        'host':     '10.81.50.231',
        'user':     'yulu',
        'passwd':   'cpropm',
        'db':       'beidoufinan',
        'local_infile': 1,
        'port':3306}
                          
    DBConf['beidou_db_bak'] = {
        'host':     '10.26.186.35',
        'user':     'yulu',
        'passwd':   'cpropm',
        'db':       'beidoufinan',
        'local_infile': 1,
        'port':3306}
    DBConf['dwelltime_db'] = {
        'host':     'yf-cm-bdbs22031.yf01',
        'user':     'ubi_work',
        'passwd':   'ubi@baidu496',
        'db':       'dwelltime_db',
        'local_infile': 1,
        'port': 8029}
    
    DBConf['test_22031'] = {
        'host':     'yf-cm-bdbs22031.yf01',
        'user':     'ubi_work',
        'passwd':   'ubi@baidu496',
        'db':       'test',
        'local_infile': 1,
        'port': 8029}
    
    DBConf['backup_BD_detail_db'] = {
        'host':     'yf-cm-xbdbs156.yf01',
        'user':     'ubi_work',
        'passwd':   'ubi@baidu496',
        'port': 8029,
        'db':       'BD_detail_db',
        'local_infile': 1}
    
    DBConf['backup_adlib_new_db'] = {
        'host':     'yf-cm-xbdbs156.yf01',
        'user':     'ubi_work',
        'passwd':   'ubi@baidu496',
        'port': 8029,
        'db':       'adlib_new_db',
        'local_infile': 1}
    
    DBConf['holmes_db_16'] = {
        'host':     'yf-cm-dastat16.yf01.baidu.com',
        'user':     'ubi_work',
        'passwd':   'ubi@baidu496',
        'port': 8029,
        'db':       'holmes_db',
        'local_infile': 1}
    
    DBConf['dan_db_remote'] = {
        'host':     '10.36.55.57',                                                                                         
        'user':     'yishaobin',
        'passwd':   'BmYgBGBjnzAkFRrWwHHl',                                                                                
        'db':       'DANDB',                                                                                               
        'local_infile': 1,                                                                                                 
        'port': 5085}
    
    DBConf['danweb_db_remote'] = {
        'host':     '10.46.152.32',
        'user':     'dnweb',
        'passwd':   'report',
        'port': 8100,
        'db':       'DanReport',
        'local_infile': 1}
        
    DBConf['dan_stat_data_db'] = {
        'host':     'yf-cm-dastat03.yf01',
        'user':     'ubi_work',
        'passwd':   'ubi@baidu496',
        'db':       'dan_stat_data_db',
        'local_infile': 1}
    
    DBConf['discover_03'] = {
        'host':     'yf-cm-dastat03.yf01',
        'user':     'ubi_work',
        'passwd':   'ubi@baidu496',
        'db':       'discover',
        'local_infile': 1}
    
    DBConf['ad_stat_db'] = {
        'host':     'yf-cm-dastat03.yf01',
        'user':     'ubi_work',
        'passwd':   'ubi@baidu496',
        'db':       'ad_stat_db',
        'local_infile': 1}
    
    DBConf['discover_15'] = {
        'host':     'yf-cm-dastat15.yf01',
        'user':     'ubi_work',
        'passwd':   'ubi@baidu496',
        'port':     8029,
        'db':       'discover',
        'local_infile': 1}
    
    DBConf['domain_overall_db'] = {
        'host':     'yf-cm-xbdbs156.yf01',
        'user':     'ubi_work',
        'passwd':   'ubi@baidu496',
        'port':     8029,
        'db':       'domain_overall_db',
        'local_infile': 1}
    
    DBConf['MH_db'] = {
        'host':     'yf-cm-dastat03.yf01',
        'user':     'ubi_work',
        'passwd':   'ubi@baidu496',
        'db':       'MH_db',
        'local_infile': 1}
    
    
    DBConf['test_16'] = {
        'host':     'yf-cm-dastat16.yf01.baidu.com',
        'user':     'ubi_work',
        'passwd':   'ubi@baidu496',
        'port': 8029,
        'db':       'test',
        'local_infile': 1}
    
    DBConf['mysql_156'] = {
        'host':     'yf-cm-xbdbs156.yf01',
        'user':     'ubi_work',
        'passwd':   'ubi@baidu496',
        'port': 8029,
        'db':       'mysql',
        'local_infile': 1}
    
    DBConf['visit_log'] = {
        'host':     'yf-cm-dastat03.yf01',
        'user':     'ubi_work',
        'passwd':   'ubi@baidu496',
        'db':       'visit_log',
        'local_infile': 1}
    
    DBConf['mysql_11045'] = {
        'host':     'yf-cm-bdbs11045.yf01',
        'user':     'ubi_work',
        'passwd':   'ubi@baidu496',
        'port': 8029,
        'db':       'mysql',
        'local_infile': 1}
    
    DBConf['mysql_22031'] = {
        'host':     'yf-cm-bdbs22031.yf01',
        'user':     'ubi_work',
        'passwd':   'ubi@baidu496',
        'port': 8029,
        'db':       'mysql',
        'local_infile': 1}
    
    DBConf['backup_groupid_db'] = {
        'host':  'yf-cm-bdbs11045.yf01.baidu.com',
        'user':  'ubi_work',
        'passwd': 'ubi@baidu496',
        'port': 8029,
        'db':   'groupid_db',
        'local_infile': 1}
    
        
    DBConf['beidouurl_db_stat'] = {
        'host':  '10.46.29.21',
        'user':  'ubi_off_r',
        'passwd': 'jEHowHtz8d4Mvv9BEPgdQTHT6VIxqkRz',
        'port': 3306,
        'db':   'beidouurl',
        'local_infile': 1}
    
    DBConf['SF_adview'] = {
        'host': 'yf-cm-xbdbs156.yf01',
        'user': 'ubi_work',
        'passwd': 'ubi@baidu496',
        'port': 8029,
        'db': 'SF_adview_db',
        'local_infile': 1}           

