#!/usr/bin/env python
# coding=utf8

import time
import random
import os
import datetime
from DBConf import DBConf
from NLog import NLogger
from NConfParser import NConfParser
from NSqlDB import NSqlDB
from _PaloClient import _PaloClient

# 引入字段/表结构类型
Schema = _PaloClient.Schema


class PaloRuntimeError(RuntimeError):
    """
    Palo 运行中产生的错误，一般通过调用 log_error 产生。
    """
    def __init__(self, data):
        RuntimeError.__init__(self, repr(data))


class TableFamilyInfo(object):
    """
    提供创建 table family 时所需的所有信息。
    """
    def __init__(self, table_name, schema,
                 short_key=0,
                 partition_method='key_range',
                 partition_columns=None,
                 key_ranges=None,
                 hash_mod=61,
                 hash_method='full_key',
                 data_file_type='row'):

        self.schema = schema
        self.basic_table = _PaloClient.BasicTable(
            table_name,
            short_key=short_key,
            partition_method=partition_method,
            partition_columns=partition_columns,
            key_ranges=key_ranges,
            hash_mod=hash_mod,
            hash_method=hash_method,
            data_file_type=data_file_type)
        self.rollup_tables = []

    def add_rollup_dict(self, input):
        """
        添加上卷表，使用 basic_table 中的配置作为缺省值。
        """
        if isinstance(input, dict):
            d = dict(input)
        else:
            d = input()
        default = self.basic_table()
        if 'short_key' not in d:
            d['short_key'] = default['short_key']
        if 'partition_method' not in d:
            d['partition_method'] = default['partition_method']
        if 'partition_columns' not in d:
            if 'partition_columns' in default:
                d['partition_columns'] = default['partition_columns']
            else:
                d['partition_columns'] = None
        if 'key_ranges' not in d:
            if 'key_ranges' in default:
                d['key_ranges'] = default['key_ranges']
            else:
                d['key_ranges'] = None
        if 'hash_mod' not in d:
            if 'hash_mod' in default:
                d['hash_mod'] = default['hash_mod']
            else:
                d['hash_mod'] = 61
        if 'hash_method' not in d:
            if 'hash_method' in default:
                d['hash_method'] = default['hash_method']
            else:
                d['hash_method'] = 'full_key'
        if 'data_file_type' not in d:
            d['data_file_type'] = default['data_file_type']
        if 'index_name' not in d:
            if 'base_index_name' not in d:
                d['index_name'] = 'PRIMARY'
            else:
                d['index_name'] = d['base_index_name']
        if 'base_table_name' not in d:
            d['base_table_name'] = None
        if 'base_index_name' not in d:
            d['base_index_name'] = None
        self.rollup_tables.append(_PaloClient.RollUpTable(
            d['table_name'],
            d['index_name'],
            d['column_ref'],
            short_key=d['short_key'],
            partition_method=d['partition_method'],
            partition_columns=d['partition_columns'],
            key_ranges=d['key_ranges'],
            hash_mod=d['hash_mod'],
            hash_method=d['hash_method'],
            base_table_name=d['base_table_name'],
            base_index_name=d['base_index_name'],
            data_file_type=d['data_file_type'])
        )
        return self

    def add_rollup(self, table_name, index_name, column_ref,
                   base_table_name,
                   short_key=None,
                   partition_method=None,
                   partition_columns=None,
                   key_ranges=None,
                   hash_mod=None,
                   hash_method=None,
                   base_index_name='PRIMARY',
                   data_file_type=None):
        """
        添加上卷表的配置
        """
        # 默认使用基础表的配置
        default = self.basic_table()
        if short_key is None:
            short_key = default['short_key']
        if partition_method is None:
            partition_method = default['partition_method']
        if partition_columns is None and 'partition_columns' in default:
            partition_columns = default['partition_columns']
        if key_ranges is None and 'key_ranges' in default:
            key_ranges = default['key_ranges']
        if hash_mod is None:
            if 'hash_mod' in default:
                hash_mod = default['hash_mod']
            else:
                hash_mod = 61
        if hash_method is None:
            if 'hash_method' in default:
                hash_method = default['hash_method']
            else:
                hash_method = 'full_key'
        if data_file_type is None:
            data_file_type = default['data_file_type']
        self.rollup_tables.append(_PaloClient.RollUpTable(
            table_name,
            index_name,
            column_ref,
            short_key=short_key,
            partition_method=partition_method,
            partition_columns=partition_columns,
            key_ranges=key_ranges,
            hash_mod=hash_mod,
            hash_method=hash_method,
            base_table_name=base_table_name,
            base_index_name=base_index_name,
            data_file_type=data_file_type)
        )
        return self

    def __call__(self):
        basic_info = self.basic_table()
        ret = {basic_info['table_name']: basic_info}
        for table in self.rollup_tables:
            rollup_info = table()
            ret[rollup_info['table_name']] = rollup_info
        return ret


class PaloClient(object):
    """
    在 Palo API 上面添加了一些额外的功能。
    """

    def __init__(self, database):
        """
        使用 NQuery.conf 中的参数连接 Palo DM。
        """
        conf = NConfParser()
        host = conf.get('palo_dm', 'host')
        port = conf.get('palo_dm', 'port', 'INT')
        user = conf.get('palo_dm', 'user')
        passwd = conf.get('palo_dm', 'passwd')
        self.client = _PaloClient(host, port, user, passwd,
                                  debug=NLogger.debug)
        self.db = DBConf[database]['db']
        self.term = database

    def log_error(self, data):
        """
        记录错误日志，并抛出PaloRuntimeError异常。
        """
        NLogger.error(repr(data))
        raise PaloRuntimeError(data)

    def is_db_exist(self, dbname=None):
        """
        检测数据库是否存在，如果存在返回 True。
        不指定 dbname 时，使用初始化时指定的数据库名。
        """
        if dbname is None:
            dbname = self.db
        succ, val = self.client.show_databases()
        return succ and dbname in val.keys()

    def is_table_exist(self, tbname, dbname=None):
        """
        检测指定 table family 是否存在，如果存在返回 True，否则返回 False。
        不指定 dbname 时，使用初始化对象时指定的数据库。
        """
        if dbname is None:
            dbname = self.db
        succ, val = self.client.show_table_families(dbname)
        if not succ:
            self.log_error(val)
        return succ and tbname in val

    def is_rollup_exist(self, table_family_name, rollup_name=None,
                        dbname=None, available_tables_only=False):
        """
        检测指定 table 是否存在，如果存在返回 True，否则返回 False。
        不指定 dbname 时，使用初始化对象时指定的数据库。
        不指定 rollupname 时，使用 tbname，此时结果同 is_table_exist()。
        available_tables_only 意义同 describe_table_family API。
        """
        if not self.is_table_exist(table_family_name, dbname=dbname):
            return False
        if dbname is None:
            dbname = self.db
        if rollup_name is None:
            return True
        # 查询表的具体结构
        succ, val = self.client.describe_table_family(dbname,
                                                      table_family_name,
                                                      available_tables_only)
        if not succ:
            return self.log_error(val)
        v = [table['table_name'] == rollup_name for table in val['tables']]
        return any(v)

    def create_database(self, dbname=None):
        """
        创建数据库，不指定 dbname 时，使用初始化时指定的数据库名。
        如果数据库已经存在，则直接返回 True。
        """
        if dbname is None:
            dbname = self.db
        if self.is_db_exist(dbname):
            return True, None
        succ, val = self.client.create_database(dbname)
        if not succ:
            self.log_error(val)
        return succ, val

    def drop_database(self, dbname=None):
        """
        删除数据库，不指定 dbname 时，使用初始化时指定的数据库名。
        如果数据库不存在，则直接返回 True。
        """
        if dbname is None:
            dbname = self.db
        if not self.is_db_exist(dbname):
            return True, None
        succ, val = self.client.drop_database(dbname)
        return succ, val

    def batch_load_sync(self, label, tbname,
                        urls=None,
                        negative_urls=None,
                        fields=None,
                        timeout=3600,
                        separator='\t',
                        line_end='CR',
                        max_filter_ratio=0,
                        parallel_tasks=0,
                        partition=None):
        """
        从 Hive 上指定 URL 中导入数据，执行成功（状态为 finished）之后返回。
        label 导入任务实际的 task_label 会以 label 参数为前缀，后面添加数字避免重复。
        parallel_tasks (废弃, 无作用)
        partition 导入之前创建删除任务删除满足 partition 条件的数据。删除任务的
            实际 label 以 label_delete 为前缀，后面添加数字避免重复。例如：
            {"st_date": "20140812"} 则会事先删除表中 st_date='20140812' 的数据。
            partition 为 None 或者 {} 时不删除数据，默认为 None。
        """

        if fields:
            fields = [field.lower() for field in fields]

        if not urls and not negative_urls:
            return False, 'no urls to batch load'

        taskinfo = _PaloClient.TaskInfo(timeout,
                                        max_filter_ratio=max_filter_ratio)
        tablesource = _PaloClient.TableSource(tbname)
        if urls:
            tablesource.add_source(urls,
                                   column_names=fields,
                                   column_separator=separator,
                                   line_end=line_end,
                                   is_negative=False)
        if negative_urls:
            tablesource.add_source(negative_urls,
                                   column_names=fields,
                                   column_separator=separator,
                                   line_end=line_end,
                                   is_negative=True)

        if partition:
            NLogger.debug('call delete_sync to delete data')
            succ, val = self.delete_sync(
                "%s_delete" % (label),
                tbname,
                partition=partition,
                timeout_second=timeout,
                skipIfNonExist=True
            )
            if not succ:
                self.log_error(val)

        label = self.find_unused_label(label)
        NLogger.info('batch_load task label is : %s', label)
        self.client.cancel_batch_load(self.db, label)

        succ, val = self.client.batch_load(self.db, label,
                                           taskinfo, [tablesource()])
        if not succ:
            self.log_error(val)
            return False, val

        lastmsg = ''
        while True:
            succ, val = self.client.show_batch_load(self.db, label, limit=1)
            if not succ or len(val) < 1:
                self.log_error(val)
                return False, val
            val = val[0]
            msg = 'label=%s state=%s (%d%%)' % (label,
                                                val['task_state'],
                                                val['progress'])
            if msg != lastmsg:
                NLogger.info(msg)
                lastmsg = msg
            if val['task_state'] == 'finished':
                NLogger.info(repr(val['etl_job_info']))
                return True, val
            elif val['task_state'] == 'cancelled':
                self.log_error(val)
                return False, val
            time.sleep(30)

    def find_unused_label(self, prefix):
        """
            returns prefix _ current_time _ random value _ pid
        """
        while True:
            now = datetime.datetime.now()
            name = '%s_%s_%s_%s' % (prefix,
                                    now.strftime('%Y%m%d%H%M%S'),
                                    random.randrange(1, 1000),
                                    os.getpid())
            succ, val = self.client.show_delete(self.db)
            if not succ:
                self.log_error(val)
            delete_list = [info['data_label'] for info in val]
            succ, val = self.client.show_batch_load(self.db)
            if not succ:
                self.log_error(val)
            load_list = [info['data_label'] for info in val]
            if name not in delete_list and name not in load_list:
                return name

    def get_table_schema(self, table_family_name):
        """
        获取表的字段。
        """
        succ, val = self.client.describe_table_family(self.db,
                                                      table_family_name)
        if not succ:
            self.log_error(val)
        return val['schema']

    def get_table_structure(self, table_family_name, rollup_name=None):
        """
        获取基础表/上卷表的结构。
        如果指定 rollup_name，则返回上卷表结构，否则返回基础表结构。
        如果指定表不存在，返回 None，否则返回一个 dict。
        """
        if rollup_name is None:
            rollup_name = table_family_name
        if not self.is_rollup_exist(table_family_name,
                                    dbname=self.db,
                                    rollup_name=rollup_name,
                                    available_tables_only=False):
            return None
        succ, val = self.client.describe_table_family(
            self.db,
            table_family_name,
            available_tables_only=False)
        if not succ:
            self.log_error(val)
        for table in val['tables']:
            if table['table_name'] == rollup_name:
                return table
        return None

    def create_table_family(self, table_family_name,
                            table_family_info,
                            overwrite=False):
        """
        使用指定的 schema 和表配置，创建指定 table family的
        基础表及多个上卷表。
        overwrite 为 True 时会删除现有的 table family。
        """

        schema = table_family_info.schema
        table_family_info = table_family_info()

        if self.is_table_exist(table_family_name) and overwrite:
            # 删除原来的表
            succ, val = self.client.drop_table_family(self.db,
                                                      table_family_name)
            if not succ:
                self.log_error(val)

        # 如果表已经存在，则直接返回
        if not self.is_table_exist(table_family_name):
            NLogger.info('create table family')
            succ, val = self.client.create_table_family(
                self.db,
                table_family_name,
                'olap',
                schema,
                basic_table=table_family_info[table_family_name])
            if not succ:
                self.log_error(val)
        else:
            NLogger.info('table family already exist, skip creation')

        waiting_msg = u'can not create roll up table, ' \
                      'as there are unfinished tasks.'

        is_data = self.get_row_count(table_family_name) > 0

        for table_name in table_family_info:
            if table_name != table_family_name:
                table = table_family_info[table_name]
                if self.is_rollup_exist(table_family_name, table_name):
                    NLogger.info('rollup table already exist, skip creation')
                    continue

                if is_data:
                    NLogger.info('data already exists, modify rollup table options')
                    _PaloClient.RollUpTable.downgrade(table)

                NLogger.info('create rollup table : ' + repr(table))
                while True:
                    succ, val = self.client.create_table(
                        self.db,
                        table_family_name,
                        table)
                    if not succ:
                        if val == waiting_msg:
                            NLogger.info('Waiting for other tasks in same db')
                            time.sleep(300 + random.randint(0, 50))
                            continue
                        self.log_error(val)
                    break

                NLogger.info('Waiting for rollup table creation ...')
                NLogger.info('This may takes a long time on huge tables')
                NLogger.info('To cancel creation, use drop_table command in palo-client.')

                # waiting for create_table
                while True:
                    succ, val = self.client.show_schema_change(
                        self.db,
                        table_family_name
                    )
                    if not succ:
                        self.log_error(val)

                    if val['table_family_state'] == 'normal':
                        NLogger.info('created rollup table %s' % (table_name))
                        break
                    time.sleep(60)
        return True

    def create_basic_table_family_hash(self, table_family_name, schema,
                                       partition_columns=None,
                                       hash_mod=61,
                                       hash_method='full_key',
                                       data_file_type='row'):
        """
        创建表格，分 partition 方法为 hash。
        """
        table = _PaloClient.BasicTable(table_family_name,
                                       partition_method='hash',
                                       partition_columns=partition_columns,
                                       hash_mod=hash_mod,
                                       hash_method=hash_method,
                                       data_file_type=data_file_type)
        succ, val = self.client.create_table_family(self.db, table_family_name,
                                                    table_family_type='olap',
                                                    schema=schema,
                                                    basic_table=table)
        if not succ:
            self.log_error(val)
        return succ, val

    def create_basic_table_family_key_range(self, table_family_name, schema,
                                            partition_columns=None,
                                            key_ranges=None,
                                            data_file_type='row'):
        """
        创建表格，分 partition 方法为 key_range。
        """
        table = _PaloClient.BasicTable(table_family_name,
                                       partition_method='key_range',
                                       partition_columns=partition_columns,
                                       key_ranges=key_ranges,
                                       data_file_type=data_file_type)
        succ, val = self.client.create_table_family(self.db, table_family_name,
                                                    table_family_type='olap',
                                                    schema=schema,
                                                    basic_table=table)
        if not succ:
            self.log_error(val)
        return succ, val

    def create_mysql_table_family(self, table_family_name, schema,
                                  mysql_db, mysql_table):
        """
        创建外部表，需要提供数据库的结构、term 和表名。
        mysql_db DBConf 中的 term
        mysql_table 表名
        """
        table = _PaloClient.MysqlTable(host=DBConf[mysql_db]['host'],
                                       port=DBConf[mysql_db]['port'],
                                       user_name=DBConf[mysql_db]['user'],
                                       password=DBConf[mysql_db]['passwd'],
                                       database_name=DBConf[mysql_db]['db'],
                                       mysql_table=mysql_table)
        succ, val = self.client.create_table_family(self.db, table_family_name,
                                                    table_family_type='mysql',
                                                    schema=schema,
                                                    mysql_table=table)
        if not succ:
            self.log_error(val)
        return succ, val

    def get_row_count(self, table_family_name, table_name=None,
                      index_name=None):
        """
        统计表格大致的总行数。
        """
        if table_name is None:
            table_name = table_family_name
        if index_name is None:
            index_name = 'PRIMARY'
        succ, val = self.client.get_table_row_count(self.db, table_family_name,
                                                    table_name, index_name)
        if not succ:
            self.log_error(val)
        return val

    def drop_table_family(self, table_family_name):
        """
        删除 table_family。
        """
        succ, val = self.client.drop_table_family(self.db, table_family_name)
        if not succ:
            self.log_error(val)
        return succ, val

    def delete_sync(self, data_label, tbname, partition,
                    timeout_second=0,
                    skipIfNonExist=False):
        """
        删除数据，执行完成（状态为 finished）之后返回。
        skipIfNonExist: 值为 True 时，如果数据已存在时才创建删除任务，
            否则不删除数据直接返回，默认为 False。
        """
        delete_label = self.find_unused_label(data_label)
        NLogger.info('delete task label is : %s', delete_label)

        # get schema
        succ, val = self.client.describe_table_family(self.db, tbname, True)
        if not succ:
            self.log_error(val)
        schema = {}  # 列名到 (数据类型，是否指标列）的映射
        for column in val['schema']:
            schema[column['column_name']] = (
                column['data_type'].upper(),
                'aggregation_method' in column
            )

        condition = self.client.ConditionTableFamily(tbname)
        if not partition:
            self.log_error('partition must have at least 1 condition')

        encoding_types = ('STRING', 'VARCHAR')

        for key in partition:
            if key not in schema:
                self.log_error('specified column %s does not exist.'
                               % (key))
            if schema[key][1]:
                self.log_error('%s must be one of the key columns.'
                               % (key))

            if (isinstance(partition[key], list) or
                    isinstance(partition[key], tuple)) and \
                    len(partition[key]) == 2 and \
                    partition[key][0] != partition[key][1]:
                condition.add_condition(key, '>=', partition[key][0],
                                        schema[key][0] in encoding_types)
                condition.add_condition(key, '<=', partition[key][1],
                                        schema[key][1] in encoding_types)

            else:
                condition.add_condition(key, '=', partition[key],
                                        schema[key][0] in encoding_types)

        # check if data exists
        if skipIfNonExist:
            sql = self.create_check_query(condition, schema)
            db = NSqlDB(self.term)
            try:
                NLogger.info(sql)
                db.execute(sql)
                rows = db.fetchone()
                if rows:
                    NLogger.info('returned %d rows' % len(rows))
                else:
                    NLogger.info('no records found')
                    NLogger.info('skip delete task since no data exist')
                    return True, 'data not exist, skipped'
            finally:
                db.close()

        # throw exception if label already exist
        succ, val = self.client.show_delete(
            self.db,
            data_label=delete_label
        )
        if not succ:
            self.log_error(val)
        for info in val:
            if info['task_info'] != 'cancelled':
                return False, info

        self.client.cancel_delete(self.db, delete_label)

        delete_msg = u'Some delete tasks are not finished, '\
                     'Please wait or cancel them.'
        load_msg = u'Some import tasks are not finished, '\
                   'Please wait or cancel them.'
        normal_msg = u'Table family state not normal.'

        while True:
            # 正在运行的 batch_load 或者 delete 会导致调用失败，需要重试
            succ, val = self.client.delete(self.db,
                                           delete_label,
                                           condition,
                                           timeout_second=timeout_second)
            if succ:
                break
            else:
                if val == delete_msg:  # delete
                    NLogger.info('Waiting for previous delete to finish')
                    time.sleep(300 + random.randint(0, 50))
                    continue
                if val == load_msg:
                    # batch_load
                    NLogger.info('Waiting for existing batch_load to finish')
                    time.sleep(300 + random.randint(0, 100))
                    continue
                if val == normal_msg:
                    # create_table / schema_change
                    NLogger.info('Waiting for existing create_table '
                                 'or change_schema to finish')
                    time.sleep(900 + random.randint(0, 100))
                    continue
                self.log_error(val)

        lastmsg = ''
        while True:
            succ, val = self.client.show_delete(self.db, delete_label,
                                                limit=None)
            val = [info for info in val if info['data_label'] == delete_label]
            # extract lastone
            val = val[-1]
            if not succ:
                self.log_error(val)

            msg = 'label=%s state=%s (%d%%)' % (delete_label,
                                                val['task_state'],
                                                val['progress'])
            if lastmsg != msg:
                NLogger.info(msg)
            lastmsg = msg
            if val['task_state'] == 'finished':
                return True, val
            elif val['task_state'] == 'cancelled':
                return False, val
            time.sleep(30)

    def create_check_query(self, condition, schema):
        """
        创建符合删除条件数据是否存在的查询语句。
        condition: 删除条件 ConditionTable
        schema: 表格字段 {column:(data_type, is_aggregation_method), ...}
        """
        fields = ['`%s`' % (cond['column_name'])
                  for cond in condition.conditions]

        def process_row(key):
            """
            调用 get_condition_sql 处理单个字段
            """
            return condition.get_condition_sql(
                key,
                schema[key][0]
            )

        conditions = [process_row(key) for key in schema if not schema[key][1]]
        conditions = [cond for cond in conditions if cond != '']

        condition = 'SELECT %s FROM %s WHERE %s LIMIT 1' % (
            ', '.join(fields),
            condition.table_family_name,
            ' AND '.join(conditions))
        return condition

    def create_rollup_table_sync(self, table_family_name):
        pass

    def drop_rollup_table(self, table_family_name, rollup_name):
        """
        删除指定上卷表，如果表格不存在则直接返回。
        返回是否执行了删除操作。
        """
        if not self.is_rollup_exist(table_family_name, rollup_name):
            return False
        # find index name match rollup_name
        succ, val = self.client.describe_table_family(
            self.db,
            table_family_name)
        if not succ:
            self.log_error(val)
        index_name = None
        for table in val['tables']:
            if table['table_name'] == rollup_name:
                index_name = table['index_name']
                break
        if index_name is None:
            self.log_error('Cannot find index name for rollup table %s'
                           % (rollup_name))
        # drop rollup table
        succ, val = self.client.drop_table(
            self.db,
            table_family_name,
            rollup_name,
            index_name)
        if not succ:
            self.log_error(val)
        return True

    def schema_change_sync(self, table_family_name, schema=None, tables=None):
        pass

    def mysql_table_schema_change(self, table_family_name,
                                  schema=None,
                                  mysql_db=None,
                                  mysql_table=None):
        """
        修改外部表的参数。
        """
        if mysql_db is not None and mysql_table is not None:
            table = _PaloClient.MysqlTable(
                host=DBConf[mysql_db]['host'],
                port=DBConf[mysql_db]['port'],
                user_name=DBConf[mysql_db]['user'],
                password=DBConf[mysql_db]['passwd'],
                database_name=DBConf[mysql_db]['db'],
                mysql_table=mysql_table)
        else:
            table = None

        if schema is None and table is None:
            # nothing to change
            return True, None

        # wait for other schema_change / create_table tasks
        while True:
            succ, val = self.client.show_schema_change(
                self.db,
                table_family_name)
            if not succ:
                self.log_error(val)

            NLogger.info('table=%s waiting for other operations'
                         % (table_family_name))

            if val['table_family_state'] == 'normal':
                break
            time.sleep(30)

        # drop old schema if exists
        self.client.drop_old_schema(self.db, table_family_name)
        succ, val = self.client.schema_change(self.db, table_family_name,
                                              schema=schema,
                                              mysql_table=table)
        if not succ:
            self.log_error(val)

        while True:
            succ, val = self.client.show_schema_change(
                self.db,
                table_family_name)
            if not succ:
                self.log_error(val)

            NLogger.info('table=%s state=%s (%d%%)'
                         % (table_family_name,
                            val['table_family_state'],
                            val['tables']['progress']))
            if val['table_family_state'] == 'normal':
                break

            time.sleep(30)

        # drop old schema if exists
        succ, val = self.client.drop_old_schema(self.db, table_family_name)

    def count_running_jobs(self, dblist=None):
        """
        统计某个 db 中正在运行的 batch_load 任务数量。
        dblist : 默认为当前 db
        """
        if not dblist:
            dblist = [self.db]

        dbmap = {}
        count = 0
        for db in dblist:
            succ, val = self.client.show_batch_load(db)
            if not succ:
                self.log_debug(val)
                # ignore error, database maybe dropped during query
                continue
            tasks = [task for task in val
                     if task['task_state'] in ('pending', 'etl', 'loading')]
            count = count + len(tasks)
            dbmap[db] = len(tasks)
        return count, dbmap

    TYPE_MAPS = {
        'hive': {
            'tinyint': ['tiny', "DEF(0)"],
            'smallint': ['short', "DEF(0)"],
            'int': ['int', "DEF(0)"],
            'bigint': ['long', "DEF(0)"],
            'boolean': ['tiny', "DEF(0)"],
            'float': ['float', "AGG(ADD),DEF(0)"],
            'double': ['double', "AGG(ADD),DEF(0)"],
            'decimal': ['decimal', "AGG(ADD),PRE(10),SCA(6),DEF(0)"],
            'string': ['varchar', "DEF(),LEN(50)"],
            'varchar': ['varchar', "DEF(),LEN(50)"]
        },
        'mysql': {
            'tinyint': ['tiny', "DEF(0)"],
            'unsigned tinyint': ['short', "DEF(0)"],
            'smallint': ['short', "DEF(0)"],
            'unsigned smallint': ['int', "DEF(0)"],
            'int': ['int', "DEF(0)"],
            'unsigned int': ['long', "DEF(0)"],
            'bigint': ['long', "DEF(0)"],
            'unsigned bigint': ['unsigned_long', "DEF(0)"],
            'mediumint': ['int', "DEF(0)"],
            'unsigned mediumint': ['int', "DEF(0)"],
            'float': ['float', "AGG(ADD),DEF(0)"],
            'double': ['double', "AGG(ADD),DEF(0)"],
            'decimal': ['decimal', "DEF(0)"],
            'bit': ['tiny', "DEF(0)"],
            'char': ['string', "DEF()"],
            'binary': ['string', "DEF()"],
            'varchar': ['varchar', "DEF()"],
            'varbinary': ['varchar', "DEF()"],
            'tinytext': ['varchar', "DEF(),LEN(50)"],
            'mediumtext': ['varchar', "DEF(),LEN(50)"],
            'text': ['varchar', "DEF(),LEN(50)"],
            'longtext': ['varchar', "DEF(),LEN(50)"],
            'enum': ['varchar', "DEF(),LEN(50)"],
            'blob': ['varchar', "DEF(),LEN(50)"],
            'date': ['date', "DEF(1900-01-01)"],
            'datetime': ['datetime', "DEF(1900-01-01 00:00:00)"],
            'timestamp': ['datetime', "DEF(1900-01-01 00:00:00)"],
            'time': ['varchar', "DEF(),LEN(10)"],
        }
    }

    def convert_mysql_schema(t):
        # 解析 mysql 数据类型
        import re
        p = re.compile("(zerofill)")
        t = p.sub("", t).strip()

        # 拆分类型和参数
        loc = t.find('(')
        if loc != -1:
            orig_type = t[:loc].lower()
            orig_arg = t[loc+1:-1]
        else:
            orig_type = t.lower()
            orig_arg = ''
        if orig_type.find('unsigned') != -1:
            orig_type = 'unsigned %s' % (
                orig_type.replace('unsigned', '').strip())
        column_type = PaloClient.TYPE_MAPS['mysql'][orig_type][0]
        type_arg = PaloClient.TYPE_MAPS['mysql'][orig_type][1]

        # 特殊处理的字段类型
        if orig_arg:
            if column_type in ('char', 'varchar', 'binary', 'varbinary'):
                type_arg = '%s,LEN(%s)' % (type_arg, orig_arg)
            if column_type == 'decimal':
                pre, sca = orig_arg.split(',', 1)
                type_arg = '%s,PRE(%s),SCA(%s)' % (type_arg,
                                                   pre.strip(),
                                                   sca.strip())
        return column_type, type_arg

    TYPE_CONVERTERS = {
        'hive': lambda t: (PaloClient.TYPE_MAPS['hive'][t][0],
                           PaloClient.TYPE_MAPS['hive'][t][1]),
        'mysql': convert_mysql_schema
    }

    @classmethod
    def convert_schema(cls, schema):
        """
        转换 schema 格式。
        输入：
        schema        : [{'字段','字段类型','palo的设置'}]
        输出：
        fields        : 字段列表 ['a', 'b', 'c',...]
        fieldType     : 默认列的类型 {'a':'int',...}
        overrideField : 手动指定的列的样式 {'a':'DEF(0)','c':'AGG(ADD)',...}
        """
        fieldType = {}
        overrideField = {}
        fields = []

        for s in schema:
            field = s[0]
            fields.append(field)
            fieldType[field] = s[1]
            if len(s) > 2:
                overrideField[field] = s[2]
        return fields, fieldType, overrideField

    @classmethod
    def create_schema(cls, fields, fieldType, overrideField,
                      keyFields=None, type_map_type='hive'):
        """
        根据参数构造 schema。
        fields        : 输入列 ['a', 'b', 'c',...]
        fieldType     : 默认列的类型 {'a':'int',...}
        overrideField : 手动指定的列的样式 {'a':'DEF(0)','c':'AGG(ADD)',...}
        keyFields     : 维度列及其顺序 ['c', 'a']
        """
        if fields is None:
            fields = []
        if fieldType is None:
            fieldType = {}
        if overrideField is None:
            overrideField = {}
        type_converter = PaloClient.TYPE_CONVERTERS[type_map_type]

        modified_schema = Schema()
        for column_name in fields:
            orig_type = fieldType[column_name].lower()
            column_type, type_arg = type_converter(orig_type)
            # override default value or types
            if column_name in overrideField:
                override_value = overrideField[column_name]
                override_type = None
                if override_value and \
                        override_value.split(',')[0].find('(') == -1:
                    override_type = \
                        override_value.split(',')[0].lower().strip()
                    override_value = \
                        override_value[override_value.find(',') + 1:]
                column_type = override_type or column_type
                type_arg = override_value
            modified_schema.add_column_raw("%s,%s,%s" % (column_name,
                                                         column_type.upper(),
                                                         type_arg))
        # if manually specified key fields
        if keyFields is not None:
            modified_schema.set_key_fields(keyFields)
        return modified_schema
