# coding=utf-8

import json_rpc_client
import json
import base64


class _PaloClient(object):
    """
        封装 Palo API 为方法调用的类
        参数用途见：http://wiki.babel.baidu.com/twiki/bin/view/Com/Inf/Palo-API
    """

    def __init__(self, host, port, user, passwd, debug=None):
        self.proxy = json_rpc_client.ServerProxy((host, port), user, passwd)
        self.decoder = json.JSONDecoder()
        self.debug = debug

    def _debug_request(self, cmd, input):
        if self.debug:
            self.debug('[REQUEST] [%s] %s' % (cmd, json.dumps(input)))

    def _debug_response(self, cmd, json_text):
        if self.debug:
            self.debug('[RESPONSE] [%s] %s' % (cmd, json_text))

    '''
        用户 API
    '''

    def show_databases(self, show_modification_time=False):
        """
        列出所有的数据库。Palo 1.3 中移除了 show_version，添加 show_modification_time。

        返回 dict，key 为各个数据库名称，
            如果 show_modification_time 为 True，
            返回 { database : modification_time, ... }，
            否则返回 { database : None, ... }
        """
        input = {'show_modification_time': show_modification_time}
        self._debug_request('show_databases', input)
        response = self.proxy.show_databases(input)
        self._debug_response('show_databases', response)
        if response['status'] != 'Success':
            return False, response['fail_msg']
        databases = {}
        if show_modification_time:
            for (k, v) in zip(response['databases'],
                              response['modification_times']):
                databases[k] = v
        else:
            for k in response['databases']:
                databases[k] = None
        return True, databases

    def create_database(self, database_name):
        """
        创建数据库。
        """
        input = {'database_name': database_name}
        self._debug_request('create_database', input)
        response = self.proxy.create_database(input)
        self._debug_response('create_database', response)
        if response['status'] == 'Success':
            return True, None
        else:
            return False, response['fail_msg']

    def drop_database(self, database_name):
        """
        删除数据库。
        """
        input = {'database_name': database_name}
        self._debug_request('drop_database', input)
        response = self.proxy.drop_database(input)
        self._debug_response('drop_database', response)
        if response['status'] == 'Success':
            return True, None
        else:
            return False, response['fail_msg']

    class Schema(object):
        """
        表示数据表的字段结构，通过 add_column 或 add_column_raw 方法添加字段，输出时自动将指标列移到维度列后面。
        """
        def __init__(self):
            self.columns = []
            self.key_columns = []

        def add_column(self, column_name, data_type,
                       string_length=None,
                       default_value=None,
                       aggregation_method=None,
                       precision=None,
                       scale=None,
                       range_key_length=None):
            """
               添加一个维度列/指标列，Palo 1.3 中增加 range_key_length 参数。
            """

            column_name = column_name.lower()
            if self.is_column_exist(column_name):
                # 重复列名时忽略
                return

            column = {'column_name': column_name,
                      'data_type': data_type}

            if string_length is not None:
                # string 的长度不能超过 255，用 varchar 类型代替
                if string_length > 255 and data_type == 'STRING':
                    column['data_type'] = 'VARCHAR'
                column['string_length'] = string_length
            if default_value is not None:
                column['default_value'] = default_value
            if aggregation_method is not None:
                column['aggregation_method'] = aggregation_method.upper()
            if precision is not None:
                column['precision'] = precision
            if scale is not None:
                column['scale'] = scale
            if range_key_length is not None:
                if range_key_length > string_length:
                    # 前缀长度不应超过字段本身的长度
                    range_key_length = string_length
                column['range_key_length'] = range_key_length

            if 'aggregation_method' in column:
                self.columns.append(column)
            else:
                self.key_columns.append(column)
            return self

        def add_column_raw(self, column_desc):
            """
            根据 column_desc 的内容设置列的参数，与 palo-client 建表时的 schema 基本相同，
            注意：列名会转化成全部小写，部分可选参数被转换成大写，例如：
                "DATE,STRING,len(50)" -> "date,STRING,LEN(50)"
                "count,INT,AGG(add)"  -> "count,INT,AGG(ADD)"
            """
            options = column_desc.strip().split(',')
            column_name = options[0].strip().lower()
            data_type = options[1].strip()
            string_length = None
            default_value = None
            aggregation_method = None
            precision = None
            scale = None
            range_key_length = None

            if self.is_column_exist(column_name):
                # 重复列名时忽略
                return

            for option in options[2:]:
                option_up = option.upper().strip()
                if option_up.startswith('LEN(') and option.endswith(')'):
                    string_length = int(option[4:-1])
                elif option_up.startswith('AGG(') and option.endswith(')'):
                    aggregation_method = option_up[4:-1]
                elif option_up.startswith('DEF(') and option.endswith(')'):
                    default_value = option[4:-1]
                elif option_up.startswith('PRE(') and option.endswith(')'):
                    precision = int(option[4:-1])
                elif option_up.startswith('SCA(') and option.endswith(')'):
                    scale = int(option[4:-1])
                elif option_up.startswith('KLEN(') and option.endswith(')'):
                    range_key_length = int(option[5:-1])

            return self.add_column(column_name, data_type,
                                   string_length=string_length,
                                   default_value=default_value,
                                   aggregation_method=aggregation_method,
                                   precision=precision,
                                   scale=scale,
                                   range_key_length=range_key_length)

        def set_key_fields(self, key_fields):
            """
            按照 key_fields 中的字段顺序重新设置与排列维度/指标列。
            表格的维度列与 key_fields 中的顺序一致（因为类型原因不能作为维度列的字段除外）
            指标列则为剩下的列，顺序不定。
            """
            if key_fields is None:
                return

            key_fields = [field.lower() for field in key_fields]
            # create map : column name -> column options
            key_columns_dict = {}
            columns_dict = {}
            for column in self.key_columns:
                key_columns_dict[column['column_name'].lower()] = column
            for column in self.columns:
                columns_dict[column['column_name'].lower()] = column

            # maintain order of key columns as in key_fields
            new_key_columns = []

            # non_key -> non_key, orders do not matter
            new_columns = [column for column in self.columns
                           if column['column_name'].lower() not in key_fields]

            key_column_types = ('varchar', 'string', 'tiny', 'short', 'int',
                                'long', 'decimal', 'date', 'datetime',
                                'unsigned_long')

            for field in key_fields:
                if field not in key_columns_dict:
                    if field not in columns_dict:
                        # unknown column name
                        continue

                    column = columns_dict[field]
                    # set non-key column -> key column

                    if column['data_type'].lower() in key_column_types\
                            and 'aggregation_method' in column:
                        column.pop('aggregation_method')
                    else:
                        # data_type is float/double, can only be non-key column
                        new_columns.append(column)
                        continue
                    # non_key -> key
                    new_key_columns.append(column)
                else:
                    # key -> key
                    new_key_columns.append(key_columns_dict[field])

            new_key_fields = [column['column_name']
                              for column in new_key_columns]

            default_replace_types = ('varchar', 'string', 'date', 'datetime')
            default_add_types = ('tiny', 'short', 'int', 'long', 'decimal',
                                 'double', 'float', 'unsigned_long')
            for column in self.key_columns:
                if column['column_name'].lower() not in new_key_fields:
                    # key -> non_key, default aggregation method
                    if column['data_type'].lower() in default_replace_types:
                        column['aggregation_method'] = 'REPLACE'
                        new_columns.append(column)
                    elif column['data_type'].lower() in default_add_types:
                        column['aggregation_method'] = 'ADD'
                        new_columns.append(column)

            self.columns, self.key_columns = new_columns, new_key_columns

        def is_column_exist(self, column_name):
            """
            返回列是否已存在
            """
            for column in self.key_columns + self.columns:
                if column['column_name'] == column_name:
                    return True
            return False

        def __call__(self):
            ret = list(self.key_columns)
            ret.extend(self.columns)
            return ret

    class BasicTable(object):
        """
        基础表的参数，Palo 1.3 中增加了参数 data_file_type。
        """
        def __init__(self, table_name,
                     short_key=0,
                     partition_method='key_range',
                     partition_columns=None,
                     key_ranges=None,
                     hash_mod=61,
                     hash_method='full_key',
                     data_file_type='row'):

            if not partition_columns:
                partition_columns = []
            if not key_ranges:
                key_ranges = []

            self.desc = {'table_name': table_name,
                         'short_key': short_key,
                         'partition_method': partition_method,
                         'partition_columns': partition_columns,
                         'data_file_type': data_file_type}

            if partition_method == 'key_range':
                if isinstance(key_ranges, list):
                    self.desc['key_ranges'] = key_ranges
                else:
                    self.desc['key_ranges'] = key_ranges()
            elif partition_method == 'hash':
                self.desc['hash_mod'] = hash_mod
                self.desc['hash_method'] = hash_method
                if hash_method == 'full_key':
                    self.desc.pop('partition_columns')

        def __call__(self):
            return dict(self.desc)

    class RollUpTable(BasicTable):
        """
        上卷表的参数
        """
        def __init__(self, table_name, index_name, column_ref,
                     short_key=0,
                     partition_method='key_range',
                     partition_columns=None,
                     key_ranges=None,
                     hash_mod=61,
                     hash_method='full_key',
                     base_table_name=None,
                     base_index_name='PRIMARY',
                     data_file_type='row'):

            _PaloClient.BasicTable.__init__(
                self, table_name,
                short_key=short_key,
                partition_method=partition_method,
                partition_columns=partition_columns,
                key_ranges=key_ranges,
                hash_mod=hash_mod,
                hash_method=hash_method,
                data_file_type=data_file_type)

            self.desc['index_name'] = index_name
            self.desc['column_ref'] = column_ref
            if base_table_name is not None and base_index_name is not None:
                self.desc['base_index_name'] = base_index_name
                self.desc['base_table_name'] = base_table_name

        @classmethod
        def downgrade(cls, table):
            """
            当已有数据时部分字段无法配置。
            """
            for column in ['partition_method',
                    'partition_columns', 'key_ranges',
                    'hash_mod', 'hash_method']:
                if column in table:
                    table.pop(column)


    class TableSchemaChange(RollUpTable):
        """
        修改表结构时的参数
        """
        def __init__(self, table_name, index_name, column_ref):
            _PaloClient.RollUpTable.__init__(self, table_name,
                                             index_name, column_ref)

        def __call__(self):
            d = _PaloClient.RollUpTable.__call__(self)
            return {'index_name': d['index_name'],
                    'column_ref': d['column_ref'],
                    'table_name': d['table_name']}

    class MysqlTable(object):
        """
        外部（Mysql 数据库）表的参数
        """

        def __init__(self, host, port,
                     user_name, password,
                     database_name, table_name):

            self.desc = {'host': host,
                         'port': port,
                         'user_name': user_name,
                         'password': password,
                         'database_name': database_name,
                         'table_name': table_name}

        def __call__(self):
            return dict(self.desc)

    def create_table_family(self, database_name, table_family_name,
                            table_family_type,
                            schema,
                            basic_table=None,
                            mysql_table=None):
        """
        创建 table family。
        """

        input = {'database_name': database_name,
                 'table_family_name': table_family_name,
                 'table_family_type': table_family_type}
        if isinstance(schema, list):
            input['schema'] = list(schema)
        else:
            input['schema'] = schema()
        if table_family_type == 'mysql':
            if isinstance(mysql_table, dict):
                input['mysql_table'] = dict(mysql_table)
            else:
                input['mysql_table'] = mysql_table()
        elif table_family_type == 'olap':
            if isinstance(basic_table, dict):
                input['basic_table'] = dict(basic_table)
            else:
                input['basic_table'] = basic_table()
        self._debug_request('create_table_family', input)
        response = self.proxy.create_table_family(input)
        self._debug_response('create_table_family', response)
        if response['status'] == 'Success':
            return True, None
        else:
            return False, response['fail_msg']

    def drop_table_family(self, database_name, table_family_name):
        """
        删除 table family。
        """
        input = {'database_name': database_name,
                 'table_family_name': table_family_name}
        self._debug_request('drop_table_family', input)
        response = self.proxy.drop_table_family(input)
        self._debug_response('drop_table_family', response)
        if response['status'] == 'Success':
            return True, None
        else:
            return False, response['fail_msg']

    def show_table_families(self, database_name,
                            show_version=False,
                            show_modification_time=False):
        """
        列出数据库中的所有 table family。Palo 1.3 中添加了两个参数 show_version,
        show_modification_time，返回类型由 list [table_family_name, ...] 变为
        {
            table_family_name : {
                'version': version,
                'modification_time': time
            }, ...
        }
        """
        input = {'database_name': database_name,
                 'show_version': show_version,
                 'show_modification_time': show_modification_time}
        self._debug_request('show_table_families', input)
        response = self.proxy.show_table_families(input)
        self._debug_response('show_table_families', response)
        if response['status'] == 'Success':
            table_families = {}
            for k in response['table_families']:
                table_families[k] = {}
            if show_version:
                for (k, v) in zip(response['table_families'],
                                  response['versions']):
                    table_families[k]['version'] = v
            if show_modification_time:
                for (k, v) in zip(response['table_families'],
                                  response['modification_times']):
                    table_families[k]['modification_time'] = v
            return True, table_families
        else:
            return False, response['fail_msg']

    def describe_table_family(self, database_name, table_family_name,
                              available_tables_only=False):
        """
        返回 table_family 结构信息，包含 schema 和建表参数。Palo 1.3 的返回结果中
        多出两个 key：last_modification_time, last_distribute_modification_time。
        """
        input = {'database_name': database_name,
                 'table_family_name': table_family_name,
                 'available_tables_only': available_tables_only}
        self._debug_request('describe_table_family', input)
        response = self.proxy.describe_table_family(input)
        self._debug_response('describe_table_family', response)
        if response['status'] == 'Success':
            response.pop('status')
            return True, response
        else:
            return False, response['fail_msg']

    def create_table(self, database_name, table_family_name, rollup_table):
        """
        向 table_family 增加 table，一般用于创建上卷表
        """
        if isinstance(rollup_table, dict):
            input = dict(rollup_table)
        else:
            input = rollup_table()
        input['database_name'] = database_name
        input['table_family_name'] = table_family_name
        self._debug_request('create_table', input)
        response = self.proxy.create_table(input)
        self._debug_response('create_table', response)
        if response['status'] == 'Success':
            return True, None
        else:
            return False, response['fail_msg']

    def drop_table(self, database_name, table_family_name,
                   table_name, index_name):
        """
        删除上卷表。
        """
        input = {'database_name': database_name,
                 'table_family_name': table_family_name,
                 'table_name': table_name,
                 'index_name': index_name}
        self._debug_request('drop_table', input)
        response = self.proxy.drop_table(input)
        self._debug_response('drop_table', response)
        if response['status'] == 'Success':
            return True, None
        else:
            return False, response['fail_msg']

    def show_tablets(self, database_name, table_family_name,
                     table_name, index_name, available_replicas_only=False):
        """
        显示子表信息。
        """
        input = {'database_name': database_name,
                 'table_family_name': table_family_name,
                 'table_name': table_name,
                 'index_name': index_name,
                 'available_replicas_only': available_replicas_only}
        self._debug_request('show_tablets', input)
        response = self.proxy.show_tablets(input)
        self._debug_response('show_tablets', response)
        if response['status'] == 'Success':
            return True, response['tablets']
        else:
            return False, response['fail_msg']

    class TaskInfo(object):
        """
        导入数据的任务信息。
        """
        def __init__(self, timeout_second=0,
                     max_filter_ratio=0.0,
                     cluster=None):
            self.desc = {'timeout_second': timeout_second,
                         'max_filter_ratio': float(max_filter_ratio)}
            if cluster:
                self.desc['cluster'] = cluster

        def __call__(self):
            return dict(self.desc)

    class TableSource(object):
        """
        导入数据的来源信息。
        """
        def __init__(self, table_family_name):
            self.table_family_name = table_family_name
            self.sources = []

        def add_source(self, file_urls,
                       column_names=None,
                       line_end='CR',
                       column_separator='\t',
                       is_negative=False):
            """
            添加来源地址。
            """
            source = {'file_urls': file_urls,
                      'line_end': line_end,
                      'column_separator': column_separator,
                      'is_negative': is_negative}
            if column_names:
                source['column_names'] = column_names
            self.sources.append(source)
            return self

        def __call__(self):
            return {'table_family_name': self.table_family_name,
                    'sources': list(self.sources)}

    def batch_load(self, database_name, data_label, task_info, table_families):
        """
        提交数据导入的任务。
        """
        input = {'database_name': database_name,
                 'data_label': data_label,
                 'table_families': table_families}
        if isinstance(task_info, dict):
            input['task_info'] = task_info
        else:
            input['task_info'] = task_info()
        self._debug_request('batch_load', input)
        response = self.proxy.batch_load(input)
        self._debug_response('batch_load', response)
        if response['status'] == 'Success':
            return True, None
        else:
            return False, response['fail_msg']

    def cancel_batch_load(self, database_name, data_label):
        """
        取消数据导入任务
        """
        input = {'database_name': database_name,
                 'data_label': data_label}
        self._debug_request('cancel_batch_load', input)
        response = self.proxy.cancel_batch_load(input)
        self._debug_response('cancel_batch_load', response)
        if response['status'] == 'Success':
            return True, None
        else:
            return False, response['fail_msg']

    def show_batch_load(self, database_name,
                        data_label=None,
                        state=None,
                        limit=None):
        """
        查询导入任务的信息
        """
        input = {'database_name': database_name}
        if data_label:
            input['data_label'] = data_label
        if state:
            input['state'] = state
            # pending, etl, loading, finished, cancelled
        if limit:
            input['limit'] = limit
        self._debug_request('show_batch_load', input)
        response = self.proxy.show_batch_load(input)

        # parse JSON string in etl_job_info
        for info in response['batch_load_info']:
            if 'etl_job_info' in info  and info['etl_job_info']:
                info['etl_job_info'] = json.loads(info['etl_job_info'])

        self._debug_response('show_batch_load', response)
        if response['status'] == 'Success':
            return True, response['batch_load_info']
        else:
            return False, response['fail_msg']

    class ConditionTableFamily(object):
        """
        删除任务中的条件。
        """

        def __init__(self, table_family_name):
            self.table_family_name = table_family_name
            self.conditions = []

        def add_condition(self, column_name, condition_op, condition_value,
                          encode_string=False):
            """
            添加删除条件。
            encode_type 为 'string' 时使用 base64 编码 condition_value 的值。
            encode_type 为 'date' 时去除多余的引号。
            encode_type 为 None则不作处理。

            """
            condition = {'column_name': column_name,
                         'condition_op': condition_op}
            if encode_string:
                condition['condition_value'] = \
                    base64.encodestring(str(condition_value))
            else:
                condition['condition_value'] = str(condition_value)
            self.conditions.append(condition)

        def get_condition_sql(self, column_name, types):
            """
            获取指定字段的删除条件的 SQL。
            column_name 字段名称，必须是维度列。
            types 为 字段类型，会特殊处理 string/varchar/date 类型。
            """

            text = []
            for cond in self.conditions:
                if cond['column_name'] == column_name:
                    value = cond['condition_value']
                    if types in ['STRING', 'VARCHAR']:
                        value = "'%s'" % (base64.decodestring(value))
                    elif types in ['DATE']:
                        value = "'%s'" % value
                    condstr = '`%s` %s %s' % (
                        column_name,
                        cond['condition_op'],
                        value
                    )
                    text.append(condstr)
            return ' AND '.join(text)

        def __call__(self):
            return {'table_family_name': self.table_family_name,
                    'condition_list': list(self.conditions)}

    def delete(self, database_name, data_label, conditions,
               timeout_second=0):
        """
        提交数据删除任务。
        """
        input = {'database_name': database_name,
                 'data_label': data_label,
                 'task_info': {'timeout_second': timeout_second}}
        if isinstance(conditions, list):
            input['table_families'] = conditions
        if isinstance(conditions, dict):
            input['table_families'] = [conditions]
        else:
            input['table_families'] = [conditions()]
        self._debug_request('delete', input)
        response = self.proxy.delete(input)
        self._debug_response('delete', response)

        if response['status'] == 'Success':
            return True, None
        else:
            return False, response['fail_msg']

    def cancel_delete(self, database_name, data_label, force=False):
        """
        取消数据删除任务。
        """
        input = {'database_name': database_name,
                 'data_label': data_label,
                 'force': force}
        self._debug_request('cancel_delete', input)
        response = self.proxy.cancel_delete(input)
        self._debug_response('cancel_delete', response)

        if response['status'] == 'Success':
            return True, None
        else:
            return False, response['fail_msg']

    def show_delete(self, database_name,
                    data_label=None,
                    limit=None):
        """
        查询删除任务信息。
        """
        input = {'database_name': database_name}
        if data_label:
            input['data_label'] = data_label
        if limit:
            input['limit'] = limit
        self._debug_request('show_delete', input)
        response = self.proxy.show_delete(input)
        self._debug_response('show_delete', response)

        if response['status'] == 'Success':
            return True, response['delete_info_list']
        else:
            return False, response['fail_msg']

    def get_table_row_count(self, database_name, table_family_name,
                            table_name, index_name,
                            begin_key=None, end_key=None):
        """
        统计表格大致行数。
        """

        input = {'database_name': database_name,
                 'table_family_name': table_family_name,
                 'table_name': table_name,
                 'index_name': index_name}
        if begin_key:
            input['begin_key'] = begin_key
        if end_key:
            input['end_key'] = end_key
        self._debug_request('get_table_row_count', input)
        response = self.proxy.get_table_row_count(input)
        self._debug_response('get_table_row_count', response)
        if response['status'] == 'Success':
            return True, response['row_count']
        else:
            return False, response['fail_msg']

    def schema_change(self, database_name, table_family_name,
                      schema=None,
                      tables=None,
                      mysql_table=None):
        """
        修改表格结构。
        """
        input = {'database_name': database_name,
                 'table_family_name': table_family_name}
        if schema is dict:
            input['schema'] = schema
        elif schema is not None:
            input['schema'] = schema()
        if mysql_table is dict:
            input['mysql_table'] = mysql_table
        elif mysql_table is not None:
            input['mysql_table'] = mysql_table()
        if tables:
            input['tables'] = tables
        self._debug_request('schema_change', input)
        response = self.proxy.schema_change(input)
        self._debug_response('schema_change', response)
        if response['status'] == 'Success':
            return True, None
        else:
            return False, response['fail_msg']

    def drop_old_schema(self, database_name, table_family_name):
        """
        修改表格桔构后删除旧的 schema。
        """
        input = {'database_name': database_name,
                 'table_family_name': table_family_name}
        self._debug_request('drop_old_schema', input)
        response = self.proxy.drop_old_schema(input)
        self._debug_response('drop_old_schema', response)
        if response['status'] == 'Success':
            return True, None
        else:
            return False, response['fail_msg']

    def show_schema_change(self, database_name, table_family_name):
        """
        查询表格结构变更/建立上卷表进度。
        """
        input = {'database_name': database_name,
                 'table_family_name': table_family_name}
        self._debug_request('show_schema_change', input)
        response = self.proxy.show_schema_change(input)
        self._debug_response('show_schema_change', response)
        if response['status'] == 'Success':
            response.pop('status')
            return True, response
        else:
            return False, response['fail_msg']

    def create_udf_function(self, database_name, function_name,
                            arg_types, return_type, location,
                            udf_fn):
        """
        引入UDF函数。
        """
        other_info = {'udf_fn': udf_fn}
        create_cmd = 'create_function %s(%s) return_type=%s location=%s udf_fn=%s -t udf -d %s' \
            % (function_name, ','.join(arg_types),
               return_type, location, udf_fn, database_name)
        input = {'database_name': database_name,
                 'function_name': function_name,
                 'arg_types': arg_types,
                 'return_type': return_type,
                 'location': location,
                 'other_info': other_info,
                 'create_cmd': create_cmd}
        self._debug_request('create_function', input)
        response = self.proxy.create_function(input)
        self._debug_response('create_function', response)
        if response['status'] == 'Success':
            return True, None
        else:
            return False, response['fail_msg']

    def create_uda_function(self, database_name, function_name,
                            arg_types, return_type, location,
                            uda_init_fn, uda_update_fn,
                            uda_merge_fn, uda_finalize_fn):
        """
        引入UDA函数。
        """
        other_info = {'uda_init_fn': uda_init_fn,
                      'uda_update_fn': uda_update_fn,
                      'uda_merge_fn': uda_merge_fn,
                      'uda_finalize_fn': uda_finalize_fn}
        create_cmd = 'create_function %s(%s) return_type=%s location=%s uda_init_fn=%s uda_update_fn=%s uda_merge_fn=%s uda_finalize_fn=%s -t udf -d %s' \
            % (function_name, ','.join(arg_types),
               return_type, location, uda_init_fn,
               uda_update_fn, uda_merge_fn, uda_finalize_fn, database_name)
        input = {'database_name': database_name,
                 'function_name': function_name,
                 'arg_types': arg_types,
                 'return_type': return_type,
                 'location': location,
                 'other_info': other_info,
                 'create_cmd': create_cmd}
        self._debug_request('create_function', input)
        response = self.proxy.create_function(input)
        self._debug_response('create_function', response)
        if response['status'] == 'Success':
            return True, None
        else:
            return False, response['fail_msg']

    def drop_function(self, database_name, function_id):
        input = {'database_name': database_name,
                 'function_id': function_id}
        self._debug_request('drop_function', input)
        response = self.proxy.drop_function(input)
        self._debug_response('drop_function', response)
        if response['status'] == 'Success':
            return True, None
        else:
            return False, response['fail_msg']

    def show_functions(self, database_name=None,
                       function_name=None,
                       function_type=None,
                       all_databases=False):
        """
        查询 UDF / UDA 函数信息。
        """
        if all_databases:
            input = {'all_databases': all_databases}
        else:
            input = {'database_name': database_name}
            if function_name is not None:
                input['function_name'] = function_name
            if function_type is not None:
                input['function_type'] = function_type
        self._debug_request('show_function', input)
        response = self.proxy.show_function(input)
        self._debug_response('show_function', response)
        if response['status'] == 'Success':
            return True, response['functions']
        else:
            return False, response['fail_msg']

    def describe_functiion(self, function_id, database_name=None):
        input = {'function_id': function_id}
        if database_name is not None:
            input['database_name'] = database_name
        self._debug_request('describe_function', input)
        response = self.proxy.describe_function(input)
        self._debug_response('describe_function', response)
        if response['status'] == 'Success':
            return True, response['function']
        else:
            return False, response['fail_msg']

    '''
    系统管理 API
    '''
    '''
    def add_engine():
        pass

    def drop_engine():
        pass

    def alter_engine():
        pass
    '''

    def show_engines(self, engine_id=None, host=None, port=None,
                     work_path=None, ub_monitor_port=None,
                     supervisord_port=None, engine_status=None,
                     show_data=False):
        """
        查询 olapengine 信息
        """
        input = {'show_data': show_data}
        if engine_id is not None:
            input['engine_id'] = engine_id
        if host is not None:
            input['host'] = host
        if port is not None:
            input['port'] = port
        if work_path is not None:
            input['work_path'] = work_path
        if ub_monitor_port is not None:
            input['ub_monitor_port'] = ub_monitor_port
        if supervisord_port is not None:
            input['supervisord_port'] = supervisord_port
        if engine_status is not None:
            input['engine_status'] = engine_status
        self._debug_request('show_engines', input)
        response = self.proxy.show_engines(input)
        self._debug_response('show_engines', response)
        if response['status'] == 'Success':
            return True, response['engines']
        else:
            return False, response['fail_msg']

    '''
    def deploy():
        pass

    def start_engine():
        pass

    def stop_engine():
        pass

    def show_engine_status():
        pass

    def execute_engine_command():
        pass

    def show_engine_disks():
        pass

    def alter_engine_root_path():
        pass

    def show_users():
        pass

    def add_user():
        pass

    def del_user():
        pass

    def password():
        pass

    def grant():
        pass

    def show_privilege():
        pass

    def set_split():
        pass

    def cancel_split():
        pass

    def show_split():
        pass

    def alter_partition_columns():
        pass
    '''
