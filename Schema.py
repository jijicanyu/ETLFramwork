# coding=utf-8
"""
Schema.py
"""
import re
import ordereddict

from HiveConfig import hiveConfig


def __parse_schema_and_options_hive(schema_str):
    """
    :param schema_str:
    :return:
    """
    detail_header = 'Detailed Table Information'
    detail_index = schema_str.find(detail_header)
    if detail_index < 0:
        return {}, {}
    output = schema_str[detail_index + len(detail_header):]
    cols_pattern = re.compile(r'cols:\[(.*?)\]')
    cols = cols_pattern.findall(output)
    if len(cols) >= 1:
        cols = cols[0]
        schema_pattern = re.compile(
            r'FieldSchema\(name:(.*?),\s*type:(.*?),\s*comment:.*?\)'
        )
        schema = ordereddict.OrderedDict()
        for (column_name, column_type) in schema_pattern.findall(cols):
            schema[column_name] = column_type
    options = {}
    location_pattern = re.compile(r'location:(hdfs://.*?)[,}]')
    delim_pattern = re.compile(r'field.delim=(.*?)[,}]')
    options['location'] = location_pattern.findall(output)
    delims = delim_pattern.findall(output)
    if not delims:
        options['delim'] = '\x01'
    else:
        options['delim'] = delims[0]
    return schema, options


def __parse_schema_and_options_qe(schema_str):
    """
    :param schema_str:
    :return:
    """
    detail_header = 'Detailed Table Information'
    detail_index = schema_str.find(detail_header)
    if detail_index < 0:
        return {}, {}
    schema = ordereddict.OrderedDict()
    lines = schema_str.strip().split('\n')
    for dict in get_schema_list('\n'.join(lines[:lines.index('')])):
        for k, v in dict.items():
            schema[k] = v
    output = schema_str[detail_index + len(detail_header):]
    options = {}
    location_pattern = re.compile(r"Location:\s*(hdfs://.*?)[\s|$]")
    delim_pattern = re.compile(r'field.delim=(.*?)[,}]')
    options['location'] = location_pattern.findall(output)
    delims = delim_pattern.findall(output)
    if not delims:
        options['delim'] = '\x01'
    else:
        options['delim'] = delims[0]
    return schema, options


def get_schema_list(schema_str):
    """
    :param schema_str:
    :return:
    """
    rows = schema_str.strip().split('\n')
    if len(rows) > 0 and rows[0].startswith('#'):
        rows = rows[1:]
    if len(rows) > 0 and ('rows fetched.' in rows[-1] or 'row fetched.' in rows[-1]):
        rows = rows[:-1]
    schema = []
    regex = re.compile('\s+')
    for row in rows:
        tokens = regex.split(row.strip(), 2)
        if len(tokens) >= 2:
            schema.append({tokens[0]: tokens[1]})
    return schema


def get_schema_comment_list(schema_str):
    """
    :param schema_str:
    :return:
    """
    rows = schema_str.strip().split('\n')
    if len(rows) > 0 and rows[0].startswith('#'):
        rows = rows[1:]
    if len(rows) > 0 and ('rows fetched.' in rows[-1] or 'row fetched.' in rows[-1]):
        rows = rows[:-1]
    schema = []
    regex = re.compile('\s+')
    for row in rows:
        tokens = regex.split(row.strip(), 2)
        if len(tokens) >= 2:
            schema.append({tokens[0]: tokens[2] if len(tokens) >= 3 else ""})
    return schema


def parse_schema_and_options(schema_str):
    """
    :param schema_str:
    :return:
    """
    return __parse_schema_and_options_qe(schema_str) \
        if hiveConfig.get_engine() == 'queryengine' \
        else __parse_schema_and_options_hive(schema_str)
