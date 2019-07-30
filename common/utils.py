# coding=utf-8
"""Common utils.
"""
import errno
import logging
import os
import re
import sys

import requests

from contextlib import contextmanager
from functools import wraps
from collections import deque


def _format_path(_path):
    return os.path.abspath(_path).replace('/', os.sep)


def _get_caller():
    caller = getattr(sys, '_getframe')(2)
    while 'utils.py' in caller.f_code.co_filename:
        caller = caller.f_back
    return caller


def _get_caller_file():
    return _get_caller().f_code.co_filename


def _get_caller_path():
    return os.path.abspath(os.path.dirname(_get_caller_file()))


def path(file_path):
    """Convert formatted path string to os absolute path.

    Arguments:
        file_path {string} -- a formatted path string

    Raises:
        ValueError -- raise when invalid path format

    Returns:
        string -- os absolute path string
    """

    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../'))

    if os.path.isabs(file_path):
        return file_path
    elif file_path.startswith('@/'):
        return _format_path(base_dir + file_path.lstrip('@'))
    elif file_path.startswith('./'):
        caller_home_dir = _get_caller_path()
        return _format_path(caller_home_dir + file_path.lstrip('.'))
    elif file_path.startswith('../'):
        caller_home_dir = _get_caller_path()
        caller_parent_dir = os.path.abspath(
            os.path.join(caller_home_dir, '../'))
        return _format_path(caller_parent_dir + file_path.lstrip('.'))
    else:
        msg = u'Invalid path["{}"].'.format(file_path)
        logging.error(msg)
        raise ValueError(msg)


def mkdir(dir_path):
    """Make directories recusively with specified path.

    Arguments:
        dir_path {string} -- directory path string

    Raises:
        OSError -- raise when parent path is not directory
    """

    current_path = path(dir_path)
    try:
        os.makedirs(current_path)
    except OSError as err:
        if err.errno == errno.EEXIST and os.path.isdir(current_path):
            pass
        else:
            raise


@contextmanager
def pushd(directory):
    """
    Change current dir to directory between context
    :param directory:
    :return:
    """
    if not os.path.isdir(directory):
        raise ValueError(u'"{}" is not a directory.'.format(directory))

    origin_dir = os.curdir
    os.chdir(directory)

    try:
        yield
    finally:
        os.chdir(origin_dir)


def ls_dir(file_pattern=None, base_dir=None):
    """
    List all files under directory
    :param file_pattern: file pattern for listing
    :param base_dir: directory to list files, if None, use current dir
    :return:
    """
    files = []

    if not base_dir:
        base_dir = '.'

    file_pattern = re.compile(
        file_pattern.replace('*', '.*')) if file_pattern else None

    for file_name in os.listdir(base_dir):
        if not os.path.isfile('{}/{}'.format(base_dir, file_name)):
            continue

        if not file_pattern or file_pattern.match(file_name):
            files.append(file_name)

    return files


def load_module_by_path(module_path, package='', object_name=''):
    """
    Load module by file path.
    :param module_path: module file path
    :param package: package name
    :param object_name: object name in module
    :return:
    """

    if not os.path.isfile(module_path):
        raise IOError('Please specify a file path[{}].'.format(module_path))

    base_dir = os.path.dirname(module_path)
    module_name = '.'.join(os.path.basename(module_path).split('.')[:-1])

    if package:
        base_dir = base_dir.replace(os.sep.join(package.split('.')), '')
        module_name = '.'.join((package, module_name))

    sys.path.append(base_dir)
    _module = __import__(module_name, fromlist=package.split('.'))

    if object_name:
        return getattr(_module, object_name)
    return _module


def singleton(cls):
    """
    Singleton wrapper for class.
    :param cls: class to transform to singleton class
    :return:
    """

    instances = {}

    @wraps(cls)
    def get_instance(*args, **kwargs):
        if cls not in instances:
            instances[cls] = cls(*args, **kwargs)
        return instances[cls]

    return get_instance


# 匹配python及c++下的double极大值字符串
DBL_MAX_PATTERN = re.compile(r'\d{300,}(?:\.\d{6,8})?|\d\.\d{10,}e\+?308')

# 匹配整形或浮点型(包含科学计数法)的数字字符串
NUM_PATTERN = re.compile(r'^-?[0-9]+(?:\.[0-9]{1,18})?$|'
                         r'^-?\d\.\d+e[+-]?(\d{1,2}|[12]\d{2}|30[1-8])$')

BOOL_PATTERN = re.compile(r'^[Tt](?:[Rr][Uu][Ee])?$|'
                          r'^[Ff](?:[Aa][Ll][Ss][Ee])?$|'
                          r'^[Yy](?:[Ee][Ss])?$|'
                          r'^[Nn](?:[Oo])?$')

SET_PATTERN = re.compile(r'^(?:[\w \t(,)\'"]+\|)+[\w \t(,)\'"]+$')

LITERAL_PATTERN = re.compile(r'(?:[{[(].*[}\])][,\s\r\n]?)+')

REGEX_PATTERN = re.compile(r'(?:(?P<prefix>reg(?:ybmex)?://)|/)'
                           r'(?P<pattern>.+)'
                           r'(?(prefix)|/)$',
                           flags=re.IGNORECASE)

MYSQL_CONN_PATTERN = re.compile(r'(?:mysql://)'
                                r'(?P<user>\w+):(?P<password>\w+)@'
                                r'(?P<host>[^:/]+)(?::(?P<port>\d+))?'
                                r'/(?P<db>\w+)'
                                r'(?:\?charset=(?P<charset>.+))?',
                                flags=re.IGNORECASE)

CSV_FILE_PATTERN = re.compile(r"(?:csv://)"
                              r"(?P<base_dir>[^?]+)"
                              r"(?:\?encoding=(?P<encoding>.+))?")

CONN_PATTERN = re.compile(r'(?P<proto>(?:tcp|udp)+)://'
                          r'(?P<host>[^:]+):'
                          r'(?P<port>[0-9]+)',
                          flags=re.IGNORECASE)

IP_PATTERN = re.compile(r'((?:(?:25[0-5]|2[0-4]\\d|'
                        r'[01]?\\d?\\d)\\.){3}(?:25[0-5]|2[0-4]\\d|'
                        r'[01]?\\d?\\d))')

QUOTE_PATTERN = re.compile(r'^(?:(?P<single>\')|")'
                           r'(?P<content>.+)'
                           r'(?(single)\'|")$')


def is_ip_addr(host_string):
    if IP_PATTERN.match(host_string):
        return True

    return False


def is_double_maximum(num_string):
    if DBL_MAX_PATTERN.match(num_string):
        return True

    return False


def is_number(num_string):
    if not num_string:
        return False

    if NUM_PATTERN.match(num_string):
        return True

    return False


def try_parse_num(num_string):
    """Try to parse a string to number.

    Arguments:
        num_string {string} -- a string number

    Returns:
        int/long/float -- if the string is number
        string -- string itself if not a number
    """

    if is_double_maximum(num_string):
        return True, sys.float_info.max
    if is_number(num_string):
        if num_string.isdigit():
            # 数字大于2147483647时(sys.maxint)，将转换为long
            return True, int(num_string)
        else:
            return True, float(num_string)
    return False, num_string


def try_parse_time(time_string, format_string=u''):
    """Try to parse a string to datetime tupel.

    if format_string is specified, parse in specified format
    else try any available time format

    Arguments:
        time_string {string} -- a time string

    Keyword Arguments:
        format_string {string} -- a time format string (default: {u''})

    Returns:
        datetime tuple -- parsed datetime tuple
        string -- string itself if not time
    """

    from datetime import datetime
    from dateutil.parser import parse

    try:
        if format_string:
            datetime_tuple = datetime.strptime(time_string, format_string)
        else:
            datetime_tuple = parse(time_string)
    except (ValueError, OverflowError):
        return False, time_string
    else:
        return True, datetime_tuple


def try_parse_boolean(value):
    """Try to parse the string to boolean.

    Arguments:
        bool_string {string} -- a boolean string

    Returns:
        boolean -- parsed boolean
        string -- string itself if not boolean
    """
    if isinstance(value, bool):
        return True, value

    if BOOL_PATTERN.match(value):
        return True, value.lower()[0] in ['y', 't']

    return False, value


def try_parse_set(set_string):
    """Try to parse the string to set.

    Arguments:
        set_string {string} -- a set string seperated by '|'

    Retruns:
        set -- parsed set
        string -- string itself if not set
    """

    if SET_PATTERN.match(set_string):
        return True, set(set_string.split('|'))
    return False, set_string


def try_parse_quote(quote_string):
    if QUOTE_PATTERN.match(quote_string):
        return True, quote_string.lstrip('\'"').rstrip('\'"')
    return False, quote_string


def transform_pascal(input_word):
    pattern = re.compile(r'[A-z][a-z]*')

    words = deque(pattern.findall(input_word))

    results = list()

    special_noun = list()

    while words:
        word = words.popleft()

        if len(word) == 1:
            special_noun.append(word.lower())
        else:
            if special_noun:
                results.append(u''.join(special_noun))
                special_noun = list()

            results.append(word.lower())

    if special_noun:
        results.append(u''.join(special_noun))

    return u'_'.join(results)


def to_pascal(input_word, special_word=None):
    results = list()

    for word in input_word.split('_'):
        if special_word and word in special_word:
            results.append(word.upper())
        else:
            results.append(word.capitalize())

    return u"".join(results)


def get_env_bool(name):
    flag = os.environ.get(name, False)
    if isinstance(flag, bool):
        return flag
    parsed, value = try_parse_boolean(flag)
    if parsed:
        return value
    parsed, value = try_parse_num(flag)
    return bool(value)


def get_env_string(name, default=''):
    return os.environ.get(name, default)
