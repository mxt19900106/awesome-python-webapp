#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = 'hlsky'

'''
description: database operation module
'''

import threading
import mysql.connector
import logging
import time
import functools
import traceback

# db.py

# logging config
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s  [%(levelname)s] [%(filename)s] [line:%(lineno)d]  %(message)s',
                    datefmt='%d.%b.%Y %H:%M:%S',
                    # filename='myapp.log',
                    filemode='w')


class Dict(dict):
    """
    Simple dict but support access as x.y style.
    >>> d1 = Dict()
    >>> d1['x'] = 100
    >>> d1.x
    100
    >>> d1.y = 200
    >>> d1['y']
    200
    >>> d2 = Dict(a=1, b=2, c='3')
    >>> d2.c
    '3'
    >>> d2['empty']
    Traceback (most recent call last):
        ...
    KeyError: 'empty'
    >>> d2.empty
    Traceback (most recent call last):
        ...
    AttributeError: 'Dict' object has no attribute 'empty'
    >>> d3 = Dict(('a', 'b', 'c'), (1, 2, 3))
    >>> d3.a
    1
    >>> d3.b
    2
    >>> d3.c
    3
    """
    def __init__(self, names=(), values=(), **kw):
        super(Dict, self).__init__(**kw)
        for k, v in zip(names, values):
            self[k] = v

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Dict' object has no attribute '%s'" % key)

    def __setattr__(self, key, value):
        self[key] = value


# profile func
def _profiling(start, sql=''):
    """
    print sql execute time
    :param start: start time
    :param sql: sql which is executed
    :return: none
    """
    t = time.time() - start
    if t > 0.1:
        logging.warning("[PROFILING] [DB] %s: %s" % (t, sql))
    else:
        logging.info("[PROFILING] [DB] %s: %s" % (t, sql))


def exception_logging(etype, value, tb, func_name=''):
    """
    用于__exit__中向logging输出traceback和错误信息
    :param etype: 错误类型
    :param value: 错误说明
    :param tb: traceback信息
    :param func_name: 错误打印函数名
    :return: None
    """
    if tb:
        err_info = ''.join(['Traceback: '] + traceback.format_tb(tb))
        err_info = err_info.replace('\n', ' ')
        logging.info(err_info)
    if etype or value:
        func_str = 'in func <' + func_name + '>: ' if func_name else ''
        err_type = func_str + ''.join(traceback.format_exception_only(etype, value))
        # print "funcstr: ", func_str
        err_type = err_type.replace('\n', '')
        # print err_type
        logging.error(err_type)


# raise error object(错误定义对象)
class DBError(Exception):
    pass


class MultiColumnsError(DBError):
    pass


# database engine object(数据库引擎对象)
class _Engine(object):
    def __init__(self, connect):
        """
        :param connect: mysql db connect
        :return: none
        """
        self._connect = connect

    def connect(self):
        """
        :return: pointer of mysql connect
        """
        return self._connect()

# global engine object:
engine = None


def create_engine(user, password, database, host='127.0.0.1', port=3306, **kw):
    """
    创建engine连接
    :param user: database username
    :param password: database password
    :param database: database name
    :param host: host
    :param port: port
    :param kw: 其他参数
    :return: None
    """
    global engine
    if engine is not None:
        raise DBError("Engine is already initialized.")
    params = dict(user=user, password=password, database=database, host=host, port=port)
    defaults = dict(use_unicode=True, charset='utf8', collation='utf8_general_ci', autocommit=False)
    for k, v in defaults.iteritems():
        params[k] = kw.pop(k, v)
    params.update(kw)
    # 使用lambda可以使mysql.connector.connect(**params)整体变为一个回调函数，只有在实际调用时才会运行函数。
    engine = _Engine(lambda: mysql.connector.connect(**params))
    # buffered参数：cursor是否立即返回fetch结果（缓存区）
    params['buffered'] = True
    # test connection...
    logging.info('Init mysql engine <%s> ok.' % hex(id(engine)))


# 数据库底层连接封装
class _LasyConnection(object):
    """
    惰性连接封装，仅当需要调用cursor时才连接数据库
    """
    def __init__(self):
        self.connection = None

    def cursor(self):
        """
        只有当需要调用cursor时才会连接数据库
        :return: None
        """
        if self.connection is None:
            conn = engine.connect()
            # logging.info('open connection <%s>...' % hex(id(connection)))
            logging.info('[CONNECTION] [OPEN] connection <%s>...' % hex(id(connection)))
            self.connection = conn
        return self.connection.cursor()

    def commit(self):
        return self.connection.commit()

    def rollback(self):
        return self.connection.rollback()

    def cleanup(self):
        """
        关闭数据库连接
        :return: None
        """
        if self.connection:
            conn = self.connection
            self.connection = None
            # logging.info('close connection <%s>...' % hex(id(connection)))
            logging.info('[CONNECTION] [CLOSE] connection <%s>...' % hex(id(connection)))
            conn.close()


# 持有数据库连接的上下文对象:
# threading.local的继承，会对每一个线程生成新的局部变量，即使_db_ctx是全局的
class _DbCtx(threading.local):
    def __init__(self):
        super(_DbCtx, self).__init__()
        self.connection = None
        self.transactions = 0

    def is_init(self):
        """
        判断数据库是否初始化（建立连接）
        :return:
        """
        return self.connection is not None

    def init(self):
        """
        建立数据库连接
        :return:
        """
        logging.info("open lazy connection ...")
        self.connection = _LasyConnection()
        self.transactions = 0

    def cleanup(self):
        """
        关闭数据库连接
        :return:
        """
        self.connection.cleanup()
        self.connection = None

    def cursor(self):
        return self.connection.cursor()


_db_ctx = _DbCtx()


# 用于with方法，建立数据库连接
class _ConnectionCtx(object):
    def __enter__(self):
        global _db_ctx
        self.should_cleanup = False
        if not _db_ctx.is_init():
            _db_ctx.init()
            self.should_cleanup = True
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        exception_logging(exc_type, exc_value, exc_tb, "_ConnectionCtx.__exit__")
        global _db_ctx
        if self.should_cleanup:
            _db_ctx.cleanup()


# 当外部调用多条语句，使用with connection()的用法时，只打开一条数据库连接，和__enter__中的if判断有关
def connection():
    return _ConnectionCtx()


def with_connection(func):
    """
    装饰器
    :param func:
    :return:
    """
    @functools.wraps(func)
    def wrapper(*args, **kw):
        with connection():
            return func(*args, **kw)
    return wrapper


@with_connection
def _select(sql, first, *args):
    """
    select实现函数
    :param sql:
    :param first:
    :param args:
    :return:
    """
    global _db_ctx
    cursor = None
    names = []
    sql = sql.replace('?', '%s')
    logging.info("Sql: %s, Args: %s" % (sql, args))
    try:
        cursor = _db_ctx.cursor()
        cursor.execute(sql, args)
        if cursor.description:
            names = [x[0] for x in cursor.description]
        if first:
            values = cursor.fetchone()
            if not values:
                return None
            return Dict(names, values)
        return [Dict(names, x) for x in cursor.fetchall()]
    finally:
        if cursor:
            cursor.close()


# @with_connection
def select_one(sql, *args):
    return _select(sql, True, *args)


# @with_connection
def select_int(sql, *args):
    d = _select(sql, True, *args)
    # 若只返回一条记录，则为DICT，此时仍为一条记录
    if len(d) != 1 and not isinstance(d, Dict):
        raise MultiColumnsError('Expect only one column.')
    return d.values()[0]


# @with_connection
def select(sql, *args):
    return _select(sql, False, *args)


@with_connection
def _update(sql, *args):
    """
    update实现函数
    :param sql:
    :param args:
    :return:
    """
    global _db_ctx
    sql = sql.replace('?', '%s')
    cursor = None
    logging.info("Sql: %s, Args: %s" % (sql, args))
    try:
        cursor = _db_ctx.cursor()
        cursor.execute(sql, args)
        r = cursor.rowcount
        # 当不处于事务状态下，需要提交
        if _db_ctx.transactions == 0:
            _db_ctx.connection.commit()
            logging.info("auto commit")
        return r
    finally:
        if cursor:
            cursor.close()


def update(sql, *args):
    return _update(sql, *args)


def insert(table, **kw):
    """
    Execute insert SQL.
    :param table: 表明
    :param kw: sql参数
    :return: 影响行数

    >>> update("delete from user where id = 2000")
    1
    >>> u1 = dict(id=2000, name='Bob', email='bob@test.org', passwd='bobobob', last_modified=time.time())
    >>> insert('user', **u1)
    1
    >>> u2 = select_one('select * from user where id=?', 2000)
    >>> u2.name
    u'Bob'
    >>> insert('user', **u2)
    Traceback (most recent call last):
      ...
    IntegrityError: 1062 (23000): Duplicate entry '2000' for key 'PRIMARY'
    """
    # 加*是zip的参数，把迭代器转化为list
    cols, args = zip(*kw.iteritems())
    sql = 'insert into %s (%s) values (%s)' % \
          (table, ','.join(['%s' % col for col in cols]), ','.join(['?' for i in range(len(cols))]))
    return _update(sql, *args)


class _TransactionCtx(object):
    def __enter__(self):
        global _db_ctx
        self.should_close_conn = False
        if not _db_ctx.is_init():
            _db_ctx.init()
            self.should_close_conn = True
        _db_ctx.transactions += 1
        logging.info('begin transaction...' if _db_ctx.transactions == 1 else 'join current transaction...')
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        exception_logging(exc_type, exc_value, exc_tb, "_TransactionCtx.__exit__")
        global _db_ctx
        # _db_ctx.transactions = _db_ctx.transactions - 1
        _db_ctx.transactions -= 1
        try:
            if _db_ctx.transactions == 0:
                if exc_type is None:
                    self.commit()
                else:
                    self.rollback()
        finally:
            if self.should_close_conn:
                _db_ctx.cleanup()
            logging.info('end transaction...')

    @staticmethod
    def commit():
        global _db_ctx
        logging.info('commit transaction...')
        try:
            _db_ctx.connection.commit()
            logging.info('commit ok.')
        except:
            logging.warning('commit failed. try rollback...')
            _db_ctx.connection.rollback()
            logging.warning('rollback ok.')
            raise

    @staticmethod
    def rollback():
        global _db_ctx
        logging.warning('rollback transaction...')
        _db_ctx.connection.rollback()
        logging.info('rollback ok.')


def transaction():
    return _TransactionCtx()


def with_transaction(func):
    start = time.time()

    @functools.wraps(func)
    def wrapper(*args, **kw):
        with transaction():
            func(*args, **kw)
        _profiling(start)
    return wrapper


# curs = _LasyConnection().cursor()的写法会导致弱连接（连接可能被垃圾回收），导致报错；应写为2句话
if __name__ == "__main__":
    create_engine('root', 'password', 'test')

    # print update('insert into user (id, name) values (?, ?)', '4', 'John')
    # print select("select * from user")

    #update('drop table if exists user')
    #update('create table user (id int primary key, name text, email text, passwd text, last_modified real)')

    #update("delete from user where id =?", 1)
    #u1 = dict(id=1, name='Bob', email='bob@test.org', passwd='bobobob', last_modified=time.time())
    #insert('user', **u1)
    #print select("select * from user")
    import doctest
    doctest.testmod(verbose=True)
