#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""description: encapsulation sql to achieve function which can connect and control mysql"""

import threading
import mysql.connector

__author__ = 'hlsky'

# db.py

DB_USER = 'root'
DB_PASSWORD = 'password'
DB_NAME = 'test'

#数据库底层连接封装
class _LasyConnection(object):
    def __init__(self):
        self.connect = mysql.connector.connect(user=DB_USER, password=DB_PASSWORD, database=DB_NAME, use_unicode=True)
        self.cursormysql = self.connect.cursor()

    def cursor(self):
        return self.cursormysql

    def connect(self):
        return self.connect

    def cleanup(self):
        self.cursormysql.close()
        self.connect.close()

# 数据库引擎对象:
class _Engine(object):
    def __init__(self, connect):
        self._connect = connect

    def connect(self):
        return self._connect()

engine = None


# 持有数据库连接的上下文对象:
class _DbCtx(threading.local):
    def __init__(self):
        self.connection = None
        self.transactions = 0

    def is_init(self):
        return self.connection is not None

    def init(self):
        self.connection = _LasyConnection()
        self.transactions = 0

    def cleanup(self):
        self.connection.cleanup()
        self.connection = None

    def cursor(self):
        return self.connection.cursor()

_db_ctx = _DbCtx()


class _ConnectionCtx(object):
    def __enter__(self):
        global _db_ctx
        self.should_cleanup = False
        if not _db_ctx.is_init():
            _db_ctx.init()
            self.should_cleanup = True
        return self

    def __exit__(self, exctype, excvalue, traceback):
        global _db_ctx
        if self.should_cleanup:
            _db_ctx.cleanup()

def connection():
    return _ConnectionCtx()

def with_connection(func):
    def wrapper(*args, **kw):
        with connection():
            return func(*args, **kw)
    return wrapper

@with_connection
def select(sql, *args):
    global _db_ctx
    _db_ctx.cursor().execute(sql, *args)
    #_db_ctx.cursor().execute("select * from user where id = ?", ('1'))
    return _db_ctx.cursor().fetchall()


@with_connection
def update(sql, *args):
    pass

class _TransactionCtx(object):
    def __enter__(self):
        global _db_ctx
        self.should_close_conn = False
        if not _db_ctx.is_init():
            _db_ctx.init()
            self.should_close_conn = True
        _db_ctx.transactions = _db_ctx.transactions + 1
        return self

    def __exit__(self, exctype, excvalue, traceback):
        global _db_ctx
        _db_ctx.transactions = _db_ctx.transactions - 1
        try:
            if _db_ctx.transactions==0:
                if exctype is None:
                    self.commit()
                else:
                    self.rollback()
        finally:
            if self.should_close_conn:
                _db_ctx.cleanup()

    def commit(self):
        global _db_ctx
        try:
            _db_ctx.connection.commit()
        except:
            _db_ctx.connection.rollback()
            raise

    def rollback(self):
        global _db_ctx
        _db_ctx.connection.rollback()


if __name__ == "__main__":
    print select("select * from user where id = ?", '1')