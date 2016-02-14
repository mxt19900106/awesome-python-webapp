#!/usr/bin/env python
# -*- coding: utf-8 -*-


__author__ = 'hlsky'


'''
description: test file
'''


import time
import uuid
import logging
import threading
import sys
import traceback

#test Dict
class Dict(dict):
    '''
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
    '''
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


logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s  [%(levelname)s] [%(filename)s] [line:%(lineno)d]  %(message)s',
                    datefmt='%d.%b.%Y %H:%M:%S',
                    # filename='myapp.log',
                    filemode='w')

#test mysql 连接
'''
# 导入MySQL驱动:
import mysql.connector
# 注意把password设为你的root口令:
conn = mysql.connector.connect(user='root', password='password', database='test', use_unicode=True)
cursor = conn.cursor()
# 创建user表:
#cursor.execute('create table user (id varchar(20) primary key, name varchar(20))')
# 插入一行记录，注意MySQL的占位符是%s:
#cursor.execute('insert into user (id, name) values (%s, %s)', ['2', 'Tom'])
#print cursor.rowcount
# 提交事务:
#conn.commit()
#cursor.close()
# 运行查询:
#cursor = conn.cursor()
cursor.execute('select * from user where id = %s', ['1'])
values = cursor.fetchall()
names = [x[0] for x in cursor.description]
names = []
print names
print [Dict(names,x) for x in values]
print values
# 关闭Cursor和Connection:
cursor.close()

conn.close()
'''

#test git
'''
print "test git in pycharm"
print "test dev branch"
'''

#test some dict func
'''
kw = {}
params = dict(user='user', password='password', database='database', host='host', port='port')
defaults = dict(use_unicode=True, charset='utf8', collation='utf8_general_ci', autocommit=False)
for k, v in defaults.iteritems():
    #print k,v
    params[k] = kw.pop(k, v)

print params
'''

#test uuid,可以用于生成唯一ID
'''
def next_id(t=None):
    if t is None:
        t = time.time()
    return '%015d%s000' % (int(t * 1000), uuid.uuid4().hex)

print next_id()
'''

#test logging

#print logging.debug("test")
#print logging.info("test")
'''
logging.basicConfig(level=logging.DEBUG,
                format='%(asctime)s  [%(levelname)s] %(filename)s[line:%(lineno)d]  %(message)s',
                datefmt='%d.%b.%Y %H:%M:%S',
                #filename='myapp.log',
                filemode='w')

logging.debug('This is debug message')
logging.info('This is info message')
logging.warning('This is warning message')
logging.warning("test")
'''

#test lambda
'''
def a():
    a = 1
    print 'test'

print a
#a()
b = lambda:a()
print b
#b()
'''


#test thread.local
'''
class a(object):
    def __init__(self):
        pass
class ThreadTest(threading.local):
    def __init__(self):
        self.count = 0

    def count_plus(self):
        #self.count = a()
        self.count = self.count + 1
        print self.count
        time.sleep(10)

thd_test = ThreadTest()

def test_thd():
    global thd_test
    #thd_test = ThreadTest()
    thd_test.count_plus()

for i in range(10):
    t1 = threading.Thread(target=test_thd)
    t1.start()
    #t1.join()
    #time.sleep(2)
'''

#test dict.values
'''
#第一元素是固定的？
d = { u'test':'djklsj',u'name':u'M', u'id':u'2',u'value': '1'}
d = Dict(test='djk', name = 'M', id = '2', value = '1')
print d
print d.values()
'''

#test 父类、子类
'''
class parent(object):
    def __init__(self):
        print "this is parent"

class son(parent):
    def __init__(self):
        super(son, self).__init__()
        print "this is son"

s = son()
'''




#test with and exception print
'''
def exception_logging(etype, value, tb):
    if tb:
        err_info = ''.join(['Traceback: ']+ traceback.format_tb(tb))
        err_info = err_info.replace('\n', ' ')
        logging.info(err_info)
    if etype or value:
        err_type = ''.join(traceback.format_exception_only(etype, value))
        logging.error(err_type)

class withtest(object):
    def __enter__(self):
        print "with begin"

    def __exit__(self, exctype, excvalue, tb):
        #print exctype.__name__
        #print excvalue
        #print dir(tb)
        #traceback.print_tb(tb)
        #traceback.print_exc()
        #err = ''.join(format_exception(exctype, excvalue, tb))
        #err = err.replace('\n', ' ')
        exception_logging(exctype, excvalue, tb)
        #ogging.error(err)
        print "with end"

#print sys.exc_info()
with withtest():
    print  [i for i in range(10)]
    [x/0 for x in range(1,200,200)]
try:
    with withtest():
        print  [i for i in range(10)]
        [x/0 for x in range(1,200,200)]
except:
    pass
'''

#test transwarp.db
from www.transwarp import db
db.create_engine('root', 'password', 'test')
print db.select("select * from user")

print "test whether dev can push"