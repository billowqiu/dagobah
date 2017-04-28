#!/bin/env python
# -*- coding: utf-8 -*-
#########################################################################
# File Name: dagobah/backend/redis.py
# Author: billowqiu
# mail: billowqiu@163.com
# Created Time: 2017-04-21 14:48:17
# Last Changed: 2017-04-21 15:01:08
#########################################################################
import redis
import json
from dateutil import parser
import datetime
print redis.__file__
from ..backend.base import BaseBackend
TRUNCATE_LOG_SIZES_CHAR = {'stdout': 500000,
                           'stderr': 500000}

import inspect
def get_current_function_name():
    return inspect.stack()[1][3]

def log(func):
    def wrapper(*args, **kv):
        print('call %s func, args: %s, kv: %s' % (func.__name__, args, kv))
        return func(*args, **kv)
    return wrapper

class RedisBackend(BaseBackend):
    def __init__(self, host, port, db, dagobah_key='dagobah', job_key='dagobah_job', log_key='dagobah_log'):
        super(RedisBackend, self).__init__()
        self.host = host
        self.port = port
        self.db = db
        self.dagobah_key =  dagobah_key
        self.job_key = job_key
        self.log_key = log_key

        self.client = redis.StrictRedis(self.host, self.port, self.db)
    
    def __repr__(self):
        return '<RedisBackend> host:%s, port:%d, db:%d' % (self.host, self.port, self.db)

    #从backend中获取已知的dagobah id
    def get_known_dagobah_ids(self):
        dagobah_ids = self.client.hkeys(self.dagobah_key)
        print dagobah_ids
        return dagobah_ids

    #通过dagobah_id得到对应的dagobah
    def get_dagobah_json(self, dagobah_id):
        dagobah_json = self.client.hget(self.dagobah_key, dagobah_id)
        #将json字符串转回为python数据结构
        dagobah_json = json.loads(dagobah_json)
        return dagobah_json

    def commit_dagobah(self, dagobah_json):
        #json字符串必须是双引号，这里dumps之后会将单引号转为双引号
        dagobah_id = dagobah_json['dagobah_id']
        dagobah_json = json.dumps(dagobah_json)
        self.client.hset(self.dagobah_key, dagobah_id, dagobah_json)

    def delete_dagobah(self, dagobah_id):
        self.client.hdel(self.dagobah_key, dagobah_id)

    def commit_job(self, job_json):
        #json字符串必须是双引号，这里dumps之后会将单引号转为双引号
        job_name = job_json['name']
        job_json = json.dumps(job_json)
        self.client.hset(self.job_key, job_name, job_json)

    def delete_job(self, job_name):
        self.client.hdel(self.job_key, job_name)

    def commit_log(self, log_json):
        #取出log_id作为key，上层框架会多次调用这个函数
        print "commit_log: %s" % log_json
        log_id = log_json['log_id']
        job_id = log_json['job_id']

        append = {'save_date': str(datetime.datetime.utcnow())}

        for keys, values in log_json.items():
            if keys == 'tasks':
                task_count = 0
                task_complete_count = 0
                for task_name, task_values in values.items():

                    for key, size in TRUNCATE_LOG_SIZES_CHAR.iteritems():
                        if isinstance(task_values.get(key, None), str):
                            if len(task_values[key]) > size:
                                task_values[key] = '\n'.join([task_values[key][:size/2],
                                                         'DAGOBAH STREAM SPLIT',
                                                         task_values[key][-1 * (size/2):]])
                    #按照json存储，需要先将datetime类型转换为str
                    for k,v in task_values.items():
                        if isinstance(v, datetime.datetime):
                            #print "k:%s, v: %s, type: %s" % (k, v, type(v))
                            task_values[k] = str(v)
                    
                    task_count += 1   
                    #把已经完成的task log先存储下来，即有complete_time字段
                    if task_values.has_key('complete_time'):
                        task_complete_count += 1
                        #self.client.hset(job_id+':'+task_name, log_id, json.dumps(dict((log_json.items() + append.items()))))            

            if isinstance(values, datetime.datetime):                    
                log_json[keys] = str(values)
        
        if task_count > 0 and (task_count == task_complete_count):
            print "-------commit_log result-------: %s" % dict((log_json.items() + append.items()))
            log_result = json.dumps(dict(log_json.items() + append.items()))
            self.client.lpush(job_id, log_result)
            #这里为了方便按照log_id查找
            self.client.set(job_id+':'+log_id, log_result)

        #print "commit_log end"

    @log
    def get_latest_run_log(self, job_id, task_name):
        log = self.client.lrange(job_id, 0, 0)
        return json.loads(log[0])

    @log    
    def get_run_log_history(self, job_id, task_name, limit=10):
        print "get_run_log_history: %s" % job_id

        logs = self.client.lrange(job_id, 0, limit)
        ret_logs = []
        for log in logs:
            ret_logs.append(json.loads(log))
        return ret_logs

    @log
    def get_run_log(self, job_id, task_name, log_id):
        log = json.loads(self.client.get(job_id+':'+log_id))
        #只需要当前task_name的数据
        log = log['tasks'][task_name]
        return log
        # q = {'job_id': ObjectId(job_id),
        #      'tasks.%s' % task_name: {'$exists': True},
        #      'log_id': ObjectId(log_id)}
        # return self.log_coll.find_one(q)['tasks'][task_name]

