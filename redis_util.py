#!/usr/bin/python
# -*- coding: utf-8 -*-
import redis
import datetime
import json

ENV_TYPE = getEnvType()

class RedisUtil:
    def __init__(self):
        if ENV_TYPE == ENV_ONLINE:
            host = '192.168.1.12'
            port = 6380
        else:
            host = '10.251.163.113'
            port = 6380
        self.__inst = redis.StrictRedis(host=host, port=port, db=0)

    def get_number(self, key):
        key = str(key)
        if self.__inst.exists(key):
            return int(self.__inst.get(key))
        else:
            self.__inst.set(key, 0)
            return 0
        
    def set_number(self, key, value):
        key = str(key)
        self.__inst.set(key, str(value))
        
    def get_obj(self, key):
        key = str(key)
        if self.__inst.exists(key):
            return json.loads(self.__inst.get(key))
        else:
            return None
        
    def set_obj(self, key, value):
        key = str(key)
        self.__inst.set(key, json.dumps(value))

    def get(self, key):
        return self.__inst.get(key)
    
    def set(self, key, value):
        return self.__inst.set(key, value)

    def keys(self, keys):
        return self.__inst.keys(keys)

    def delete(self, key):
        return self.__inst.delete(key)


if __name__ == '__main__':
    redis = RedisUtil()
    tt = redis.get('ethspy:lastOnlineTime')
    print tt
