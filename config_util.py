#!/usr/bin/env python
# coding=utf-8
from ConfigParser import *
import logging
import os

class ConfigUtil:
    def __init__(self, config):
        self.__cf = ConfigParser()
        self.__cf.read(config)
        self.__cfs = []
        if 'global' in self.__cf.sections() and 'include' in self.__cf.options('global'):
            try:
                dirname = os.path.dirname(config)
                if len(dirname) > 0:
                    dirname += '/'
                for cfg in self.__cf.get('global', 'include').split(';'):
                    if cfg[0] != '/':
                        cfg = dirname+cfg
                    cf = ConfigUtil(cfg)
                    self.__cfs.append(cf)
            except Exception, e:
                logging.error('Included config file error: %s'%(str(e)))

    def get(self, sect, item):
        try:#local
            return self.__cf.get(sect, item)
        except Exception, e:
            pass

        try:#global
            return self.__cf.get('global', item)
        except Exception, e:
            pass

        for cf in self.__cfs:
            try:#include
                return cf.get(sect, item)
            except Exception, e:
                pass
        raise TypeError('[%s, %s] does not exist.'%(sect, item))

    def getboolean(self, sect, item):
        s = self.get(sect, item).lower()
        if s == 'yes' or s == 'y' or s == '1':
            return True
        else:
            return False

    def sections(self):
        return [sect for sect in self.__cf.sections() if sect != 'global']
        
