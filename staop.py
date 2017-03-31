#!/usr/bin/python
# coding=utf-8
import sys
import os
from AlgoCommonLib.config_util import *

def start(name, cmd):
    sh = 'nohup %s > log/%s.out 2>&1 &'%(cmd, name)
    ret = os.system(sh)
    if ret != 0:
        print 'Something wrong! cmd: %s'%sh
        
def stop(name):
    grep = 'ps x'
    for nm in name.split(':'):
        grep += '|grep %s'%nm
    sh = """%s|grep -v grep|grep -v staop|awk '{print $1}'"""%grep
    pids = os.popen(sh).read().split('\n')
    sh = """%s|grep -v grep|grep -v staop|awk '{for(i=5;i<=NF;i++)printf $i" ";printf "\\n"}'"""%grep
    cmds = os.popen(sh).read().split('\n')
    process = [(pids[i], cmds[i]) for i in range(len(pids)) if len(pids[i]) > 0]
    if len(process) > 1:
        print '\n'.join(cmds)
        print 'Process matched more than 1, continue? y/n'
        c = raw_input()
        if c != 'y':
            return
    for pid, cmd in process:
        sh = 'kill -9 %s'%pid
        ret = os.system(sh)
        if ret != 0:
            print 'Something wrong! cmd: %s'%sh

if __name__ == '__main__':
    usage = 'Usage: python staop.py start|stop appname\n\
@appname must from staop.cfg when start a process;\n\
@appname when stop a process, parts(splited by ":") used to grep process.'

    try:
        operation = sys.argv[1]
        name = sys.argv[2]
        if operation == 'start':
            cf = ConfigUtil('staop.cfg')
            cmd = '%s --mark=%s'%(cf.get(name, 'command'), name)
            start(name, cmd)
        elif operation == 'stop':
            stop(name)
        else:
            print usage
    except Exception, e:
        print str(e)
        print usage
