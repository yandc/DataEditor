#!/usr/bin/env python
# coding=utf-8
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
from editor import *
import time
reload(logging)

logfmt = '%(asctime)s %(filename)s[%(funcName)s %(lineno)d] %(levelname)s %(message)s'
logdatefmt = '%d %b %Y %H:%M:%S'

if __name__ == "__main__":
    #class(args...)
    option = {}
    cmds = []
    for i in range(1, len(sys.argv)):
        if sys.argv[i].find('--') == 0:
            opt = sys.argv[i][2:].lower().split('=')
            if len(opt) == 2:
                option[opt[0]] = opt[1]
            else:
                option[opt[0]] = True
        else:
            cmds.append(sys.argv[i])

    if 'log' in option:
        logging.basicConfig(level=logging.INFO, format=logfmt, datefmt=logdatefmt, filename=option['log'], filemode='w')

    else:
        logging.basicConfig(level=logging.INFO, format=logfmt, datefmt=logdatefmt)

    while True:
        for cmd in cmds:
            exec('obj = %s'%cmd)
            obj.run()
            del obj
        if 'period' in option:
            time.sleep(int(option['period'])*60)
        else:
            break
