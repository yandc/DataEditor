#!/usr/bin/env python
# coding=utf-8
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

import json
import redis

#info = {
#    'user_id':3925615,
#    'content':'涨姿势了!这个麻麻分享的待产包真的hin专业！速速围观→',
#    'url':'miyabaobei://subject?id=10044009&push=personalized_post'
#}
#
#rds = redis.StrictRedis(host = '10.1.52.187')
#rds.lpush('app_custom_push_list', json.dumps(info))

#长文点击统计
import commands
begin = '20170830'
end = '20170831'
res = commands.getoutput('awk \'BEGIN{ORS=","}{print $3}\' /opt/parsed_data/personalized_post/%s/push_clicked_format.txt'%begin)
clicked = res.split(',')
res = commands.getoutput('mysql -h 10.1.117.2 -uspider -pspider material -e "select pid from pushlog where created>\'%s\' and created<\'%s\' and tmpIdx=10001 group by pid"'%(begin, end))
pset = set(res.split('\n')[1:])
print len([x for x in clicked if x in pset])
res = commands.getoutput('mysql -h 10.1.117.2 -uspider -pspider material -e "select pid from pushlog where created>\'%s\' and created<\'%s\' and tmpIdx=10002 group by pid"'%(begin, end))
pset = set(res.split('\n')[1:])
print len([x for x in clicked if x in pset])
