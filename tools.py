#!/usr/bin/env python
# coding=utf-8
from constant import *
from model import *
import json
import urllib
import logging
import requests
import time
import hashlib
import pdb

quality_field = {'project':['host', 'companyName', 'brief', 'des', 'label', 'teamInfo'],
                 'investment':['companyName', 'stage', 'scale', 'fundNames', 'investTime']}
banned = set(['暂未收录', '暂无', 'weibo.com', 'www.11467.com',
              'www.baidu.com', 'baike.baidu.com', 'www.pgyer.com',
              '36kr.com', 'mp.weixin.qq.com', 'itunes.apple.com',
              'android.myapp.com', 'www.lagou.com', 'www.wandoujia.com',
              '未透露', '不祥', 'N/A', '非公开', '不明确', 'N,A', '--', '', None])
def calc_quality(info, category):
    sc = 0
    for field in quality_field[category]:
        if field not in info or info[field] in banned:
            continue
        if category == 'project':
            sc += 1
        elif category == 'investment':
            sc += 1
    return sc

def obj2string(obj):
    s = json.dumps(obj, ensure_ascii=False)
    res = ''
    for c in s:
        if c in set(['{', '}', '[', ']', '"']):
            continue
        res += c
    return res

def get_format_stage(stage):
    stg = stage.lower().replace('轮', '').replace('series', '').replace('vc-', '').replace(' ', '').replace('(', '').replace(')', '').encode()
    if stg in rounds:
        return rounds[stg].decode()
    else:
        return stg

def decodeSrc(fromId):
    return fromId/FROM_ID_BASE, fromId%FROM_ID_BASE

def encodeSrc(srcType, srcId):
    return srcType*FROM_ID_BASE + srcId

def getIdByUrl(url):
    try:
        row = BaseProject.select().where(BaseProject.fromUrl==url).get()
        return row.matchId, row.fromId
    except:
        return 0, 0

def getFromSearch(url):
    num = 1
    fromIdList = set()
    while True:
        try:
            _url = url + '&index=%s'%num
            res = json.loads(urllib.urlopen(_url).read())
            size = int(res['count'])
            for x in res['pl_list']:
                xtype = x['type']
                factor = 1
                if x['id'] < 0:
                    xtype = -xtype
                    factor = -1
                if xtype in ID_SWITCHER:
                    fromIdList.add(ID_SWITCHER[xtype] + factor*x['id'])
            if size < SEARCH_PAGESIZE:
                break
            num += 1
        except Exception, e:
            logging.error('Search error: %s, %s'%(str(e), url))
    return list(fromIdList)

def uploadImage(imgType, fromLink, path=None):
    mdl = None
    try:
        mdl = LinkMap.select().where(LinkMap.fromLink==fromLink).get()
        if not mdl.toLink:
            return 'broken', 'Fail link.'
        if mdl.srvLink:
            return 'succ', mdl.srvLink
        if path == None:
            path = mdl.toLink
    except:
        if path == None:
            return 'empty', 'Empty Image Path.'
        
    ts = str(int(time.time()))
    params = '{"type":"user_returns","resource_id":"0","timestemp":"%s"}'%ts
    args = {
        'params':params,
        'resource_id':0,
        'sign':hashlib.md5(params+'MIABABAY_ID_UP_2014!@').hexdigest(),
        'timestamp':ts,
        'type':'user_returns'
    }
    
    idx = path.rfind('/')
    name = path[idx+1:]
    if name[-3:].lower() in ('jpg', 'bmp', 'png', 'gif', 'pic'):
        name = '%s.%s'%(name[:-4], name[-3:])
    elif name[-4:].lower() in ('jpeg'):
        name = '%s.%s'%(name[:-5], 'jpg')
    else:
        name += '.jpg'

    url = 'http://uploads.miyabaobei.com/app_upload.php'
    try:
        files = {'Filedata':(name, open(path, 'rb'), 'image/jpeg')}
        resp = requests.post(url, data=args, files=files)
        result = json.loads(resp.content)
        if result['code'] != 200:
            logging.error(result['content'])
            return 'fail', result['content']
        else:
            if mdl != None:
                mdl.srvLink = result['content']
                mdl.save()
            else:
                info = {
                    'type':imgType,
                    'fromLink':fromLink,
                    'toLink':path,
                    'srvLink':result['content']
                }
                pushInto(LinkMap, info, ['fromLink'])
            return 'succ', result['content']
    except Exception, e:
        return 'fail', str(e)

import math
def calcCrt(click, expose):
    if expose == 0:
        return 0
    p = min(float(click)/expose, 0.95)
    n = expose
    z = 1.96
    z2 = z*z
    a = p+z2/(2*n)-z*math.sqrt(p*(1-p)/n+z2/(4*n*n))
    b = 1+z2/n
    return round(a/b, 4)
    
