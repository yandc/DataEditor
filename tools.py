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
    if click == 0 or expose == 0:
        return 0
    elif expose < click:
        click = expose
    p = min(float(click)/expose, 0.95)
    n = expose
    z = 1.96
    z2 = z*z
    a = p+z2/(2*n)-z*math.sqrt(p*(1-p)/n+z2/(4*n*n))
    b = 1+z2/n
    return round(a/b, 4)
    
templates = [
    ('跟你一起关注%name的人早就下单且美得不可方物，愿意接受这样的差距你就别点开→', 365, 208, 194, 341),
    ('天呐噜~你关注的【%name】已经成为网红爆款，%age宝宝都在用！想知道妈咪们肿么评价TA？请戳→', 54, 329, 43, 102, 189, 14, 63),
    ('%age宝宝都在玩这款%name，没TA你别跟我谈什么德智体美全面发展，点我→', 83,290 ),
    ('%age宝宝都在吃这款%name，看看你家宝贝是不是已被甩了几条街了→', 12, 28, 1),
    ('你感兴趣的【%name】已经有人败了，快过来看看这位妈妈怎么说→', 0),
    ('还在纠结要不要入【%name】？有人已经快你一步下手了，她有话对你说→',0),
    ('嘿~暗中观察你很久了！知道这件宝贝【%name】你很心水，来看别人怎么评价它吧！', 0),
    ('我等的花儿都谢了~从你关注【%name】至今已有两个世纪，为什么还没下单？这个理由够不够？', 0),
    ('不敢保证这件%name会让你的生活有质的飞跃，但至少每一次量变都能让你离质变更近一步，戳开→',0)
]

def getTextFromTemplate(index, catgyId, varDict):
    text = ''
    idx = 0
    for temp in templates:
        if temp[1] == 0 or catgyId not in temp or temp[0].count('%') !=len(varDict):
            idx += 1
            continue
        text = temp[0]
        for name, value in varDict.iteritems():
            rep = '%'+name
            if temp[0].find(rep) >= 0:
                text = text.replace(rep, value)
        break
    if text:
        return idx, text
    
    idx = index%len(templates)
    tempList = templates[idx:]+templates[:idx]
    for temp in tempList:
        if temp[1] != 0:
            idx += 1
            continue
        text = temp[0]
        for name, value in varDict.iteritems():
            rep = '%'+name
            if temp[0].find(rep) >= 0:
                text = text.replace(rep, value)
        break
    return idx%len(templates), text

def getAgeDesc(month):
    year = month/12
    month = month%12
    desc = ''
    if year == 0:
        desc = '%s个月'%month
    else:
        i = month/3
        if i==1 or i==2:
            desc = '%s岁半'%year
        else:
            if i == 3:
                year += 1
            desc = '%s岁'%year
    return desc.decode()

def uploadFile(path, fname):
    link = 'http://10.1.115.114:12001/d1'
    header = {'real_filename':'/opt/fsroot/p5/subjects/%s'%fname, 'expect':'100-continue'}
    fp = open(path+fname, 'rb')
    resp = requests.post(link, data=fp.read(), headers=header)
    fp.close()
    if resp.status_code == 200:
        #return 'http://mia-img.ks3-cn-beijing.ksyun.com/d1/p5/subjects/%s'%fname
        return 'http://img01.miyabaobei.com/d1/p5/subjects/%s'%fname
    else:
        return ''
