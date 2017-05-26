#!/usr/bin/env python
# coding=utf-8
from orm_util import *

Article = getOrmModel('material', 'article')
LinkMap = getOrmModel('material', 'linkmap')
Offset = getOrmModel('material', 'offset')
Links = getOrmModel('material', 'links')
Avatar = getOrmModel('material', 'avatar')
SpyLog = getOrmModel('material', 'spylog')
PostStats = getOrmModel('material', 'poststats')
PostInfo = getOrmModel('material', 'postinfo')
RelateSku = getOrmModel('mia', 'item')
Koubei = getOrmModel('mia_group', 'koubei')
Subject = getOrmModel('mia_group', 'group_subjects')
ItemCatgy = getOrmModel('mia', 'item_category_ng')
BaseProject = None
BaseInfo = None
BaseIvent = None
MatchGraph = None

AdminLeads = None
AdminProj = None
AdminIvent = None
AdminFund = None
AdminCatgy = None
Category = None

MonitorLog = None
Leads = None
Ivent = None
Investor = None
Fund = None
Investment = None
FundMap = None
News = None
JuziData = None

OrgDetail = None

def pushInto(model, info, where=[], fnOnExist=None):
    if len(where) == 0:
        mdl = model(**info)
        mdl.save()
        return True, mdl
    kwargs = {'defaults':info}
    for field in where:
        kwargs[field] = info[field]

    row, isCreate = model.get_or_create(**kwargs)
    if isCreate == False:
        if fnOnExist != None:
            fnOnExist(model, row, info)
        else:
            mdl = model(**info)
            mdl.id = row.id
            mdl.save()
        return 0
    return 1
