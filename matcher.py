#!/user/bin/env python
# coding=utf-8
from model import *
from tools import *
import logging
import sys
import urllib
import pygraphviz as pgv
import json
import datetime

def graph(node, srcModel):
    table = srcModel._meta.db_table
    if node.fromId == 0:
        return
    #deadline = datetime.datetime.now()-datetime.timedelta(days=35)
    #if type(node.updateTime) == type(deadline) and node.updateTime < deadline and table != 'base_project':#too old
    #    return
    queryFail = False
    rows = MatchGraph.select().where(MatchGraph.toId==node.fromId)
    strategy = set(range(len(strategy_list)))
    for row in rows:#avoid repeat search
        keyword = strategy_list[row.matchType].split('_')[-1]
        if getattr(node, keyword).lower().find(row.matchWord.lower()) >= 0 and row.matchType in strategy:
            strategy.remove(row.matchType)

    for keyword, matchType in [(strategy_list[x], x) for x in strategy]:
        if keyword == 'total_projectName':
            fromId_search = set([x.fromId for x in BaseProject.select().where(BaseProject.projectName==node.projectName)])
            if table != 'base_project':
                fromId_search |= set([x.fromId for x in srcModel.select().where(srcModel.projectName==node.projectName)])
        elif hasattr(node, keyword):
            value = getattr(node, keyword)
            if len(value) == 0 or value.encode() in banned:
                continue
            if keyword == 'companyName' and (len(value) <= 4
                                             or value == node.projectName
                                             or value.find('公司') < 0):#companyName length must more then 4
                continue
            
            if keyword == 'projectName':
                url = search_url%(UNUSABLE_UPPER_BOUND)+urllib.urlencode({'query':value})+'&ttlsearch=1'
            else:
                url = search_url%(UNUSABLE_UPPER_BOUND)+urllib.urlencode({'query':value})+'&ttlsearch=4'
            while True:
                try:
                    search_res = json.loads(urllib.urlopen(url).read())
                    break
                except Exception, e:
                    logging.error('Search error: %s, %s'%(str(e), url))
                    time.sleep(5)
                    
            if int(search_res['matchcount']) > UNUSABLE_UPPER_BOUND:
                logging.warning('%s(%s) unusable: %s'%(keyword, node.id, value))
                continue
            fromId_search = set()
            for x in search_res['pl_list']:
                xtype = x['type']
                factor = 1
                if x['id'] < 0:
                    xtype = -xtype
                    factor = -1
                if xtype in ID_SWITCHER:
                    fromId_search.add(ID_SWITCHER[xtype] + factor*x['id'])
        else:
            continue
        
        if len(fromId_search) == 0:
            logging.error('No result: %s(%s): %s'%(keyword, node.id, getattr(node, keyword.split('_')[-1])))
            queryFail = True
            continue
            
        if keyword == 'projectName':#exclude same name id
            st = set([x.fromId for x in BaseProject.select().where((BaseProject.fromId << list(fromId_search)) & (BaseProject.projectName != node.projectName))])
            fromId_search = st
            
        match_info = {}
        for toId in fromId_search:
            fromId = node.fromId
            if toId == fromId:
                continue
            if toId == 0:
                continue
            match_info['fromId'] = fromId
            match_info['toId'] = toId
            match_info['matchType'] = matchType
            match_info['matchWord'] = getattr(node, keyword.split('_')[-1])
            if table == 'base_project':#avoid unique restrict cause by investment
                try:
                    MatchGraph.update(toId=fromId).where((MatchGraph.toId==toId)&(MatchGraph.matchType==matchType)&(MatchGraph.matchWord==match_info['matchWord'])&(MatchGraph.fromId>encodeSrc(INVESTMENT_TYPE, 0))).execute()
                except:
                    pass
            pushInto(MatchGraph, match_info, ['toId', 'matchType', 'matchWord'], lambda x,y,z:False)
            
    if queryFail == True:
        srcModel.update(mark=srcModel.mark.bin_or(1)).where(srcModel.id==node.id).execute()
    elif node.mark&1 > 0:
        srcModel.update(mark=srcModel.mark.bin_and(~1)).where(srcModel.id==node.id).execute()

def traverse(fromId):
    graph = {}
    nodeSet = set([fromId])
    newNodeSet = set([fromId])
    while True:
        if len(nodeSet) > 128:
            break
        instr = ', '.join([str(x) for x in newNodeSet])
        rows = MatchGraph.select().where((MatchGraph.fromId << list(newNodeSet)) | (MatchGraph.toId << list(newNodeSet)))
        for cols in rows:
            fromId = cols.fromId
            toId = cols.toId
            matchType = cols.matchType
            matchWord = cols.matchWord
            if toId == 0:
                continue
            if toId not in graph:
                li = ['', '', '', '']
                li[matchType] = matchWord
                graph[toId] = {fromId:li}
            elif fromId not in graph[toId]:
                li = ['', '', '', '']
                li[matchType] = matchWord
                graph[toId][fromId] = li
            else:
                graph[toId][fromId][matchType] = matchWord
                
        nSet = set([x.fromId for x in rows])
        nSet |= set([x.toId for x in rows])
        newNodeSet = nSet - nodeSet
        nodeSet |= nSet
        if len(newNodeSet) == 0:
            break
    return graph, nodeSet

def fmtStr(s):
    return ''.join(s.split()).lower()
def confirm_edge(fromId, toId, graph, nodeInfo):
    edgeListIn = graph[toId][fromId]
    if toId in nodeInfo and edgeListIn[1].lower() == nodeInfo[toId].projectName.lower():#only have same projectName
        return True
#        if len(graph[toId]) == 1:
#            return True
#        if len(nodeInfo[toId]['host']) == 0 and len(nodeInfo[toId]['companyName']) == 0:
#            return True
    if toId in nodeInfo:
        if len(edgeListIn[2]) > 0 and nodeInfo[toId].host.lower().find(edgeListIn[2].lower()) >= 0:
            return True
        if len(edgeListIn[3]) > 0 and nodeInfo[toId].companyName.lower().find(edgeListIn[3].lower()) >= 0:
            return True
        if len(edgeListIn[3]) > 0 and nodeInfo[toId].projectName.lower().find(edgeListIn[3].lower()) >= 0:#projectName is companyName
            return True
    if len(edgeListIn[2]) > 0 and len(edgeListIn[3]) > 0:
        return True
    if (len(edgeListIn[0]) > 0 or len(edgeListIn[1]) > 0) and (len(edgeListIn[2]) > 0 or len(edgeListIn[3]) > 0):
        return True
    if fromId in graph and toId in graph[fromId]:#circle
        return True
    #if fromId in graph and len(set([x for x in graph[fromId]])&set([x for x in graph[toId]])) > 0:#triangle
    #    return True
    if toId in nodeInfo and len(edgeListIn[0]) > 0:#title splited by '/'
        fromNameSet = set(fmtStr(edgeListIn[0]).split('/'))
        toNameSet = set(fmtStr(nodeInfo[toId].projectName).split('/'))
        if len(fromNameSet&toNameSet) > 0:
            return True
    if fromId > 500000000 and fromId < 600000000:#investment
        if toId in nodeInfo:
            if edgeListIn[1].lower() == nodeInfo[toId].projectName.lower():#same projectName
                return True
            if len(edgeListIn[0]) > 0:
                pNames = nodeInfo[toId].projectName.lower().split('/')
                if len(pNames) > 1:
                    for pName in pNames:
                        if pName == edgeListIn[0].lower():
                            return True
        if toId > 500000000 and toId < 600000000:#investment to investment, same projectName
            return True
    return False

def calc_match(graph, nodeInfo):
    confirm = {}
    matchSetList = []
    for toId, fromDict in graph.iteritems():
        for fromId, edgeList in fromDict.iteritems():
            ret = confirm_edge(fromId, toId, graph, nodeInfo)
            if ret == True:
                if fromId in confirm and toId in confirm:
                    if confirm[fromId] != confirm[toId]:
                        setId1 = min(confirm[fromId], confirm[toId])
                        setId2 = max(confirm[fromId], confirm[toId])
                        for nodeId in matchSetList[setId2]:
                            confirm[nodeId] = setId1
                        matchSetList[setId1] |= matchSetList[setId2]
                        matchSetList[setId2] = None
                elif fromId in confirm:
                    setId = confirm[fromId]
                    matchSetList[setId].add(toId)
                    confirm[toId] = setId
                elif toId in confirm:
                    setId = confirm[toId]
                    matchSetList[setId].add(fromId)
                    confirm[fromId] = setId
                else:
                    confirm[fromId] = len(matchSetList)
                    confirm[toId] = len(matchSetList)
                    newSet = set([fromId, toId])
                    matchSetList.append(newSet)
            else:
                if fromId not in confirm:
                    confirm[fromId] = len(matchSetList)
                    matchSetList.append(set([fromId]))
                if toId not in confirm:
                    confirm[toId] = len(matchSetList)
                    matchSetList.append(set([toId]))
    return [x for x in matchSetList if type(x) == set]

def match(fromId, srcModel, graphFocus=False):
    table = srcModel._meta.db_table
    nodeInfo = {}
    result = []
    graph, nodeSet = traverse(fromId)

    for _fromId in nodeSet:
        try:
            base = BaseProject.select().where(BaseProject.fromId == _fromId).get()
        except:
            base = None
        if base == None:
            continue
        nodeInfo[_fromId] = base
            
    matchSetList = calc_match(graph, nodeInfo)
    if len(matchSetList) == 0:
        matchSetList.append(set([fromId]))
    matchIdSet = set()
    for matchSet in matchSetList:
        matchId = 0
        matchIdList = [nodeInfo[x].matchId for x in matchSet if x in nodeInfo and nodeInfo[x].matchId > 0]
        if len(matchIdList) > 0:
            matchId = min(matchIdList)
        if matchId > 0 and table == 'base_project':
            try:
                BaseProject.select().where((BaseProject.matchId==matchId)&(BaseProject.fromId>0)&(~(BaseProject.fromId << list(matchSet)))).get()
                matchId = 0
            except Exception, e:
                pass
        if (matchId in matchIdSet or matchId == 0) and table == 'base_project':
            try:
                row = BaseProject.select(fn.Max(BaseProject.matchId).alias('nextMatchId')).get()
                matchId = row.nextMatchId + 1
            except:
                matchId = 1
        matchIdSet.add(matchId)#prevent same matchId in this matchSet list
        srcModel.update(matchId=matchId, mark=srcModel.mark.bin_and(~4)).where((srcModel.fromId << list(matchSet)) & (srcModel.mark.bin_and(0x10000)==0)).execute()
        
        if fromId not in matchSet:
            continue
        if graphFocus == True:
            make_graph(graph, matchSet, nodeInfo)
            return list(matchSet)
        for _fromId in matchSet:
            if _fromId in nodeInfo:
                result.append(nodeInfo[_fromId])
    return result

def make_graph(graph, matchSet, nodeInfo):
    pg = pgv.AGraph(directed=True, strict=False)
    for toId, fromDict in graph.iteritems():
        for fromId, edgeList in fromDict.iteritems():
            if fromId not in matchSet and toId not in matchSet:
                continue
            u = str(fromId)
            if fromId in matchSet:
                pg.add_node(u, color='red')
            else:
                pg.add_node(u)
                
            v = str(toId)
            if toId in matchSet:
                pg.add_node(v, color='red')
            else:
                pg.add_node(v)
            if confirm_edge(fromId, toId, graph, nodeInfo) == True:
                pg.add_edge(u, v, color='red')
            else:
                pg.add_edge(u, v)
            pg.draw('match_graph.png', prog='dot')
            
def reset_match():
    return BaseProject.update(mark=BaseProject.mark|4).where((BaseProject.fromId>0)&(BaseProject.mark&0x10000)).execute()

if __name__ == '__main__':
    if len(sys.argv) == 1:
        reset_match()
    elif len(sys.argv) == 2:
        matchSetList = match(int(sys.argv[1]))
        for matchSet in matchSetList:
            print ', '.join([str(x) for x in matchSet])
    elif len(sys.argv) == 3:
        matchSetList = match(int(sys.argv[1]), graphFocus=True)
        for matchSet in matchSetList:
            print ', '.join([str(x) for x in matchSet])
