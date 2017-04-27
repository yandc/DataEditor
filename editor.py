#!/user/bin/env python
# coding=utf-8
from constant import *
from playhouse.csv_loader import *
from tools import *

class Editor:
    srcModel = None
    batchSize = 100
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.count = 0
        self.pid = 0
        self.loadCheckPoint()

    def loadData(self):
        srcModel = self.srcModel
        return srcModel.select().where(srcModel.id>self.checkPoint).order_by(srcModel.id).limit(100)

    def edit(self, row):
        return 0

    def progress(self):
        self.count += 1
        if self.count % self.batchSize == 0:
            logging.info('Processed: %s'%self.count)

    def setCheckPoint(self, row):
        self.checkPoint = row.id

    def loadCheckPoint(self):
        key = 'Patcher:%s:Checkpoint'%self.__class__.__name__
        try:
            mdl = Offset.select().where(Offset.name==key).get()
            self.checkPoint = mdl.offset
        except:
            self.checkPoint = 0

    def saveCheckPoint(self):
        key = 'Patcher:%s:Checkpoint'%self.__class__.__name__
        pushInto(Offset, {'name':key, 'offset':self.checkPoint}, ['name'])

    def finish(self):
        pass
                        
    def run(self):
        count = 0
        clsName = self.__class__.__name__
        while True:
            rows = self.loadData()
            if len(rows) == 0:
                break
            for row in rows:
                count += self.edit(row)
                self.setCheckPoint(row)
                self.progress()
        self.finish()
        self.saveCheckPoint()
        logging.info('%s Done %s/%s'%(clsName, count, self.count))

class Leads2Base(Editor):
    srcModel = AdminLeads
    batchId = 2
    infoMap = {'cityId':'upCityId', 'cityName':'upCityName', 'abstract':'upAbstract',
               'stage':'upStage', 'wxPub':'upWxPub', 'upCatgyId':'upCatgyId',
               'upCatgyParentName':'upCatgyParentName', 'upCatgyName':'upCatgyName',
               'provinceId':'upProvinceId', 'provinceName':'upProvinceName', 'homepage':'upHomePage'}

    def getKeyPoint(self, leads):
        return leads.finishTime
    def loadData(self):
        srcModel = self.srcModel
        yday = datetime.date.today() - datetime.timedelta(days=1)
        return srcModel.select().where((srcModel.id>self.pid) & (srcModel.finishTime>=str(yday))).order_by(srcModel.id).limit(100)
    
    def edit(self, leads):
        if leads.status != 250 and leads.status != 500 and leads.status != 600:
            return 0
        if leads.claimUserId == 0 and leads.status == 600:
            return 0
        if leads.source in set(['manual', 'platform', 'union']):
            return 0
        
        fromId = encodeSrc(LEADS_TYPE, leads.id)
        try:
            base = BaseInfo.select().where(BaseInfo.fromId == fromId).get()
        except Exception, e:
            base = None
        if base == None:
            base = BaseInfo()
            
        try:
            info = json.loads(leads.data)
        except Exception, e:
            logging.info(str(e))
        base.fromId = fromId
        base.projectName = leads.title
        idx = leads.abstract.find('公司全称')
        if idx > 0:
            base.companyName = leads.abstract[idx+5:].split('\n')[0]
        base.ceoName = leads.founderName
        base.ceoPhone = leads.founderPhone
        base.leadsId = leads.id
        base.fromUrl = leads.website
        base.fromSrc = leads.source
        base.batchId = self.batchId
        base.stage = ''
        try:
            base.score = float(leads.score)*100
        except:
            pass
        try:
            catgy = AdminCatgy.select().where(AdminCatgy.id == leads.categoryId).get()
            base.catgyId = catgy.parent
        except:
            base.catgyId = 0
        
        #info fields
        for k, v in self.infoMap.iteritems():
            try:
                if k.find('Id') > 0:
                    try:
                        testId = int(info[v])
                    except:
                        info[v] = '0'
                setattr(base, k, info[v])
            except:
                pass
        try:
            if len(base.fromUrl) > 0:
                row = BaseProject.select().where((BaseProject.fromId<encodeSrc(LEADS_TYPE, 0)) & (BaseProject.fromUrl==base.fromUrl)).get()
                base.refStage = row.stage
        except:
            base.refStage = base.stage

        idx = leads.abstract.find('网站】')
        if idx > 0:
            base.homepage = leads.abstract[idx+3:].split('\n')[0]
        return base.save()
        
class Investment2Admin(Editor):
    srcModel = Investment
    mapping = {'itjuzi':1, 'innotree':2, 'chinaventure':3, 'cyzone':4, 'pedaily':5}
    def edit(self, event):
        if event.stage not in care_stage:
            return 0
        if event.fromSrc not in self.mapping:
            return 0
        info = {}
        info['source'] = self.mapping[event.fromSrc]
        info['name'] = event.projectName
        info['field'] = event.label
        info['stage'] = event.stage
        info['scale'] = event.scale
        info['time'] = event.investTime
        info['investor'] = ''
        info['currency'] = 0

        funds = event.fundNames.split(',')
        rows = AdminIvent.select().where((AdminIvent.name==info['name']) & (AdminIvent.stage==event.stage))
        if len(rows) > len(funds):
            return 0
        count = 0
        for fund in funds:
            try:
                row = FundMap.select().where(FundMap.name==fund).order_by(FundMap.id.desc()).get()
                info['fundId'] = row.fundId
            except Exception, e:
                info['fundId'] = 0
            if len(rows) > 0 and count < len(rows):
                if info['source'] < rows[0].source:
                    info['id'] = rows[count].id
                else:
                    break
            info['fundName'] = ''.join(fund.split())
            url = 'https://admin.ethercap.com/in/investment/addEvent'
            SignUtils.signed_request(url, 'ir', '6224d12cbebc9ddea8135ce5f4f7d3ed', data=info, method='post')
            count += 1
        return count
        
class Admin2FundMap(Editor):
    srcModel = AdminFund
    def getPid(self, fund):
        return fund.fundId
    def getKeyPoint(self, fund):
        return fund.creationTime
    def loadData(self):
        srcModel = self.srcModel
        return srcModel.select().where((srcModel.fundId>self.pid) & (srcModel.creationTime>=self.savePoint)).limit(100)
    
    def edit(self, fund):
        ret = 0
        where = ['name']
        info = {'fundId':fund.fundId}
        for name in [fund.name, fund.englishName, fund.nameShort, fund.englishNameShort]:
            info['name'] = ''.join(name.split())
            if len(info['name']) > 0:
                ret += pushInto(FundMap, info, where)
        return ret

class FundIdFix(Editor):
    srcModel = AdminIvent
    def edit(self, ivent):
        info = {}
        info['source'] = ivent.source
        info['name'] = ivent.name
        info['stage'] = ivent.stage
        info['field'] = ivent.field
        info['scale'] = ivent.scale
        info['time'] = ivent.time
        info['investor'] = ivent.investor
        info['currency'] = ivent.currency
        info['fundName'] = ivent.fundName
        info['id'] = ivent.id
        fundId = ivent.fundId
        try:
            row = FundMap.select().where(FundMap.name==ivent.fundName).order_by(FundMap.id.desc()).get()
            fundId = row.fundId
        except:
            fundId = 0
        if fundId != ivent.fundId:
            info['fundId'] = fundId
            url = 'https://admin.ethercap.com/in/investment/addEvent'
            SignUtils.signed_request(url, 'ir', '6224d12cbebc9ddea8135ce5f4f7d3ed', data=info, method='post')
            return 1
        return 0

class Leads2Admin(Editor):
    srcModel = Leads
    def edit(self, leads):
        if leads.source == 'qimingpian':
            return 0
        ab = json.loads(leads.abstract)
        if 'stage' not in ab:
            stage = '未知'
        else:
            stage = get_format_stage(obj2string(ab['stage']))
        if stage.decode() not in care_stage:
            return 0

        if len(leads.city) > 0:
            isFind = False
            for ct in cities:
                if leads.city.find(ct) >= 0:
                    isFind = True
                    break
            if isFind == False:
                return 0

        info = {}
        info['source'] = leads.source
        if leads.source == '36kr':
            info['source'] = '36kr_rong'
        info['title'] = leads.title
        info['label'] = leads.label
        info['city'] = leads.city
        info['website'] = leads.website
        abstr = ''.decode()
        for k, v in ab.iteritems():
            if len(v) == 0:
                continue
            vstr = obj2string(v)
            if len(vstr) == 0:
                continue
            if k in abstract_info:
                abstr += abstract_info[k] + vstr + '\n'
        info['abstract'] = abstr
        url = 'https://admin.ethercap.com/in/leads/addLeads'
        SignUtils.signed_request(url,'ir','6224d12cbebc9ddea8135ce5f4f7d3ed', data=info, method='post')
        return 1

class Leads2Project(Editor):
    srcModel = Leads
    @staticmethod
    def itjuziData(url, ele):
        matchId, fromId = getIdByUrl(url)
        if fromId == 0:
            return
        for key, value in ele.iteritems():
            try:
                ele[key] = int(obj2string(value))
            except Exception, e:
                return
        ele['fromId'] = fromId
        ele['matchId'] = matchId
        pushInto(JuziData, ele)
    @staticmethod
    def itjuziNews(url, eles):
        matchId, fromId = getIdByUrl(url)
        if fromId == 0:
            return
        for ele in eles:
            for key, value in ele.iteritems():
                ele[key] = obj2string(value)
            ele['fromId'] = fromId
            ele['matchId'] = matchId
            ele['quality'] = 1
            pushInto(News, ele, ['link'], lambda x,y,z:False)
    def edit(self, leads):
        ab = json.loads(leads.abstract)
        for key in required_field:
            if key not in ab:
                ab[key] = required_field[key]
        info = {}
        info['projectName'] = ''.join(leads.title.split())
        info['location'] = leads.city
        label = ''.join(leads.label.split())
        info['label'] = label.replace('/', ',').replace('，', ',').replace('·', ',')
        info['des'] = obj2string(ab['description'])
        info['brief'] = obj2string(ab['brief'])
        info['teamInfo'] = obj2string(ab['founder'])
        info['fromId'] = encodeSrc(ETHSPY_TYPE, leads.id)
        info['companyName'] = obj2string(ab['fullname']).replace('公司全称：', '')
        info['fromSrc'] = leads.source
        info['fromUrl'] = leads.website
        stage = '未知'
        if 'stage' in ab:
            stage = obj2string(ab['stage'])
        info['stage'] = get_format_stage(stage)
        if leads.source in quality:
            info['quality'] = quality[leads.source]
        else:
            info['quality'] = 2
        info['creationTime'] = leads.creationTime
        info['updateTime'] = leads.updateTime
        host = obj2string(ab['homepage']).lower()
        if len(host) > 0:
            idx = host.find('http:')
            if idx > 0:
                host = host[idx:]
            idx = host.find('https:')
            if idx > 0:
                host = host[idx:]
            host = host.replace('https:', '').replace('http:', '')
            i = 0
            for c in host:
                if (c <= 'z' and c >= 'a') or (c <= '9' and c >= '0'):
                    break
                i += 1
            host = host[i:]
        info['host'] = host
        info['quality'] += calc_quality(info, 'project') * 10
        pushInto(BaseProject, info, where=['fromId'])
        if info['fromSrc'] == 'itjuzi':
            if 'data' in ab:
                Leads2Project.itjuziData(info['fromUrl'], ab['data'])
            if 'news' in ab:
                Leads2Project.itjuziNews(info['fromUrl'], ab['news'])
        return 1

class Ivent2Project(Editor):
    srcModel = Ivent
    def edit(self, ivent):
        info = {}
        info['projectName'] = ''.join(ivent.project.split())
        if ivent.company != None:
            info['companyName'] = ''.join(ivent.company.split())
        if ivent.label != None:
            label = ''.join(ivent.label.split())
            info['label'] = label.replace('/', ',').replace('，', ',').replace('·', ',')
        info['stage'] = get_format_stage(ivent.stage)
        info['scale'] = ivent.amount
        info['fundNames'] = ivent.investor.replace('/', ',').replace('，', ',').replace('、', ',').replace('领投', '').replace('跟投', '')
        info['investTime'] = ivent.date.replace('-', '.').replace('.0', '.')
        info['fromId'] = encodeSrc(ETHSPY_TYPE, ivent.id)
        info['fromSrc'] = ivent.source
        info['creationTime'] = ivent.creationTime
        if ivent.source in quality:
            info['quality'] = quality[ivent.source]
        else:
            info['quality'] = 2
        info['quality'] += calc_quality(info, 'investment') * 10
        pushInto(BaseIvent, info, ['fromId'], lambda x,y,z:False)
        try:
            row = Investment.select().where((Investment.projectName==info['projectName']) & (Investment.stage==info['stage'])).get()
            if row.quality < info['quality']:
                info['id'] = row.id
                pushInto(Investment, info)
        except:
            pushInto(Investment, info)
        return 1
    
class AdminLeads2Project(Editor):
    srcModel = AdminLeads
    def getKeyPoint(self, leads):
        return leads.finishTime
    def loadData(self):
        srcModel = self.srcModel
        yday = datetime.date.today() - datetime.timedelta(days=1)
        return srcModel.select().where((srcModel.id>self.pid) & ((srcModel.finishTime>=str(yday)) | (srcModel.creationTime>=str(yday)))).order_by(srcModel.id).limit(100)
    def edit(self, leads):
        info = {}
        fn = lambda x,y:x[x.find(y)+len(unicode(y))+1:].split('\n')[0] if x.find(y)>0 else ''
        info['projectName'] = ''.join(leads.title.split())
        info['location'] = leads.city
        info['companyName'] = fn(leads.abstract, '公司全称')
        info['stage'] = fn(leads.abstract, '融资阶段')
        info['host'] = fn(leads.abstract, '项目网站')
        info['des'] = fn(leads.abstract, '项目描述')
        info['teamInfo'] = fn(leads.abstract, '创始人')
        info['brief'] = fn(leads.abstract, '简介')
        try:
            catgy = Category.select().where(Category.id == leads.categoryId).get()
            info['label'] = catgy.name
        except:
            info['label'] = ''
        if len(info['label']) == 0:
            label = ''.join(leads.label.split())
            info['label'] = label.replace('/', ',').replace('，', ',').replace('·', ',')
        try:
            data = json.loads(leads.data)
            if 'abstract' in data and len(data['abstract']) > 0:
                info['des'] = data['abstract']
        except:
            pass
        info['fromId'] = encodeSrc(LEADS_TYPE, leads.id)
        info['fromSrc'] = leads.source
        info['fromUrl'] = leads.website
        if leads.source in quality:
            info['quality'] = quality[leads.source]
        else:
            info['quality'] = 2
        info['creationTime'] = leads.creationTime
        info['updateTime'] = leads.updateTime
        if info['updateTime'] == None:
            info['updateTime'] = info['creationTime']
        info['quality'] += calc_quality(info, 'project') * 10
        try:
            info['leadsScore'] = float(leads.score)*100
        except:
            pass
        pushInto(BaseProject, info, ['fromId'])
        return 1
    
class AdminProj2Project(Editor):
    srcModel = AdminProj
    def loadData(self):
        srcModel = self.srcModel
        return srcModel.select().where((srcModel.projectId>self.pid) & (srcModel.updateTime>=self.savePoint)).order_by(srcModel.projectId).limit(100)
    def getPid(self, row):
        return row.projectId
    def edit(self, proj):
        info = {}
        info['projectName'] = ''.join(proj.title.split())
        if len(proj.field) > 1:
            info['label'] = proj.field
        info['brief'] = proj.abstract
        info['des'] = proj.companyInfo
        info['fromId'] = encodeSrc(PROJECT_TYPE, proj.projectId)
        info['fromSrc'] = proj.source
        info['quality'] = 9
        info['companyName'] = proj.bizName
        host = ''
        if len(proj.links) > 0:
            links = json.loads(proj.links)
            for link in links:
                if link['name'].find('公司网站链接') == 0:
                    host = link['url']
                    break
        info['host'] = host
        info['creationTime'] = proj.creationTime
        info['updateTime'] = proj.updateTime
        info['quality'] += calc_quality(info, 'project') * 10
        pushInto(BaseProject, info, ['fromId'])
        return 1

class GraphEditor(Editor):
    def loadData(self):
        srcModel = self.srcModel
        return srcModel.select().where((srcModel.id>self.pid)&((srcModel.updateTime>self.savePoint)|(srcModel.mark.bin_and(1)>0))).order_by(srcModel.id).limit(100)
class Project2Graph(GraphEditor):
    srcModel = BaseProject
    def edit(self, proj):
        i = 0
        for c in proj.host.lower():
            if (c <= 'z' and c >= 'a') or (c <= '9' and c >= '0') or c == '.':
                i += 1
            else:
                break
        if len(proj.host[i:]) > 1:
            proj.host = ''
        else:
            proj.host = proj.host[:i]
        graph(proj, BaseProject)
        return 1
    
class Investment2Graph(GraphEditor):
    srcModel = BaseIvent
    def edit(self, ivent):
        graph(ivent, BaseIvent)
        return 1

class MatchEditor(Editor):
    def loadData(self):
        srcModel = self.srcModel
        return srcModel.select().where((srcModel.id>self.pid)&((srcModel.matchId==0)|(srcModel.mark.bin_and(4)>0))).order_by(srcModel.id).limit(100)
class Project2Match(MatchEditor):
    srcModel = BaseProject
    def edit(self, proj):
        if proj.fromId == 0:
            return 0
        if proj.mark&2 > 0:#same website, same matchId
            try:
                row = BaseProject.select().where((BaseProject.fromUrl==proj.fromUrl) & (BaseProject.matchId>0)).get()
                BaseProject.update(matchId=row.matchId, mark=BaseProject.mark&~4).where(BaseProject.id==proj.id)
            except:
                logging.error('Same website no matchId: %s'%(proj.fromUrl))
            return 1
        i = 0
        for c in proj.host.lower():
            if (c <= 'z' and c >= 'a') or (c <= '9' and c >= '0') or c == '.':
                i += 1
            else:
                break
        if len(proj.host[i:]) > 1:
            proj.host = ''
        else:
            proj.host = proj.host[:i]
        match(proj.fromId, BaseProject)
        return 1
    
class Investment2Match(MatchEditor):
    srcModel = BaseIvent
    def edit(self, ivent):
        if ivent.fromId == 0:
            return 0
        match(ivent.fromId, BaseIvent)
        return 1
    
class Lagou2File(Editor):
    srcModel = BaseProject
    def loadData(self):
        today = time.strftime('%Y-%m-%d')
        if today != self.checkPoint[:10]:
            srcModel = self.srcModel
            today = datetime.date.today()
            yday = today - datetime.timedelta(days=1)
            q = srcModel.select().where((srcModel.fromId<100000000) & (srcModel.fromSrc=='lagou') & (srcModel.creationTime<str(today)) & (srcModel.creationTime>str(yday)))
            fp = open('lagou.csv', 'w')
            dump_csv(q, fp)
        self.checkPoint = time.strftime("%Y-%m-%d %H:%M:%S")
        return []

class Jobui2File(Editor):
    srcModel = BaseProject
    def loadData(self):
        today = time.strftime('%Y-%m-%d')
        if today != self.checkPoint[:10]:
            srcModel = self.srcModel
            today = datetime.date.today()
            yday = today - datetime.timedelta(days=1)
            q = srcModel.select().where((srcModel.fromId<100000000) & (srcModel.fromSrc=='jobui') & (srcModel.creationTime<str(today)) & (srcModel.creationTime>str(yday)))
            fp = open('jobui.csv', 'w')
            dump_csv(q, fp)
        self.checkPoint = time.strftime("%Y-%m-%d %H:%M:%S")
        return []

class Leads2File(Editor):
    srcModel = AdminLeads
    def loadData(self):
        today = time.strftime('%Y-%m-%d')
        if today != self.checkPoint[:10]:
            srcModel = self.srcModel
            today = datetime.date.today()
            yday = today - datetime.timedelta(days=1)
            q = srcModel.select().where((srcModel.score>='3') & (srcModel.source<<['36kr_rong', 'itjuzi', 'media', 'protfolio', 'www']) & (srcModel.finishTime<str(today)) & (srcModel.finishTime>str(yday)))
            fromIdList = [x.id+LEADS_TYPE*FROM_ID_BASE for x in q]
            q = BaseProject.select().where(BaseProject.fromId << fromIdList)
            fp = open('leads.csv', 'w')
            dump_csv(q, fp)
        self.checkPoint = time.strftime("%Y-%m-%d %H:%M:%S")
        return []

class File2Email(Editor):
    def loadData(self):
        today = time.strftime('%Y-%m-%d')
        mailAddrs = ['yandechen@ethercap.com', 'lindanning@ethercap.com', 'yexiaoyu@ethercap.com']
        fileList = ['lagou.csv', 'jobui.csv', 'leads.csv']
        title = '昨日新增【%s】'%(today)
        if 'files' in self.kwargs:
            fileList = self.kwargs['files'].split(',')
        if 'title' in self.kwargs:
            title = self.kwargs['title']
        if 'addrs' in self.kwargs:
            mailAddrs = self.kwargs['addrs'].split(',')
        if 'files' in self.kwargs or today != self.checkPoint[:10]:
            EmailUtil.sendEmail(mailAddrs, title, '', fileList)
        self.checkPoint = time.strftime("%Y-%m-%d %H:%M:%S")
        return []

class Report2Email(Editor):
    models = [Leads, Investor, Ivent, Fund]
    mailAddrs = ['yandechen@ethercap.com',
                 'yangchao@ethercap.com',
                 'yexiaoyu@ethercap.com',
                 'duhe@ethercap.com',
                 'wangyukun@ethercap.com']
    def loadData(self):
        today = time.strftime('%Y-%m-%d')
        if today != self.checkPoint[:10]:
            today = datetime.date.today()            
            yday = today - datetime.timedelta(days=1)
            logging.info('Send crawler count mail.')
            html = ''
            for mdl in self.models:
                table = mdl._meta.db_table
                html += '<h2>' + table + '</h2>'
                rows = mdl.select(mdl.source, fn.COUNT(mdl.id).alias('count')).where((mdl.creationTime>yday)&(mdl.creationTime<today)).group_by(mdl.source)
                new = {}
                for row in rows:
                    new[row.source] = str(row.count)
                if table == 'project_leads':
                    rows = AdminLeads.select(AdminLeads.source, fn.COUNT(AdminLeads.id).alias('count')).where((AdminLeads.creationTime>yday)&(AdminLeads.creationTime<today)).group_by(AdminLeads.source)
                    for row in rows:
                        if row.source in new:
                            new[row.source] += ' \ %s'%row.count
                html += '<p>'.join([key+':&#9;&#9;'+new[key] for key in new])
            EmailUtil.sendEmail(self.mailAddrs, '每日抓取统计【%s】'%(yday), html)
        self.checkPoint = time.strftime("%Y-%m-%d %H:%M:%S")
        return []

class Search2File(Editor):
    def loadData(self):
        fromIdList = []
        queries = self.kwargs['q'].split(',')
        for q in queries:
            url = search_url%(SEARCH_PAGESIZE)+urllib.urlencode({'query':q})+'&ttlsearch=4'
            fromIdList += getFromSearch(url)
        q = BaseProject.select().where(BaseProject.fromId << fromIdList)
        fp = open('search.csv', 'w')
        dump_csv(q, fp)
        return []
        
class Base2Base(Editor):
    srcModel = BaseInfo
    def edit(self, base):
        if len(base.fromUrl) == 0:
            return 0
        try:
            row = BaseProject.select().where((BaseProject.fromId<encodeSrc(LEADS_TYPE, 0)) & (BaseProject.fromUrl==base.fromUrl)).get()
            base.refStage = row.stage
            base.save()
            return 1
        except:
            return 0

class QimpIvent2Admin(Editor):
    srcModel = OrgDetail
    def getKeyPoint(self, row):
        return row.creationTime
    def loadData(self):
        srcModel = self.srcModel
        return srcModel.select().where(srcModel.creationTime>'2017-02-01')
    def edit(self, ivent):
        if len(ivent.stage) == 0 or ivent.stage == '待披露':
            ivent.stage = '未知'
        if ivent.stage.decode() not in care_stage:
            return 0
        info = {'source':'protfolio'}
        info['title'] = ivent.projectName
        info['city'] = ivent.city
        info['label'] = ivent.label
        info['website'] = 'qimingpian:%s:%s:%s'%(ivent.orgName, ivent.seqno, ivent.projectName)
        abstr = ''
        abstr += u'【基金名称】'+ivent.orgName+'\n'
        abstr += u'【融资阶段】'+ivent.currentStage+'\n'
        abstr += u'【融资时间】'+ivent.date+'\n'
        abstr += u'【简介】'+ivent.brief+'\n'
        info['abstract'] = abstr
        url = 'https://admin.ethercap.com/in/leads/addLeads'
        SignUtils.signed_request(url,'ir','6224d12cbebc9ddea8135ce5f4f7d3ed', data=info, method='post')
        return 1

class SpiderMonitorLog2Email(Editor):
    def loadData(self):
        today = time.strftime('%Y-%m-%d')
        yday = datetime.date.today() - datetime.timedelta(days=1)
        mailAddrs = ['yandechen@ethercap.com', 'yangchao@ethercap.com']
        title = '抓取监控【%s】'%(today)
        if today != self.checkPoint[:10]:
            srcModel = MonitorLog
            rows = srcModel.select(srcModel.spider, srcModel.source, srcModel.taskType, srcModel.errorCode, fn.COUNT(srcModel.id).alias('gcount'), srcModel.status).where((srcModel.creationTime>yday)&(srcModel.creationTime<today)).group_by(srcModel.spider, srcModel.source, srcModel.taskType, srcModel.errorCode).tuples()
            from jinja2 import Template
            temp = Template(open('table_template.html').read())
            html = temp.render(heads=('spider','source','taskType','errorCode','gcount','status'), rows=rows)
            EmailUtil.sendEmail(mailAddrs, title, html)
        self.checkPoint = time.strftime("%Y-%m-%d %H:%M:%S")
        return []

class Link2Map(Editor):
    srcModel = Article
    def loadCheckPoint(self):
        self.checkPoint = 0
        
    def edit(self, model):
        ret = 0
        pics = json.loads(model.pics)
        if model.srvPics:
            srvPics = json.loads(model.srvPics)
            if len(pics) != len(srvPics):
                return ret
        else:
            return ret

        count = 0
        for pic in pics:
            try:
                mdl = LinkMap.select().where(LinkMap.fromLink==pic).get()
                mdl.srvLink = srvPics[count]
                mdl.save()
                ret += 1
            except:
                pdb.set_trace()
                logging.error('No exist: %s'%pic)
            count += 1

        return ret
        
class Image2Server(Editor):
    srcModel = Article
    def loadCheckPoint(self):
        self.checkPoint = 0
    def loadData(self):
        srcModel = self.srcModel
        return srcModel.select().where((srcModel.status==DOWNLOAD_STATUS) & (srcModel.id>self.checkPoint)).order_by(srcModel.id).limit(self.batchSize)
    
    def edit(self, model):
        ret = 0
        try:
            pics = json.loads(model.pics)
        except Exception, e:
            logging.error(str(e))
            return 0
            
        srvLinks = []
        count = {'succ':0, 'fail':0, 'broken':0, 'repeat':0, 'empty':0}
        validCount = 0
        for pic in pics:
            if pic.find('http') != 0:
                continue
            validCount += 1
            status, msg = uploadImage('article', pic)
            count[status] += 1
            if status == 'fail':
                validCount -= 1
                continue
            if status == 'broken' or status == 'empty':
                continue
            srvLinks.append(msg)

        logging.info('%s/%s in %s.'%(str(count), len(pics), model.id))
        model.srvPics = json.dumps(srvLinks)
        if len(srvLinks) == validCount:
            model.status = UPLOADED_STATUS
        elif count['empty'] > 0:
            model.status = INIT_STATUS
        model.save()
        return 1

class PostImport(Editor):
    srcModel = Article
    def loadCheckPoint(self):
        self.checkPoint = 0
        
    def loadData(self):
        srcModel = self.srcModel
        return srcModel.select().where(((srcModel.status==UPLOADED_STATUS) | (srcModel.status==BROKEN_STATUS)) & (srcModel.id>self.checkPoint)).order_by(srcModel.id).limit(self.batchSize)

    def edit(self, model):
        try:
            text = '\n'.join(json.loads(model.text))
        except Exception, e:
            text = model.text
        try:
            title = json.loads(model.title)
        except:
            title = model.title

        info = {
            'class':'Robot',
            'action':'importSubjectMaterial',
            'params':{
                'import_data':{
                    'id':str(model.id),
                    'title':title,
                    'text':text,
                    'srvPics':model.srvPics,
                    'catgy':model.catgy,
                    'keyword':model.keyword,
                    'brand':model.brand,
                    'source':model.source
                }
            }
        }
        try:
            resp = requests.post('http://groupservice.miyabaobei.com', json=info)
            result = json.loads(resp.content)
            if result['code'] != 0:
                pdb.set_trace()
                logging.error(result['msg'])
            else:
                model.status = COMPLETE_STATUS
                model.save()
                return 1
        except Exception, e:
            logging.error('Import error %s: %s'%(model.id, str(e)))
            model.status = BROKEN_STATUS
            model.save()
        return 0
    
class AvatarImport(Editor):
    srcModel = Avatar
    batchSize = 10
    def loadCheckPoint(self):
        self.checkPoint = 0
    def edit(self, model):
        idx = model.pic.rfind('.')
        suffix = ''
        if idx > 0:
            suffix = model.pic[idx+1:]
        path = '/opt/data/avatar/%s.%s'%(model.id, suffix)
        if not os.path.exists(path):
            return 0
        
        status, msg = uploadImage('avatar', model.pic, path)
        try:
            if status == True:
                infoPic = {
                    'class':'Robot',
                    'action':'importAvatarMaterial',
                    'params':{
                        'import_data':{
                            'id':model.id,
                            'link':msg,
                            'category':model.catgy
                        }
                    }
                }
                resp = requests.post('http://groupservice.miyabaobei.com', json=infoPic)
                result = json.loads(resp.content)
                if result['code'] != 0:
                    logging.error(result['msg'])
                    return 0
            if model.name:
                infoName = {
                    'class':'Robot',
                    'action':'importNicknameMaterial',
                    'params':{
                        'import_data':{
                            'nickname':model.name,
                            'category':model.catgy
                        }
                    }
                }
                resp = requests.post('http://groupservice.miyabaobei.com', json=infoName)
                result = json.loads(resp.content)
                if result['code'] != 0:
                    logging.error(result['msg'])
                    return 0
        except Exception, e:
            pdb.set_trace()
            logging.error(str(e))
            return 0
        return 1
        
class PicsFilter(Editor):
    srcModel = Article
    def loadCheckPoint(self):
        self.checkPoint = 0
        
    def loadData(self):
        srcModel = self.srcModel
        return srcModel.select().where((srcModel.status==INIT_STATUS) & (srcModel.id>self.checkPoint)).order_by(srcModel.id).limit(self.batchSize)

    def edit(self, model):
        try:
            if model.absentPics:
                pics = json.loads(model.absentPics)
            else:
                pics = json.loads(model.pics)
        except Exception, e:
            logging.error(str(e))
            return 0

        result = []
        for pic in pics:
            if pic.find('http') != 0:
                continue
            try:
                mdl = LinkMap.select().where(LinkMap.fromLink==pic).get()
                continue
            except:
                result.append(pic)

        if len(result) != len(pics):
            model.absentPics = json.dumps(result)
            model.save()
            return 1
        return 0

class MapRepair(Editor):
    srcModel = Article
    def loadCheckPoint(self):
        self.checkPoint = 0
        
    def loadData(self):
        srcModel = self.srcModel
        return srcModel.select().where((srcModel.source%'weibo:%') &(srcModel.id>self.checkPoint)).order_by(srcModel.id).limit(self.batchSize)

    def edit(self, model):
        try:
            pics = json.loads(model.pics)
            imgs = []
            for pic in pics:
                idx1 = pic.rfind('/')
                idx2 = pic[:idx1].rfind('/')
                pic = pic[:idx2]+'/mw690'+pic[idx1:]
                imgs.append(pic)
            if len(imgs) > 0:
                model.pics = json.dumps(imgs)
                model.status = 'INIT'
                model.save()
                mdls = LinkMap.select().where(LinkMap.srcId==model.id)
                for mdl in mdls:
                    if mdl.toLink and os.path.exists(mdl.toLink):
                        os.remove(mdl.toLink)
                    mdl.status = 'Discard'
                    mdl.toLink = ''
                    mdl.save()
                return 1

#            sect = model.source.split(':')[0]
#            fakeUrl = '%s//%s'%(sect, model.srcId)
#            try:
#                mdl = SpyLog.select().where(SpyLog.link==fakeUrl).get()
#            except:
#                mdl = SpyLog.select().where(SpyLog.link==model.srcId).get()
#            ele = json.loads(mdl.eles)
#            model.text = json.dumps(ele['text'])
        except Exception, e:
            logging.error('%s, %s'%(str(e), model.id))
            return 0
        
#        try:
#            pics = json.loads(model.pics)
#        except Exception, e:
#            logging.error(str(e))
#            return 0
#
#        result = []
#        for pic in pics:
#            if pic.find('http') != 0:
#                continue
#            try:
#                mdl = LinkMap.select().where(LinkMap.fromLink==pic).get()
#                if not mdl.srcId:
#                    mdl.source = model.source
#                    mdl.srcId = model.id
#                    mdl.save()
#                if mdl.status == 'Fail' and str(mdl.creationTime) > '2017-04-10':
#                    model.status = 'INIT'
#                    model.save()
#            except:
#                pass
        return 0

import cv2
class AvatarFilter(Editor):
    srcModel = LinkMap
    cascade = cv2.CascadeClassifier('/usr/share/opencv/haarcascades/haarcascade_frontalface_alt.xml')
    fp = open('faceId.txt', 'w')
    def __del__(self):
        self.fp.close()
    def loadData(self):
        srcModel = self.srcModel
        return srcModel.select().where((srcModel.id>self.checkPoint) & (srcModel.status=='Succ') & (srcModel.source=='douban')).limit(self.batchSize)

    def edit(self, model):
        path = model.toLink
        if not path or not os.path.exists(path):
            return 0
        try:
            img = cv2.imread(path)
            gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
            faces = self.cascade.detectMultiScale(gray, scaleFactor=1.1, minNeighbors=2)
            if len(faces) > 0:
                self.fp.write(str(model.srcId)+'\n')
                return 1
        except Exception, e:
            logging.error('%s %s'%(str(e), path))
        return 0

class SimPost(Editor):
    filename = 'data/postdata'
    index = {}
    indexInc = {}
    postInfo = {}
    postInfoInc = {}
    months = []
    thresh = 0.2
    part = 0
    partFlag = False
    maxPostNum = 200000
    minWords = 10
    def __init__(self):
        Editor.__init__(self)
        self.fp = open(self.filename)

    def setCheckPoint(self, post):
        return int(post[0])

    def loadData(self):
        res = []
        count = 0
        for line in self.fp:
            if count > self.batchSize:
                break
            post = line.split('\t')
            while len(post) < 18:
                line += self.fp.next()
                post = line.split('\t')
            res.append(post)
            count += 1
        return res

    def edit(self, post):
        try:
            pid = int(post[0])
            title = post[11]
            content = post[12]
            imgNum = post[3]
            textLen = post[4]
            postPv = post[7]
            month = post[8][:7]
            sku = post[10]
        except:
            pdb.set_trace()
            return 0
        if sku == 'NULL':
            return 0
        if os.path.exists('data/sim-%s.txt'%month):
            return 0
        
        text = ''
        if title != 'NULL':
            text += title
        if content != 'NULL':
            text += content
        words = set()
        asi = 0
        count = 0
        text = text.lower().decode()
        for word in text:
            v = ord(word)
            if v >= 19904 and v <= 40895:
                if asi != 0:
                    words.add(asi)
                    asi = 0
                    count = 0
                words.add(v)
            elif (v >= ord('a') and v <= ord('z')) or (v >= ord('0') and v <= ord('9')):
                if count == 4:
                    continue
                asi <<= 8
                asi += v
                count += 1
            else:
                if asi != 0:
                    words.add(asi)
                    asi = 0
                    count = 0
        if asi != 0:
            words.add(asi)
        if len(words) < self.minWords:
            return 0

        #[wordCount, info, matchId, maxSim, simPid]
        pinfo = '%s,%s,%s'%(imgNum, postPv, textLen)
        self.postInfo[pid] = [len(words), pinfo, 0, 0, 0]
        self.postInfoInc[pid] = [len(words), pinfo, 0, 0, 0]
        for word in words:
            if word not in self.index:
                self.index[word] = []
            if word not in self.indexInc:
                self.indexInc[word] = []
            self.index[word].append(pid)
            self.indexInc[word].append(pid)

        if month not in self.months:#new month
            self.part = 0
            self.partFlag = False
            length = len(self.months)
            if length < 2:
                self.index = self.indexInc
                self.indexInc = {}
                self.postInfo = self.postInfoInc
                self.postInfoInc = {}
                self.months.append(month)
                return 1
        elif len(self.postInfo) > self.maxPostNum or len(self.postInfoInc) > self.maxPostNum/2:#too much post
            self.partFlag = True
        else:
            return 1

        #batch end, calc intersect
        logging.info('Calc intersect, post num: %s, index len:%s'%(len(self.postInfo), len(self.index)))
        intersect = {}
        for word, pidList in self.index.iteritems():
            if len(pidList) > 0.03*len(self.postInfo):
                continue
            for pid1 in pidList:
                if pid1 not in intersect:
                    intersect[pid1] = {}
                for pid2 in pidList:
                    if pid1 >= pid2:
                        continue
                    if pid2 not in intersect[pid1]:
                        intersect[pid1][pid2] = 0
                    intersect[pid1][pid2] += 1

        _list = []
        logging.info('Calc sim...')
        matchId = 0
        matchList = set()
        for pid1, inter in intersect.iteritems():
            for pid2, count in inter.iteritems():
                post1 = self.postInfo[pid1]
                post2 = self.postInfo[pid2]
                sim = float(count)/(post1[0]+post2[0]-count)
                if sim > self.thresh:
                    mid = max(post1[2], post2[2])
                    if mid == 0:
                        matchId += 1
                        mid = matchId
                    
                    post1[2] = mid
                    post2[2] = mid
                    if sim > post1[3]:
                        post1[3] = sim
                        post1[4] = pid2
                    if sim > post2[3]:
                        post2[3] = sim
                        post2[4] = pid1
                    matchList.add(pid1)
                    matchList.add(pid2)
        _li = sorted(matchList, key=lambda x:str(self.postInfo[x][2])+','+self.postInfo[x][1], reverse=True)

        if self.partFlag == True:
            name1 = 'data/sim-%s-part%s.txt'%(self.months[-1], self.part)
            name2 = 'data/dup-%s-part%s.txt'%(self.months[-1], self.part)
            self.part += 1
        else:
            name1 = 'data/sim-%s.txt'%self.months[-1]
            name2 = 'data/dup-%s.txt'%self.months[-1]

        posts = [[x]+self.postInfo[x] for x in _li]
        fp = open(name1, 'w')
        fp.write(json.dumps(posts))
        fp.close()
        
        mid = 0
        discard = []
        for post in posts:
            if post[3] == mid:
                discard.append(post[0])
            else:
                mid = post[3]
        fp = open(name2, 'w')
        fp.write(json.dumps(discard))
        fp.close()
        
        self.index = self.indexInc
        self.indexInc = {}
        self.postInfo = self.postInfoInc
        self.postInfoInc = {}
        if month not in self.months:
            self.months.append(month)
        return 1
