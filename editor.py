#!/user/bin/env python
# coding=utf-8
from constant import *
from playhouse.csv_loader import *
from tools import *
from redis_util import *

class Editor:
    srcModel = None
    batchSize = 100
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.count = 0
        self.pid = 0
        self.loadCheckPoint()
        self.loadInitData()

    def loadInitData(self):
        pass

    def loadData(self):
        srcModel = self.srcModel
        self.heartbeat()
        return srcModel.select().where(srcModel.id>self.checkPoint).order_by(srcModel.id).limit(100)

    def heartbeat(self, status=1):
        name = self.__class__.__name__
        info = {'name':name, 'status':status, 'ts':time.time()}
        pushInto(Heartbeat, info, ['name'])

    def edit(self, row):
        return 0

    def progress(self):
        self.count += 1
        if self.count % self.batchSize == 0:
            logging.info('Processed: %s/%s'%(self.valid, self.count))

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
        self.heartbeat(0)
        return True
                        
    def run(self):
        self.valid = 0
        clsName = self.__class__.__name__
        while True:
            rows = self.loadData()
            if len(rows) == 0:
                break
            for row in rows:
                self.valid += self.edit(row)
                self.setCheckPoint(row)
                self.progress()
        self.finish()
        self.saveCheckPoint()
        logging.info('%s Done %s/%s'%(clsName, self.valid, self.count))

class Monitor(Editor):
    def loadData(self):
        mdls = Heartbeat.select().where(Heartbeat.status==1)
        for mdl in mdls:
            tsdiff = int(time.time() - mdl.ts)
            if tsdiff > 10*60:#send mail
                title = '【报警】%s已超过%s分钟没有心跳'%(mdl.name, tsdiff/60)
                addr = ['yandechen@mia.com']
                mail = EmailUtil('exmail.qq.com', 'miasearch@mia.com', 'HelloJack123')
                mail.sendEmail(addr, title)
        return []
    
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
        if count['empty'] > 0:
            model.status = INIT_STATUS
        elif len(srvLinks) > 0:
            model.status = UPLOADED_STATUS
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
            if title and title[:3] == '"\\u':
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

#import cv2
class AvatarFilter(Editor):
    srcModel = LinkMap
#    cascade = cv2.CascadeClassifier('/usr/share/opencv/haarcascades/haarcascade_frontalface_alt.xml')
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

import heapq
import hashlib
import os
class SimPost(Editor):
    thresh = 0.2
    maxPostNum = 200000
    minWords = 10
    batchSize = 10000
    def loadInitData(self):
        self.index = {}
        self.indexInc = {}
        self.postInfo = {}
        self.postInfoInc = {}
        self.fp = None
    
    def setCheckPoint(self, post):
        return 0

    def loadData(self):
        if self.fp == None:
            yday = datetime.date.today() - datetime.timedelta(days=1)
            self.dupDate = yday.strftime('%Y%m%d')
            path = '/opt/article_in_mia/deduped/%s'%self.dupDate
            if os.path.exists(path):
                return []
            filename = '/opt/article_in_mia/%s/dump_subject_file_do_not_delete'%self.dupDate
            try:
                self.fp = open(filename)
            except:
                return []
            self.startDate = str(datetime.date.today() - datetime.timedelta(days=300))

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

    def splitTextSent(self, text):
        text = text.lower().decode()
        offset = 0
        idx = 0
        sentList = []
        for word in text:#sentence split
            v = ord(word)
            if (v >= 19904 and v <= 40895) or (v >= ord('a') and v <= ord('z')) or (v >= ord('0') and v <= ord('9')) or word==' ':
                pass
            else:#sentence split
                if idx - offset > 5:#exclude short sentence
                    sentList.append(text[offset:idx])
                offset = idx + 1
            idx += 1
        if idx - offset > 5:#short sentence
            sentList.append(text[offset:idx])
        return sentList

    def calcDupList(self):
        #batch end, calc intersect
        logging.info('Calc intersect, post num: %s, index len:%s'%(len(self.postInfo), len(self.index)))
        intersect = {}
        for key, pidList in self.index.iteritems():
            #if len(key) < 10 and len(pidList) > 10:#too common
            if len(pidList) > 20:#too common
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
        dupList = sorted(matchList, key=lambda x:str(self.postInfo[x][2])+','+self.postInfo[x][1], reverse=True)
        return dupList


    def edit(self, post):
        try:
            pid = int(post[0])
            uid = int(post[1])
            title = post[11]
            content = post[12]
            imgNum = post[3]
            textLen = post[4]
            postPv = post[7]
            date = post[8][:10]
            sku = post[10]
            typ = post[2]
        except:
            return 0
        if date < self.startDate:
            return 0
        
        text = ''
        if title != 'NULL':
            text = title+',,,'
        if content != 'NULL':
            text += content
        sentList = self.splitTextSent(text)
        if len(sentList) < 4:#short post
            return 0
        
        topSent = heapq.nlargest(20, sentList, key=lambda x:len(x))
        for sent in topSent:
            if sent not in self.index:
                self.index[sent] = set()
            self.index[sent].add(pid)
        #[wordCount, info, matchId, maxSim, simPid]
        pinfo = '%s,%s,%s'%(imgNum, postPv, textLen)
        self.postInfo[pid] = [len(topSent), pinfo, 0, 0, 0, uid, post[8], text]
        return 1

    def finish(self):
        if not self.index:
            return
        path = '/opt/article_in_mia/deduped/%s'%self.dupDate
        if not os.path.exists(path):
            os.mkdir(path)
        name1 = path+'/sim.txt'
        name2 = path+'/dup.txt'
        dupList = self.calcDupList()
        posts = [[x]+self.postInfo[x] for x in dupList]
        fp = open(name1, 'w')
        for post in posts:
            fp.write('\t'.join([str(x) for x in [post[0], post[3], post[6], post[7], post[8]]]) + '\n')
        fp.close()
        matchid = 0
        discard = []
        for post in posts:
            if post[3] == matchid:
                discard.append(post[0])
            else:
                matchid = post[3]
        fp = open(name2, 'w')
        fp.write(json.dumps(discard))
        fp.close()
        os.system('cp %s %s'%(name1, path[:-9]))
        os.system('cp %s %s'%(name2, path[:-9]))

import redis
import copy
import re
#每日曝光抽取为文件
class DailyExpose(Editor):
    def loadData(self):
        rds = redis.StrictRedis(host = '10.1.60.190')
        today = datetime.date.today()
        yday = today - datetime.timedelta(days=1)
        ydayStr = yday.strftime('%Y%m%d')
        if self.checkPoint == int(today.strftime('%Y%m%d')):
            return []
        date = yday
        lenDict = {}
        while True:
            dateStr = date.strftime('%Y%m%d')
            path = '/opt/parsed_data/did_article/%s/'%dateStr
            if not os.path.exists(path):
                os.mkdir(path)
            os.system('/usr/local/hadoop/bin/hadoop fs -get  /search/userprofile/%s/article/part* %s'%(dateStr, path))
            if not os.path.exists(path+'part-00000'):
                return []
            fp = open('data/expose_%s'%dateStr, 'w')
            for fname in os.listdir(path):
                logging.info('Process %s'%(path+fname))
                for line in open(path+fname):
                    idx = line.find("',")
                    key = line[3:idx]
                    lkey = 'session_%s'%(key)
                    dkey = 'session_detail_%s_%s'%(dateStr, key)
                    #expose data of list page
                    if lkey not in lenDict:
                        lenDict[lkey] = rds.llen(lkey)
                    for ele in rds.lrange(lkey, start=0, end=-1):
                        if not ele:
                            continue
                        eleli = ele.split(',')
                        ts = int(eleli[-1])
                        if ts < 0:
                            dt = datetime.datetime.fromtimestamp(abs(ts))
                            if dt.date() > date:
                                continue
                            elif dt.date() == date:
                                fp.write('list:%s:%s\n'%(key, ele))
                            else:
                                break
                        else:
                            break
                    #expost data of detail page
                    if dkey not in lenDict:
                        lenDict[dkey] = rds.llen(dkey)
                    for ele in rds.lrange(dkey, start=0, end=-1):
                        if not ele:
                            continue
                        eleli = ele.split(',')
                        ts = int(eleli[-1])
                        if ts < 0:
                            dt = datetime.datetime.fromtimestamp(abs(ts))
                            if dt.date() > date:
                                continue
                            elif dt.date() == date:
                                fp.write('detail:%s:%s\n'%(key, ele))
                            else:
                                break
                        else:
                            break
            fp.close()
            if date == yday:
                break
            date += datetime.timedelta(days=1)
        fp = open('/opt/parsed_data/ctr/expose_length_%s'%ydayStr, 'w')
        for key, llen in lenDict.iteritems():
            if llen > 100:
                fp.write('%s, %s\n'%(key, llen))
        fp.close()
        self.checkPoint = int(today.strftime('%Y%m%d'))
        return []
#每日真实曝光
class DailyExpose2(Editor):
    def loadData(self):
        rds = redis.StrictRedis(host = '10.1.60.190')
        today = datetime.date.today()
        yday = today - datetime.timedelta(days=1)
        ydayStr = yday.strftime('%Y%m%d')
        if self.checkPoint == int(today.strftime('%Y%m%d')):
            return []
        date = yday
        lenDict = {}
        while True:
            itemCtr = {}
            dateStr = date.strftime('%Y%m%d')
            path = 'data/click-%s/'%dateStr
            if not os.path.exists(path):
                os.mkdir(path)
            os.system('hadoop fs -get /search/parsed_data/%s/article_uv_with_referer/part-* %s'%(dateStr, path))
            if not os.path.exists(path+'part-00000'):
                return []
            fp = open('data/expose_%s'%dateStr, 'w')
            fpClick = open('/opt/parsed_data/ctr/exposed-click-%s'%dateStr, 'w')
            for fname in os.listdir(path):
                if fname[:4] != 'part':
                    continue
                logging.info('Process %s'%(path+fname))
                for line in open(path+fname):
                    idx = line.find("',")
                    li = line[3:idx].split('_')
                    num = line[idx+2:-2]
                    pid = li[0]
                    _type = li[1]
                    dvcId = li[2]
                    clickTs = int(li[3])/1000
                    if _type == 'grouphome':
                        key = 'session_%s'%(dvcId)
                        typ = 'list'
                    elif _type == 'koubeidetail':
                        key = 'session_detail_%s_%s'%(dateStr, dvcId)
                        typ = 'detail'
                        ppid = li[4]
                    else:
                        continue
                    isExpose = False
                    #expose data of list page
                    if key not in lenDict:
                        lenDict[key] = rds.llen(key)
                    for ele in rds.lrange(key, start=0, end=-1):
                        if not ele:
                            continue
                        eleli = ele.split(',')
                        ts = int(eleli[-1])
                        if ts < 0:
                            if clickTs < abs(ts):#repeat expose
                                continue
                            dt = datetime.datetime.fromtimestamp(abs(ts))
                            if dt.date() > date:
                                continue
                            elif dt.date() == date:
                                try:
                                    idx = eleli.index(pid)
                                    ctn = '%s:%s:%s\n'%(typ, dvcId, ','.join(eleli[:idx+1]))
                                    fp.write(ctn)
                                    if typ == 'detail':
                                        if ppid not in itemCtr:
                                            itemCtr[ppid] = {}
                                        for _pid in eleli[:idx+1]:
                                            if _pid not in itemCtr[ppid]:
                                                itemCtr[ppid][_pid] = [0, 0, 0]
                                            itemCtr[ppid][_pid][1] += 1
                                    isExpose = True
                                    break
                                except:
                                    pass
                            else:
                                break
                        else:
                            break
                    if isExpose:
                        fpClick.write(line)
                        if typ == 'detail':
                            if ppid not in itemCtr:
                                itemCtr[ppid] = {}
                            if pid not in itemCtr[ppid]:
                                itemCtr[ppid][pid] = [0, 0, 0]
                            itemCtr[ppid][pid][0] += int(num)
            fp.close()
            fpClick.close()
            fp = open('/opt/parsed_data/ctr/item-%s'%dateStr, 'w')
            for ppid, eles in itemCtr.iteritems():
                for pid, stat in eles.iteritems():
                    stat[2] = calcCrt(stat[0], stat[1])
                    fp.write('%s\t%s\t%s\t%s\t%s\n'%(ppid, pid, stat[0], stat[1], stat[2]))
            fp.close()
            if date == yday:
                break
            date += datetime.timedelta(days=1)
        fp = open('/opt/parsed_data/ctr/expose_length_%s'%ydayStr, 'w')
        for key, llen in lenDict.iteritems():
            if llen > 100:
                fp.write('%s, %s\n'%(key, llen))
        fp.close()
        self.checkPoint = int(today.strftime('%Y%m%d'))
        return []
#koubeilist的点击率
class DailyExpose3(Editor):
    def getStrategy(self, dvcId):
        strategy = 0
        abTest = [0, 10]
        if abTest:
            hint = zlib.crc32(dvcId)&0xffffffff
            left = hint % sum(abTest)
            for i, v in enumerate(abTest):
                if left < v:
                    break
                left -= v
            strategy = i
        return strategy
    
    def loadData(self):
        today = datetime.date.today()
        yday = today - datetime.timedelta(days=1)
        ydayStr = yday.strftime('%Y%m%d')
        if self.checkPoint == int(today.strftime('%Y%m%d')):
            return []
        date = today - datetime.timedelta(days=1)
        rds = RedisUtil(env='online')
        while True:
            kblCtr = {}
            dateStr = date.strftime('%Y%m%d')
            path = 'data/click-%s/'%dateStr
            if not os.path.exists(path+'part-00000'):
                return []
            for fname in os.listdir(path):
                logging.info('Process %s'%(path+fname))
                for line in open(path+fname):
                    idx = line.find("',")
                    li = line[3:idx].split('_')
                    num = line[idx+2:-2]
                    pid = li[0]
                    _type = li[1]
                    dvcId = li[2]
                    if _type != 'koubeilist':
                        continue
                    stgy = self.getStrategy(dvcId)
                    if stgy == 0:
                        continue
                    if pid not in kblCtr:
                        kblCtr[pid] = [0, 0, 0]
                    kblCtr[pid][0] += int(num)
            for key in rds.keys('koubei:expose:%s:more:*'%dateStr):
                for expose in rds.inst.lrange(key, start=0, end=-1):
                    if not expose:
                        continue
                    eles = json.loads(expose)
                    if len(eles) > 2 and type(eles[-2]) == unicode:#dvcId
                        dvcId = eles[-2]
                        stgy = self.getStrategy(dvcId)
                        if stgy == 0:
                            continue
                        mdls = Koubei.select(Koubei.subject_id).where(Koubei.id<<eles[:-2])
                    else:
                        continue
                        #mdls = Koubei.select(Koubei.subject_id).where(Koubei.id<<eles)
                    for mdl in mdls:
                        pid = str(mdl.subject_id)
                        if pid not in kblCtr:
                            kblCtr[pid] = [0, 0, 0]
                        kblCtr[pid][1] += 1
            fp = open('/opt/parsed_data/ctr/kblist-%s'%dateStr, 'w')
            for pid, stat in kblCtr.iteritems():
                stat[2] = calcCrt(stat[0], stat[1])
                fp.write('%s\t%s\t%s\t%s\n'%(pid, stat[0], stat[1], stat[2]))
            fp.close()
            if date == yday:
                break
            date += datetime.timedelta(days=1)
        self.checkPoint = int(today.strftime('%Y%m%d'))
        return []
#push的点击率
class DailyExpose4(Editor):
    def loadData(self):
        today = datetime.date.today()
        yday = today - datetime.timedelta(days=1)
        dateStr = yday.strftime('%Y%m%d')
        if self.checkPoint == int(today.strftime('%Y%m%d')):
            return []
        mdls = PushLog.select().where(PushLog.created>dateStr)
        pushCtr = {}
        for mdl in mdls:
            if mdl.dvcid.find('-') > 0:
                continue
            if mdl.pid not in pushCtr:
                pushCtr[mdl.pid] = [0, 0, 0]
            pushCtr[mdl.pid][1] += 1
        path = '/opt/parsed_data/personalized_post/%s/push_clicked_format.txt'%dateStr
        for line in open(path):
            li = line.split('\t')
            if li[1].find('-') > 0:
                continue
            pid = int(li[2])
            if pid not in pushCtr:
                continue
            pushCtr[pid][0] += 1
        fp = open('/opt/parsed_data/ctr/push-%s'%dateStr, 'w')
        for pid, stat in pushCtr.iteritems():
            stat[2] = calcCrt(stat[0], stat[1])
            fp.write('%s\t%s\t%s\t%s\n'%(pid, stat[0], stat[1], stat[2]))
        fp.close()
        self.checkPoint = int(today.strftime('%Y%m%d'))
        return []

#曝光，点击，点击率，普通、达人比较
class PostUniform(Editor):
    def loadData(self):
        rds = redis.StrictRedis(host = '10.1.60.190')
        #load care info
        careInfo = {}
        fname = 'data/care.txt'
        for line in open(fname):
            li = line.split(',')
            careInfo[int(li[0])] = {'stat':[0, 0, 0, 0, 0, li[1].decode().encode('gbk')]}

        #load post info
        pinfo = {}
        today = datetime.date.today()
        yday = today - datetime.timedelta(days=1)
        date = yday
        dateStr = yday.strftime('%Y%m%d')
        if not os.path.exists('data/expose_%s'%dateStr):
            return []
        if self.checkPoint == int(today.strftime('%Y%m%d')):
            return []
        fname = '/opt/article_in_mia/%s/dump_subject_file_do_not_delete'%yday.strftime('%Y%m%d')
        fp = open(fname)
        for line in fp:
            post = line.split('\t')
            while len(post) < 18:
                line += fp.next()
                post = line.split('\t')
            pid = int(post[0])
            uid = int(post[1])
            ts = post[8]
            pinfo[pid] = [uid, ts]
            if uid in careInfo:
                careInfo[uid]['stat'][4] += 1
        fp.close()

        postExposeTime = {}
        postClickTime = {}
        #load post info
        while True:
            #ready data
            careInfo2 = copy.deepcopy(careInfo)
            stats = {}
            #expose data
            dateStr = date.strftime('%Y%m%d')
            path = 'data/expose_%s'%dateStr
            for line in open(path):
                li = line.split(':')
                _type = li[0]
                dvcId = li[1]
                ele = li[2][:-1]
                for postid in ele.split(','):
                    try:
                        pid = int(postid)
                    except:
                        continue
                    if pid not in stats:
                        stats[pid] = [0, 0, 0, 0, 0, 0]
                    if _type == 'list':
                        stats[pid][1] += 1
                    elif _type == 'detail':
                        stats[pid][4] += 1
                    
                    if pid not in pinfo:
                        continue
                    cdate = datetime.date(*[int(x) for x in pinfo[pid][1][:10].split('-')])
                    diff = date - cdate
                    if diff.days not in postExposeTime:
                        postExposeTime[diff.days] = 0
                    postExposeTime[diff.days] += 1

            #click data
            path = '/opt/parsed_data/ctr/exposed-click-%s'%dateStr
            if not os.path.exists(path):
                return []
            for line in open(path):
                li = line[3:-2].split("',")
                try:
                    pid = int(li[0].split('_')[0])
                    num = int(li[1])
                except:
                    continue
                if pid not in stats:
                    stats[pid] = [0, 0, 0, 0, 0, 0]
                if 'koubeidetail' in li[0]:
                    stats[pid][3] += num
                elif 'grouphome' in li[0]:
                    stats[pid][0] += num
                if pid not in pinfo:
                    continue
                cdate = datetime.date(*[int(x) for x in pinfo[pid][1][:10].split('-')])
                diff = date - cdate
                if diff.days not in postClickTime:
                    postClickTime[diff.days] = 0
                postClickTime[diff.days] += 1
            
            #calc crt
            for pid, stat in stats.iteritems():
                stat[2] = calcCrt(stat[0], stat[1])
                stat[5] = calcCrt(stat[3], stat[4])
                if pid in pinfo:
                    uid = pinfo[pid][0]
                    if uid in careInfo2:
                        careInfo2[uid][pid] = stat
            #pid: click1, expose1, crt1, click2, expose2, crt2
            fp = open('/opt/parsed_data/ctr/'+str(date), 'w')
            for pid, stat in stats.iteritems():
                line = str(pid)+'\t'+'\t'.join([str(x) for x in stat])+'\n'
                fp.write(line)
            fp.close()

            #calc tarento's click and expose
            for uid, pstat in careInfo2.iteritems():
                for pid, stat in pstat.iteritems():
                    if pid == 'stat':
                        continue
                    if pid not in pinfo or pinfo[pid][0] != uid:
                        print pid
                    click = stat[0] + stat[3]
                    expose = stat[1] + stat[4]
                    if expose > 0:
                        careInfo2[uid]['stat'][0] += expose
                        careInfo2[uid]['stat'][1] += 1
                    if click > 0:
                        careInfo2[uid]['stat'][2] += click
                        careInfo2[uid]['stat'][3] += 1

            if date == yday:
                break
            date += datetime.timedelta(days=1)
        fp = open('data/postExposeTime.csv', 'w')
        for days, count in postExposeTime.iteritems():
            fp.write('%s, %s\n'%(days, count))
        fp.close()
        fp = open('data/postClickTime.csv', 'w')
        for days, count in postClickTime.iteritems():
            fp.write('%s, %s\n'%(days, count))
        fp.close()
        self.checkPoint = int(today.strftime('%Y%m%d'))
        return []
#30天点击率
class CollectCtr(Editor):
    def loadData(self):
        accPath = '/opt/parsed_data/ctr/acc_ctr'
        itemAccPath = '/opt/parsed_data/ctr/item_acc_ctr'
        kblAccPath = '/opt/parsed_data/ctr/kblist_acc_ctr'
        accCtr = {}
        itemAccCtr = {}
        kblAccCtr = {}
        today = datetime.date.today()
        yday = today - datetime.timedelta(days=1)
        date = today - datetime.timedelta(days=30)
        if self.checkPoint == int(today.strftime('%Y%m%d')):
            return []
#        if os.path.exists(accPath):
#            for line in open(accPath):
#                li = line[:-1].split('\t')
#                li[3] = 0
#                li[6] = 0
#                pid = int(li[0])
#                accCtr[pid] = [int(x) for x in li[1:]]
#        if os.path.exists(itemAccPath):
#            for line in open(itemAccPath):
#                li = line[:-1].split('\t')
#                ppid = li[0]
#                pid = li[1]
#                if ppid not in itemAccCtr:
#                    itemAccCtr[ppid] = {}
#                itemAccCtr[ppid][pid] = [int(li[2]), int(li[3]), float(li[4])]
#        if os.path.exists(kblAccPath):
#            for line in open(kblAccPath):
#                li = line[:-1].split('\t')
#                pid = li[0]
#                kblAccCtr[pid] = [int(li[1]), int(li[2]), 0]

        while True:
            dateStr = date.strftime('%Y%m%d')
            path = '/opt/parsed_data/ctr/%s'%date.strftime('%Y-%m-%d')
            itemPath = '/opt/parsed_data/ctr/item-%s'%dateStr
            kblPath = '/opt/parsed_data/ctr/kblist-%s'%dateStr
            if not os.path.exists(path) or not os.path.exists(itemPath) or not os.path.exists(kblPath):
                return []
            logging.info('Process %s ctr'%dateStr)
            for line in open(path):
                li = line[:-1].split('\t')
                li[3] = 0
                li[6] = 0
                pid = int(li[0])
                if pid in accCtr:
                    for i, v in enumerate(li[1:]):
                        accCtr[pid][i] += int(v)
                else:
                    accCtr[pid] = [int(x) for x in li[1:]]
            for line in open(itemPath):
                li = line[:-1].split('\t')
                ppid = li[0]
                pid = li[1]
                if ppid not in itemAccCtr:
                    itemAccCtr[ppid] = {}
                if pid not in itemAccCtr[ppid]:
                    itemAccCtr[ppid][pid] = [0, 0, 0]
                stat = itemAccCtr[ppid][pid]
                stat[0] += int(li[2])
                stat[1] += int(li[3])
            for line in open(kblPath):
                li = line[:-1].split('\t')
                pid = li[0]
                if pid not in kblAccCtr:
                    kblAccCtr[pid] = [0, 0, 0]
                expose = int(li[2])
                if expose == 0:
                    continue
                kblAccCtr[pid][0] += int(li[1])
                kblAccCtr[pid][1] += expose
            if date == yday:
                break
            date += datetime.timedelta(days=1)

        self.checkPoint = int(today.strftime('%Y%m%d'))
        fp = open(accPath, 'w')
        for pid, stat in accCtr.iteritems():
            stat[2] = calcCrt(stat[0], stat[1])
            stat[5] = calcCrt(stat[3], stat[4])
            fp.write(str(pid)+'\t'+'\t'.join([str(x) for x in stat])+'\n')
        fp.close()
        fp = open(itemAccPath, 'w')
        for ppid, eles in itemAccCtr.iteritems():
            for pid, stat in eles.iteritems():
                stat[2] = calcCrt(stat[0], stat[1])
                fp.write('%s\t%s\t%s\t%s\t%s\n'%(ppid, pid, stat[0], stat[1], stat[2]))
        fp.close()
        fp = open(kblAccPath, 'w')
        for pid, stat in kblAccCtr.iteritems():
            stat[2] = calcCrt(stat[0], stat[1])
            fp.write('%s\t%s\t%s\t%s\n'%(pid, stat[0], stat[1], stat[2]))
        fp.close()
        return []
        
#点击位置计算，评价口碑排序效果
class ClickPos(Editor):
    def loadData(self):
        date = datetime.date.today() - datetime.timedelta(days=6)
        rankInfo = {}
        clickPos = {}
        related = {}
        #click data
        path = '/opt/parsed_data/uv/%s/1/'%date.strftime('%Y%m%d')
        processed = 0
        rds = RedisUtil()
        for name in os.listdir(path):
            if name[:4] != 'part':
                continue
            for line in open(path+name):
                print 'process %s'%processed
                processed += 1
                li = line[3:-2].split("',")
                pid = int(li[0])
                try:#calc relateSet
                    mdl = Koubei.select().where(Koubei.subject_id==pid).get()
                    itemId = mdl.item_id
                    kbid = mdl.id
                    if itemId in related:
                        relateSet = related[itemId]
                    else:
                        relateSet = set([itemId])
                        related[itemId] = relateSet
                        mdl = RelateSku.select().where(RelateSku.id==itemId).get()
                        flag = mdl.relate_flag
                        if flag:
                            mdls = RelateSku.select().where(RelateSku.relate_flag==flag)
                            for mdl in mdls:
                                relateSet.add(mdl.id)
                                related[mdl.id] = relateSet
                except:
                    continue

                #get rank info from koubei
                if kbid not in rankInfo:
                    #mdls = Koubei.select().where((Koubei.item_id<<list(relateSet))&(Koubei.status==2)&(Koubei.subject_id>0)&(Koubei.auto_evaluate==0)).order_by(Koubei.is_bottom, Koubei.auto_evaluate, Koubei.rank_score.desc(), Koubei.score.desc(), Koubei.created_time.desc())
                    #for i, mdl in enumerate(mdls):
                    #    rankInfo[mdl.subject_id] = (i, mdl.item_id)
                    result = []
                    for itemId in relateSet:
                        key = 'koubei:rank_score:%s'%itemId
                        rs = rds.get_obj(key)
                        if not rs:
                            continue
                        result += rs
                        for _kbid, score in rs:
                            rankInfo[_kbid] = [0, itemId]
                    ranked = sorted(result, key=lambda x:x[1], reverse=True)
                    for i, rank in enumerate(ranked):
                        rankInfo[rank[0]][0] = i
                        
                if kbid not in rankInfo:#post gone
                    continue
                rank = rankInfo[kbid][0]
                itemId = min(relateSet)
                if itemId not in clickPos:
                    clickPos[itemId] = {}
                if rank not in clickPos[itemId]:
                    clickPos[itemId][rank] = 0
                clickPos[itemId][rank] += int(li[1])

        cpStats = {}
        for itemId, cp in clickPos.iteritems():
            cpinfo = sorted(cp.iteritems(), key=lambda x:x[0])
            num = 0
            positive = 0
            negative = 0
            for rank, count in cpinfo:
                if rank not in cpStats:
                    cpStats[rank] = 0
                cpStats[rank] += count
                if rank > 9:
                    continue
                if num > 0:
                    diff = cpinfo[num-1][1] - count
                    if diff > 0:
                        positive += diff
                    else:
                        negative += diff
                num += 1
            clickPos[itemId]['positive'] = positive
            clickPos[itemId]['negative'] = negative
        fp = open('data/click-position-'+str(date), 'w')
        for rank, count in cpStats.iteritems():
            fp.write('%s\t%s\n'%(rank, count))
        fp.close()

        fp = open('data/sku-click-position-'+str(date), 'w')
        for itemId, cp in clickPos.iteritems():
            fp.write('%s\t%s\t%s\n'%(itemId, cp['positive'], cp['negative']))
        fp.close()

        fp = open('data/rankInfo-'+str(date), 'w')
        fp.write(json.dumps(rankInfo))
        fp.close()
        return []

class MaterialScore(Editor):
    def setCheckPoint(self, post):
        return 0
    def loadInitData(self):
        self.postCtr = {}
        self.catgyBucket = {}
        self.brandBucket = {}
        self.skuBucket = {}
        self.userBucket = {}
        self.redis = RedisUtil()
        self.fp = None
        path = '/opt/parsed_data/ctr/kblist_acc_ctr'
        if os.path.exists(path):
            for line in open(path):
                li = line[:-1].split('\t')
                pid = int(li[0])
                ctr = float(li[3])
                if ctr > 0:
                    self.postCtr[pid] = ctr

    def loadData(self):
        if not self.fp:
            date = datetime.date.today() - datetime.timedelta(days=1)
            path = '/opt/article_in_mia/%s/dump_subject_file_do_not_delete'%date.strftime('%Y%m%d')
            try:
                self.fp = open(path)
            except:
                return []

        res = []
        for line in self.fp:
            post = line[:-1].split('\t')
            while len(post) < 18:
                line += self.fp.next()
                post = line[:-1].split('\t')
            if post[21] == '1':
                res.append(post)
            if len(res) > self.batchSize:
                break
        return res

    def edit(self, post):
        try:
            pid = int(post[0])
            uid = int(post[1])
            skuIds = [int(x) for x in post[10].split(',') if x!='NULL' and x!='0']
            items = RelateSku.select().where(RelateSku.id<<skuIds)
            catgys = ItemCatgy.select().where(ItemCatgy.id<<[x.category_id for x in items])
            brandIds = [x.brand_id for x in items]
            catgyIds = []
            for mdl in catgys:
                catgyIds += [int(x) for x in mdl.path.split('-')]
            score = self.getScore(post)
            pScore = [pid, score]
            for brandId in brandIds:
                if brandId not in self.brandBucket:
                    self.brandBucket[brandId] = []
                self.brandBucket[brandId].append(pScore)
            for skuId in skuIds:
                if skuId not in self.skuBucket:
                    self.skuBucket[skuId] = []
                self.skuBucket[skuId].append(pScore)
            for catgyId in catgyIds:
                if catgyId not in self.catgyBucket:
                    self.catgyBucket[catgyId] = []
                self.catgyBucket[catgyId].append(pScore)
            if uid not in self.userBucket:
                self.userBucket[uid] = []
            self.userBucket[uid].append(pScore)
            return 1
        except:
            return 0
        
    def getScore(self, post):
        pid = int(post[0])
        textLen = int(post[4])
        picNum = int(post[3])
        score = picNum*3+min(textLen/20,20)
        if pid in self.postCtr:
            score += 20*self.postCtr[pid]
        return score
    def pushChunks(self, key, pScore, chunkSize=50):
        llen = self.redis.inst.llen(key)
        topN = heapq.nlargest(500, pScore, key=lambda x:x[1])
        for i in range(0, len(topN), chunkSize):
            chunk = topN[i:i+chunkSize]
            idx = i/chunkSize
            if llen > idx:
                self.redis.inst.lset(key, idx, json.dumps(chunk))
            else:
                self.redis.inst.rpush(key, json.dumps(chunk))
    def finish(self):
        chunkSize = 50
        for skuId, pScore in self.skuBucket.iteritems():
            key = 'material:sku:%s'%skuId
            self.pushChunks(key, pScore)
        for brandId, pScore in self.brandBucket.iteritems():
            key = 'material:brand:%s'%brandId
            self.pushChunks(key, pScore)
        for catgyId, pScore in self.catgyBucket.iteritems():
            key = 'material:category:%s'%catgyId
            self.pushChunks(key, pScore)
        for uid, pScore in self.userBucket.iteritems():
            key = 'material:user:%s'%uid
            self.pushChunks(key, pScore)
        if self.fp:
            self.fp.close()

class KoubeiScore(Editor):
    strategy = (0, 1)
    chunkSize = 100
    def __init__(self, **kwargs):
        self.clickCount = {}
        self.postCtr = {}
        Editor.__init__(self, **kwargs)
        if 'env' in kwargs:
            self.redis = RedisUtil(kwargs['env'])
        else:
            self.redis = RedisUtil()
        
    def loadInitData(self):
        self.firstLoad = True
        logging.info('load click data...')
        date = datetime.date.today() - datetime.timedelta(days=1)
        clickDays = 7
        path = '/opt/parsed_data/uv/%s/%s/'%(date.strftime('%Y%m%d'), clickDays)
        if os.path.exists(path):
            for name in os.listdir(path):
                if name[:4] != 'part':
                    continue
                for line in open(path+name):
                    li = line[3:-2].split("',")
                    try:
                        pid = int(li[0])
                        self.clickCount[pid] = int(li[1])
                    except:
                        continue
        path = '/opt/parsed_data/ctr/kblist_acc_ctr'
        if os.path.exists(path):
            for line in open(path):
                li = line[:-1].split('\t')
                pid = int(li[0])
                ctr = float(li[3])
                if ctr > 0:
                    self.postCtr[pid] = ctr
                
    def loadData(self):
        if not self.clickCount:
            return []
        if self.firstLoad:
            if self.checkPoint==int(datetime.date.today().strftime('%Y%m%d')):
                return []
            self.checkPoint = 0
            self.firstLoad = False
            
        return RelateSku.select().where(RelateSku.id>self.checkPoint).order_by(RelateSku.id).limit(self.batchSize)

    def getScore(self, mdl, sub, stagy=0):
        pid = mdl.subject_id
        if mdl.auto_evaluate and int(mdl.auto_evaluate) == 1:
            return [15]
        if mdl.is_bottom == 1:
            return [0]
        if not mdl.score:
            uscore = 5
        else:
            uscore = int(mdl.score)
        if not sub.semantic_analys:
            mscore = 2
        else:
            mscore = int(sub.semantic_analys)
        positive = uscore + mscore - 2
        if positive < 3:
            return [0]
        textLen = 0
        for c in sub.text:
            if ord(c) > 128:
                textLen += 1
        if sub.image_url:
            pics = sub.image_url.split('#')
        else:
            pics = []
        ctime = sub.created
        if pid in self.clickCount:
            click = self.clickCount[pid]
        else:
            click = 1
        pastDays = (datetime.date.today()-ctime.date()).days
        score = positive*5+len(pics)*3+min(textLen/20,10)+round(math.log(click)-0.25*pastDays/30, 2)
        if stagy==1 and sub.image_url and positive>4 and pid in self.postCtr:#abtest
            score += 100*self.postCtr[pid]
        return [score, positive, len(pics), textLen, click-1, pastDays]

    def calcRanked(self, itemId, mdls, incFlag=False):
        subjects = Subject.select().where(Subject.id<<[x.subject_id for x in mdls])
        subDict = {}
        dedup = {}
        for sub in subjects:
            if sub.user_id not in dedup:
                dedup[sub.user_id] = set()
            md5 = hashlib.md5(sub.text+sub.image_url).hexdigest()
            if md5 in dedup[sub.user_id]:
                continue
            dedup[sub.user_id].add(md5)
            subDict[sub.id] = sub
        for stagy in self.strategy:
            rankScore = {}
            if stagy == 0:
                key = 'koubei:score:%s'%itemId
            else:
                key = 'koubei:score:%s:%s'%(stagy, itemId)
            if incFlag:
                ranked = self.redis.inst.lrange(key, start=0, end=-1)
                for chunk in ranked:
                    for scinfo in json.loads(chunk):
                        kbid = scinfo[0]
                        score = scinfo[1:]
                        rankScore[kbid] = score
            for mdl in mdls:
                pid = mdl.subject_id
                if pid not in subDict:
                    continue
                kbid = mdl.id
                score = self.getScore(mdl, subDict[pid], stagy)
                rankScore[kbid] = score
    
            if len(rankScore) == 0:
                return 0
            sortList = [[kbid]+score for kbid, score in rankScore.iteritems()]
            ranked = sorted(sortList, key=lambda x:x[1], reverse=True)
            #save into redis
            llen = self.redis.inst.llen(key)
            for i in xrange(0, len(ranked), self.chunkSize):
                chunk = ranked[i:i+self.chunkSize]
                idx = i/self.chunkSize
                if llen > idx:
                    self.redis.inst.lset(key, idx, json.dumps(chunk))
                else:
                    self.redis.inst.rpush(key, json.dumps(chunk))
        return 1
    
    def edit(self, model):
        mdls = Koubei.select().where((Koubei.item_id==model.id)&(Koubei.status==2)&(Koubei.subject_id>0)).order_by(Koubei.created_time.desc())
        return self.calcRanked(model.id, mdls)

    def finish(self):
        self.checkPoint = int(datetime.date.today().strftime('%Y%m%d'))

class IncKoubeiScore(KoubeiScore):
    def loadInitData(self):
        self.IncKoubei = {}
    
    def loadData(self):
        return Koubei.select().where((Koubei.id>self.checkPoint)&(Koubei.status==2)&(Koubei.subject_id>0)).order_by(Koubei.id).limit(self.batchSize)
    
    def edit(self, model):
        itemId = model.item_id
        if itemId not in self.IncKoubei:
            self.IncKoubei[itemId] = []
        self.IncKoubei[itemId].append(model)
        return 1

    def finish(self):
        for itemId, mdls in self.IncKoubei.iteritems():
            self.calcRanked(itemId, mdls, True)

from bucket import *
#人口统计学特征：宝宝性别，年龄的bucket
class UserFeature(Editor):
    def loadData(self):
        dvcPost = {}
        today = datetime.date.today()
        yday = today - datetime.timedelta(days=1)
        date = today - datetime.timedelta(days=7)

        targetPath = '/opt/parsed_data/bucket/device-post-bucket.%s'%yday.strftime('%Y%m%d')
        if os.path.exists(targetPath):
            return []
        accCount = 0
        while True:
            path = 'data/access.log%s'%date.strftime('%Y%m%d')
            os.system('/usr/local/hadoop/bin/hadoop fs -get /search/parsed_data/%s/article/part* %s'%(date.strftime('%Y%m%d'), path))
            if not os.path.exists(path):
                return []
            for line in open(path):
                access = line.split(',')
                dvcId = access[0][3:-1]
                try:
                    pid = int(access[4][3:-1])
                except:
                    continue
                if dvcId not in dvcPost:
                    dvcPost[dvcId] = set()
                dvcPost[dvcId].add(pid)
                accCount += 1
            if date == yday:
                break
            date += datetime.timedelta(days=1)

        dvcBkt = DeviceBucket()
        bktCount = 0
        for dvcId, pSet in dvcPost.iteritems():
            bktCount += dvcBkt.put(dvcId.lower(), pSet)
        logging.info('Access count: %s, bucket count: %s'%(accCount, bktCount))
        dvcBkt.dump(targetPath)
        os.system('cp %s %s'%(targetPath, '/opt/parsed_data/bucket/final_good_post.txt'))
        del dvcBkt
        return []

#缺少评价sku统计
class NopicSku(Editor):
    skuDict = {}
    catgyDict = {}
    def loadCheckPoint(self):
        self.checkPoint = 0
    def loadData(self):
        return RelateSku.select(RelateSku.id, RelateSku.name, RelateSku.status, RelateSku.relate_flag, RelateSku.category_id_ng, RelateSku.is_single_sale, RelateSku.warehouse_type).where(RelateSku.id>self.checkPoint).order_by(RelateSku.id).limit(self.batchSize)

    def edit(self, model):
        if model.relate_flag:
            skuKey = model.relate_flag
        else:
            skuKey = model.id
        if skuKey not in self.skuDict:
            #skuId, total, nodefault, haspic, name, catgy, self-support
            self.skuDict[skuKey] = [0, 0, 0, 0, '', '', 'no']
            
        value = self.skuDict[skuKey]
        if int(model.is_single_sale) == 1 and int(model.warehouse_type) in [1, 6, 8]:
            value[6] = 'yes'
        mdls = Koubei.select(Koubei.id, Koubei.subject_id, Koubei.auto_evaluate).where((Koubei.item_id==model.id)&(Koubei.status==2)&(Koubei.subject_id>0)).order_by(Koubei.created_time.desc())
        
        for mdl in mdls:
            if value[0] == 0 and model.status and int(model.status) == 1:
                value[0] = model.id
                cid = model.category_id_ng
                value[4] = model.name
                try:
                    cmdl = ItemCatgy.select().where(ItemCatgy.id==cid).get()
                    if cmdl.path:
                        cid = int(cmdl.path.split('-')[0])
                        if cid in self.catgyDict:
                            cmdl = self.catgyDict[cid]
                        else:
                            cmdl = ItemCatgy.select().where(ItemCatgy.id==cid).get()
                            self.catgyDict[cid] = cmdl
                    catgy = cmdl.name.split('/')[0]
                    value[5] = catgy
                except:
                    pass
            value[1] += 1
            if not mdl.auto_evaluate or int(mdl.auto_evaluate) == 0:
                value[2] += 1
        if len(mdls):
            subjects = Subject.select(Subject.id, Subject.image_url).where(Subject.id<<[x.subject_id for x in mdls])
            for sub in subjects:
                if sub.image_url:
                    pics = sub.image_url.split('#')
                    if len(pics) > 0:
                        value[3] += 1
        return 1
                
    def finish(self):
        path = 'data/sku-koubei-count-%s.csv'%datetime.date.today().strftime('%Y%m%d')
        fp = open(path, 'w')
        for skuKey, value in self.skuDict.iteritems():
            if value[0] == 0:
                continue
            if value[1] < 10 or value[2] < 10 or value[3] < 3:
                fp.write('%s, %s, %s, %s, %s, %s, %s\n'%(value[0], value[1], value[2], value[3], value[4], value[5], value[6]))
        fp.close()

from email_util import *
class KbrankMonitor(Editor):
    def loadData(self):
        msg = ''
        try:
            skuId = 1743917
            pagesize = 10
            url = 'http://kbrank.rec.mia.com/koubei/get_koubei?skuIds=%s&page=0&pagesize=%s&debug=1'%(skuId, pagesize)
            resp = requests.get(url, timeout=3)
            res = json.loads(resp.content)
            if len(res['data']) == 10:
                url = 'http://10.1.106.39:5512/koubei/get_koubei?skuIds=%s&page=0&pagesize=%s&debug=1'%(skuId, pagesize)
                resp = requests.get(url, timeout=3)
                res = json.loads(resp.content)
                if len(res['data']) == 10:
                    url = 'http://10.1.106.32:5512/koubei/get_koubei?skuIds=%s&page=0&pagesize=%s&debug=1'%(skuId, pagesize)
                    resp = requests.get(url, timeout=3)
                    res = json.loads(resp.content)
                    if len(res['data']) == 10:
                        return []
        except Exception, e:
            msg = '[Exception] %s'%str(e)
        if not msg:
            msg = 'Broken %s'%url
        #send monite mail
        title = '【口碑排序监控报警】'
        addr = ['yandechen@mia.com', 'houjianyu@mia.com']
        mail = EmailUtil('exmail.qq.com', 'miasearch@mia.com', 'HelloJack123')
        mail.sendEmail(addr, title, msg)
        return []
#活动结束用户数据统计
class ActiveData(Editor):
    labelId = [31129]
    exclude = [28298, 23850, 29410]
    source = (2,)
    status = (1,)
    output = (0,1)
    titPic = False
    subItem = False
    labelNum = 0
    startDate = '20170825'
    endDate = '20170827'
    countLimit = 0
    info = {}
    detail = {}
    fpList = {}
    userActive = {}
    labelInfo = {}
    def loadCheckPoint(self):
        self.checkPoint = 0
    def loadData(self):
        if not self.labelInfo:
            mdls = RawLabel.select().where(RawLabel.id<<self.labelId)
            for mdl in mdls:
                self.labelInfo[mdl.id] = mdl.title
        return Subject.select().where((Subject.id>self.checkPoint)&(Subject.created>self.startDate)&(Subject.source<<self.source)&(Subject.status<<self.status)).order_by(Subject.id).limit(self.batchSize)

    def edit(self, model):
        pid = model.id
        if self.subItem:
            try:
                SubTag.select().where((SubTag.subject_id==pid)&(SubTag.item_id>0)).get()
            except:
                return 0
        dateStr = model.created.strftime('%Y%m%d')
        if dateStr < self.startDate or dateStr > self.endDate:
            return 0
        prefer = 0
        mdls = Label.select().where(Label.subject_id==pid)
        st = set([mdl.label_id for mdl in mdls])
        if st & set(self.exclude):
            return 0
        if self.labelId and not st & set(self.labelId):
            return 0
        if self.labelNum and len(st) != self.labelNum:
            return 0
        for mdl in mdls:
            if mdl.is_recommend:
                prefer = 1
        try:
            if model.source not in self.source:
                return 0
            if self.titPic:
                if not model.title and not model.image_url:
                    return 0
            #postCount, fineCount, firstPostId
            if model.user_id not in self.info:
                self.info[model.user_id] = [0, 0, model.id]
                self.detail[model.user_id] = {}
                self.userActive[model.user_id] = {}
            self.info[model.user_id][0] += 1
            zan = KoubeiZan.select().where(KoubeiZan.subject_id==pid).count()
            self.detail[model.user_id][pid] = zan
            self.info[model.user_id][1] += prefer
            for mdl in mdls:
                if mdl.label_id not in self.userActive[model.user_id]:
                    self.userActive[model.user_id][mdl.label_id] = [0, 0, 0]
                self.userActive[model.user_id][mdl.label_id][0] += 1
                self.userActive[model.user_id][mdl.label_id][1] += prefer
        except:
            return 0
        return 1

    def writeData(self, uid, stat):
        model = Subject.select().where((Subject.user_id==uid)&(Subject.status<<self.status)).order_by(Subject.id).limit(1).get()
        if model.id == stat[2]:
            stat[2] = model.source
        else:
            stat[2] = 0
        mdl = User.select().where(User.id==uid).get()
        for i, finfo in self.fpList.items():
            fp = finfo[1]
            if i == 0:
                m = Fans.select(fn.COUNT(Fans.id).alias('fans')).where(Fans.replation_user_id==uid).get()
                regTime = mdl.create_date.strftime('%Y-%m-%d %H:%M:%S')
                fp.write('%s, %s, %s, %s, %s, %s, %s, %s\n'%(uid, mdl.username, mdl.nickname, stat[0], stat[1], stat[2], m.fans, regTime))
            elif i == 1:
                if stat[0] < self.countLimit:
                    return
                try:
                    addr = Address.select().where((Address.user_id==uid)&(Address.is_default==1)).get()
                    prov = Prov.select().where(Prov.id==addr.prov).get()
                    city = City.select().where(City.id==addr.city).get()
                    area = Area.select().where(Area.id==addr.area).get()
                    fp.write('%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s\n'%(uid, mdl.username, mdl.nickname, mdl.cell_phone, addr.name, prov.name, city.name, area.name, addr.address, stat[0], stat[1], stat[2]))
                except:
                    fp.write('%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s\n'%(uid, mdl.username, mdl.nickname, mdl.cell_phone, '', '', '', '', '', stat[0], stat[1], stat[2]))
            elif i == 2:
                for pid, zan in self.detail[uid].iteritems():
                    fp.write('%s, %s, %s, %s, %s\n'%(uid, mdl.username, mdl.nickname, pid, zan))
            elif i == 3:
                active = self.userActive[uid]
                if stat[2] > 0:#first post
                    mdls = Label.select().where(Label.subject_id==model.id)
                    for m in mdls:
                        if m.label_id in active:
                            active[m.label_id][2] = stat[2]
                for lid, stat in active.iteritems():
                    fp.write('%s, %s, %s, %s, %s, %s, %s, %s\n'%(uid, mdl.username, mdl.nickname, lid, self.labelInfo[lid], stat[0], stat[1], stat[2]))
                    
    def finish(self):
        for i in self.output:
            path = 'data/stat-%s-%s-%s.csv'%(str(self.labelId), ','.join(map(str, self.source)), i)
            fp = open(path, 'w')
            self.fpList[i] = [path, fp]
        for uid, stat in self.info.iteritems():
            self.writeData(uid, stat)
        for typ in self.fpList:
            self.fpList[typ][1].close()
        title = '【活动数据调取】'
        addr = ['yandechen@mia.com']
        mail = EmailUtil('exmail.qq.com', 'miasearch@mia.com', 'HelloJack123')
        files = [finfo[0] for typ, finfo in self.fpList.items()]
        mail.sendEmail(addr, title, files=files)

class ActiveData2(ActiveData):
    roleId = 46
    labelId = [28298]
    exclude = []
    source = (1,2)
    output = (0,)
    startDate = '20170701'
    endDate = '20170830'
    def loadInitData(self):
        mdls = UserRole.select().where(UserRole.role_id==self.roleId)
        self.userId = [x.user_id for x in mdls]
    def loadData(self):
        return Subject.select().where((Subject.user_id<<self.userId)&(Subject.id>self.checkPoint)&(Subject.created>self.startDate)).order_by(Subject.id).limit(self.batchSize)
    
class QualityPost(Editor):
    def loadData(self):
        today = datetime.date.today()
        yday = today - datetime.timedelta(days=1)
        yyday = today - datetime.timedelta(days=2)
        dateStr = yday.strftime('%Y%m%d')
        if self.checkPoint == int(today.strftime('%Y%m%d')):
            return []
        redis = RedisUtil(env='online')
        path = 'data/user-action-%s'%dateStr
        result = {}
        count = 0
        for line in open(path):
            count += 1
            if count%100 == 0:
                logging.info('Processed %s'%count)
            li = line[:-1].split('\t')
            skuId = li[1]
            key = 'koubei:rank_score:%s'%skuId
            ranked = redis.get_obj(key)
            if not ranked:
                continue
            for pid, score in ranked:
                if score < 33:#not good
                    continue
                mdl = Subject.select(Subject.text, Subject.image_url).where(Subject.id==pid).get()
                if not mdl.image_url:
                    continue
                pics = mdl.image_url.split('#')
                if len(pics) > 1 and len(mdl.text) > 100:
                    mdl = Koubei.select(Koubei.machine_score).where(Koubei.subject_id==pid).get()
                    if not mdl.machine_score:
                        mscore = 2
                    else:
                        mscore = int(mdl.machine_score)
                    if not mdl.score:
                        uscore = 5
                    else:
                        uscore = int(mdl.score)
                    if mscore > 1 and uscore > 3:
                        if skuId not in result:
                            result[skuId] = []
                        result[skuId].append((pid, len(pics)))
                if skuId in result and len(result[skuId]) > 9:
                    break
        fp = open('data/quality-koubei-%s'%dateStr, 'w')
        fpAll = open('data/quality-koubei', 'a')
        resAll = set()
        for line in open('data/quality-koubei'):
            li = line[:-1].split('\t')
            resAll.add('%s,%s'%(li[0], li[1]))
        for skuId, qkbs in result.iteritems():
            for qkb in qkbs:
                k = '%s,%s'%(skuId, qkb[0])
                if k in resAll:
                    continue
                link = 'https://m.miyabaobei.com/wx/group_detail/%s.html'%qkb[0]
                ctn = '%s\t%s\t%s\t%s\n'%(skuId, qkb[0], qkb[1], link)
                fp.write(ctn)
                fpAll.write(ctn)
        fp.close()
        fpAll.close()
        self.checkPoint = int(today.strftime('%Y%m%d'))
        return []

class TargetUser(Editor):
    seg = [0,9]
    tmpIdx = 0
    def loadData(self):
        today = datetime.date.today()
        yday = today - datetime.timedelta(days=1)
        date = today - datetime.timedelta(days=7)
        if self.checkPoint == int(today.strftime('%Y%m%d')):
            return []
        tableName = 'sub%s_%s'%(self.seg[0], today.strftime('%Y%m%d'))
        cmd = "mysql -h 10.1.106.1 -upostpush -p'postpushqwer!@#z%%' postpush -e 'create table if not exists %s (id int(11) unsigned not null auto_increment, user_id int(11) unsigned not null, url varchar(128) not null, content varchar(256) not null, primary key(id), key uid(user_id))ENGINE=MyISAM DEFAULT CHARSET=utf8;'"%tableName
        os.system(cmd)
        self.pushTab = getOrmModel('pushtable', tableName)
        self.rds = redis.StrictRedis(host = '10.1.52.187')
        self.policy = {}
        self.pushCount = 0
        info = {}
        count = 0
        while True:
            dateStr = date.strftime('%Y%m%d')
            path = 'data/sku-event-%s/'%dateStr
            if not os.path.exists(path):
                os.mkdir(path)
            if not os.path.exists(path+'part-00000'):
                os.system('hadoop fs -get /search/parsed_data/%s/sku/part* %s'%(dateStr, path))
            if not os.path.exists(path+'part-00000'):
                return []
            dvcSet = set()
            for fname in os.listdir(path):
                if fname[:4] != 'part' or fname.find('.') > 0:
                    continue
                logging.info('Process %s'%(path+fname))
                for line in open(path+fname):
                    li = line.split(', ')
                    dvcId = li[0][3:-1]
                    hint = zlib.crc32(dvcId)&0xffffffff
                    left = hint % 10
                    if left < self.seg[0] or left > self.seg[1]:
                        continue
                    ts = int(li[2][2:-1])/1000
                    skuId = li[4][2:-1]
                    typ = int(li[5])
                    if typ in (1, 2, 4):
                        if dvcId not in info:
                            info[dvcId] = {'active':0}
                        if skuId not in info[dvcId]:
                            count += 1
                            info[dvcId][skuId] = [0, 0, 0]
                        dt = datetime.datetime.fromtimestamp(ts)
                        info[dvcId][skuId][typ/2] += 1
                    if dvcId in info and dvcId not in dvcSet:#count user active days
                        info[dvcId]['active'] += 1
                        dvcSet.add(dvcId)
            if date == yday:
                break
            date += datetime.timedelta(days=1)
        path = 'data/quality-koubei.csv'
        self.quality = {}
        for line in open(path):
            li = line[:-1].split(',')
            skuId = li[0]
            if skuId not in self.quality:
                self.quality[skuId] = [1]
            self.quality[skuId].append((li[1], li[4].decode('gbk')))
        path = 'data/article-text.csv'
        self.article = {}
        for line in open(path):
            li = line[:-1].split(',')
            pid = int(li[0])
            self.article[pid] = li[1].decode('gbk')
        mapping = {}
        for line in open('/opt/dm_rec/data_mining/data/id_mapping'):
            li = line[:-1].split('\t')
            try:
                mapping[li[0]] = int(li[1])
            except:
                pass
        self.ageInfo = {}
        for line in open('/opt/parsed_data/demography/USER_result.txt'):
            li = json.loads(line)
            dvcId = li['uid']
            age = li['baby_age_month']
            if dvcId not in mapping or age == -1:
                continue
            self.ageInfo[mapping[dvcId]] = age
        orderDate = today - datetime.timedelta(days=14)
        dateStr = orderDate.strftime('%Y%m%d')
        count = {'processed':0, 'mapError':0, 'succ':0, 'fail':0, 'repeat':0, 'total':len(info)}
        for dvcId, eles in info.iteritems():
            count['processed'] += 1
            if count['processed']%1000 == 0:
                logging.info('Progress %s'%str(count))
                self.heartbeat()
            if dvcId not in mapping:
                count['mapError'] += 1
                continue
            uid = mapping[dvcId]
            active = eles.pop('active')
#            if active < 3:#no push to unactive user
#                continue
            mdls = Order.select(Order.id, Order.user_id, OrderItems.item_id).join(OrderItems, on=(Order.id==OrderItems.order_id).alias('item')).where((Order.order_time>dateStr)&(Order.status>1)&(Order.user_id==uid))
            for mdl in mdls:
                skuId = str(mdl.item.item_id)
                if skuId in eles:
                    del eles[skuId]
            res = 0
            for skuId, action in eles.iteritems():
                res = self.policyPush(uid, skuId, action, dvcId, 'quality')
                if res < 2:
                    break
            if res == 2:#if not push succ
                res = self.policyPush(uid, skuId, action, dvcId, 'recommend')
            if res == 1:#succ
                count['succ'] += 1
            elif res == -1:#have pushed today
                count['repeat'] += 1
            else:#recommend push fail
                count['fail'] += 1
        self.checkPoint = int(today.strftime('%Y%m%d'))
        #commit push task
        nowStr = datetime.datetime.now().strftime('%Y-%m-%d%%20%H:%M:%S')
        url = 'http://msg.miyabaobei.com/apipush/create?title=postpush&execute_time=%s&user_type=2&mysql_type=1&user_value=postpush.%s&checkauth=Auth20170427forCrm'%(nowStr, tableName)
        resp = requests.get(url, timeout=3)
        logging.info('Pushed %s'%self.pushCount)
        return []

    def getPushFromQuality(self, uid, skuId):
        pid = 0
        text = ''
        goodPosts = self.quality[skuId]
        for i in range(len(goodPosts)-1):
            idx = goodPosts[0]
            pid = goodPosts[idx][0]
            mdl = PushLog.select().where((PushLog.uid==uid)&(PushLog.pid==pid))
            if len(mdl) > 0:#have pushed
                continue
            text = goodPosts[idx][1]
            goodPosts[0] = idx = idx+1
            if idx == len(goodPosts):
                goodPosts[0] = idx = 1
            break
        return pid, text
    
    def getPushFromRec(self, uid, dvcId, dateStr):
        postid = 0
        text = ''
        idx = 0
        try:
            url = 'http://content.rec.mia.com/recommend_result?did=%s&tp=8&pagesize=10&pressure=1'%dvcId
            resp = requests.get(url, timeout=3)
            eles = json.loads(resp.content)
            url = 'http://content.rec.mia.com/recommend_result?did=%s&sessionid=push%s&pagesize=20&pressure=1&forpush=1'%(dvcId, dateStr)
            resp = requests.get(url, timeout=3)
            eles2 = json.loads(resp.content)
            eles['pl_list'] += eles2['pl_list']
        except:
            return postid, idx, text
        maxTextLen = 0
        maxTextSub = None
        for ele in eles['pl_list']:
            pid = int(ele['id'])
            mdl = PushLog.select().where((PushLog.uid==uid)&(PushLog.pid==pid))
            if len(mdl) > 0:#have pushed
                continue
            try:
                sub = Subject.select().where((Subject.id==pid)&(Subject.status==1)).get()
                textLen = 0
                for c in sub.text:
                    if ord(c) > 128:
                        textLen += 1
            except:
                continue
            if textLen > maxTextLen:
                maxTextLen = textLen
                maxTextSub = sub
            extInfo = json.loads(sub.ext_info)
            if 'is_blog' in extInfo and extInfo['is_blog']==1:
                if pid in self.article:
                    return sub.id, 10002, self.article[pid]
                else:
                    return sub.id, 10001, sub.title
            if textLen > 100:
                break
        try:
            postid = maxTextSub.id
            mdl = SubTag.select().where((SubTag.subject_id==postid)&(SubTag.item_id>0)).get()
            mdl = RelateSku.select().where(RelateSku.id==mdl.item_id).get()
            m2 = ItemCatgy.select().where(ItemCatgy.id==mdl.category_id).get()
            catgyId = int(m2.path.split('-')[0])
            if mdl.activity_short_title:
                name = mdl.activity_short_title
            else:
                m1 = ItemBrand.select().where(ItemBrand.id==mdl.brand_id).get()
                name = m1.chinese_name+m2.name
            if uid in self.ageInfo:
                varDict = {'name':name, 'age':getAgeDesc(self.ageInfo[uid])}
            else:
                varDict = {'name':name}
            idx, text = getTextFromTemplate(self.tmpIdx, catgyId, varDict)
            self.tmpIdx += 1
        except:
            pass
        return postid, idx, text

    def policyPush(self, uid, skuId, action, dvcId, source):
        tmpIdx = 0
        today = datetime.date.today()
        dateStr = today.strftime('%Y%m%d')
        mdl = PushLog.select().where((PushLog.uid==uid)&(PushLog.created>dateStr))
        if len(mdl) > 0:#have pushed today
            return -1
        if source == 'quality':
            if skuId not in self.quality:
                return 2
            tmpIdx = 10000
            pid, text = self.getPushFromQuality(uid, skuId)
            if not pid or not text:
                return 2#next skuId
        else:
            pid, tmpIdx, text = self.getPushFromRec(uid, dvcId, dateStr)
            if not pid or not text:
                return 0

        dateStr = today.strftime('%Y%m%d')
        link = 'miyabaobei://subject?id=%s&push=personalized_post-%s-%s'%(pid, dateStr, int(time.time()))
#        info = {
#            'user_id':int(uid),
#            'content':text,
#            'url':link
#        }
#        self.rds.lpush('app_custom_push_list', json.dumps(info))
        pushInto(PushLog, {'uid':uid, 'skuid':skuId,'pid':pid, 'dvcid':dvcId, 'action':str(action), 'content':text, 'source':source, 'tmpIdx':tmpIdx})
        pushInto(self.pushTab, {'user_id':uid, 'content':text, 'url':link})
        self.pushCount += 1
        return 1

class TargetUser1(TargetUser): seg = [0,4]
class TargetUser2(TargetUser): seg = [5,9]

class PushStat(Editor):
    def loadData(self):
        pushCtr = {}
        today = datetime.date.today()
        yday = today - datetime.timedelta(days=1)
        mdls = PushLog.select().where((PushLog.created>yday.strftime('%Y-%m-%d'))&(PushLog.created<today.strftime('%Y-%m-%d')))
        for mdl in mdls:
            if mdl.pid not in pushCtr:
                #expose ios, android, click ios, android, ctr ios, android, total ctr
                pushCtr[mdl.pid] = [0, 0, 0, 0, 0, 0, 0, mdl.content]
            if mdl.dvcid.find('-') > 0:
                pushCtr[mdl.pid][0] += 1
            else:
                pushCtr[mdl.pid][1] += 1
        path = '/opt/parsed_data/personalized_post/%s/push_clicked_format.txt'%yday.strftime('%Y%m%d')
        for line in open(path):
            li = line.split('\t')
            dvcId = li[1]
            pid = int(li[2])
            if pid not in pushCtr:
                pushCtr[pid] = [0, 0, 0, 0, 0, 0, 0, '']
            if dvcId.find('-') > 0:
                pushCtr[pid][2] += 1
            else:
                pushCtr[pid][3] += 1

        fp = open('data/pushstat-%s.csv'%yday.strftime('%Y%m%d'),'w')
        for pid, stat in pushCtr.iteritems():
            stat[4] = calcCrt(stat[2], stat[0])
            stat[5] = calcCrt(stat[3], stat[1])
            stat[6] = calcCrt(stat[2]+stat[3], stat[0]+stat[1])
            ctn = '%s,%s\n'%(pid, ','.join(map(str, stat[:7])))
            fp.write(ctn)
        fp.close()
        return []
import zlib
class Abtest(Editor):
    def loadData(self):
        today = datetime.date.today()
        yday = today - datetime.timedelta(days=1)
        date = yday - datetime.timedelta(days=1)
        while True:
            path = 'data/click-%s/part-00000'%date.strftime('%Y%m%d')
            clicka = 0
            clickb = 0
            dvcSeta = set()
            dvcSetb = set()
            for line in open(path):
                idx = line.find("',")
                li = line[3:idx].split('_')
                num = line[idx+2:-2]
                pid = li[0]
                _type = li[1]
                dvcId = li[2]
                if _type != 'koubeilist':
                    continue
                hint = zlib.crc32(dvcId)&0xffffffff
                left = hint % 10
                if left < 3:
                    dvcSeta.add(dvcId)
                    clicka += int(num)
                else:
                    dvcSetb.add(dvcId)
                    clickb += int(num)

            avga = round(float(clicka)/len(dvcSeta), 4)
            avgb = round(float(clickb)/len(dvcSetb), 4)
            print date.strftime('%Y%m%d'), clicka, clickb, avga, avgb, round((avgb-avga)/avga, 4), round((clickb/7 - clicka/3)/float(clicka/3), 4)
            if date == yday:
                break
            date += datetime.timedelta(days=1)
        return []

class TopStat(Editor):
    def loadData(self):
        fp = open('data/topkoubeisku.csv', 'w')
        mdls = Koubei.select(Koubei.item_id, fn.COUNT(Koubei.id).alias('kbcount')).group_by(Koubei.item_id).where(Koubei.item_id>0).order_by(fn.COUNT(Koubei.id).desc()).limit(2000)
        for mdl in mdls:
            catgy = ''
            name = ''
            try:
                m1 = RelateSku.select().where(RelateSku.id==mdl.item_id).get()
                m2 = ItemCatgy2.select().where(ItemCatgy2.id==m1.category_id_ng).get()
                path = m2.path.split('-')
                m2 = ItemCatgy2.select().where(ItemCatgy2.id==path[1]).get()
                name = m1.name
                catgy = m2.name
            except:
                pass
            fp.write('%s, %s, %s, %s\n'%(mdl.item_id, name, catgy, mdl.kbcount))
        fp.close()
        return []
class TopStat2(Editor):
    def loadData(self):
        #level1Catgy = [10011,15512,10006,10017,15511,15509]
        level1Catgy = [10026]
        catgySet = set()
        for catgy in level1Catgy:
            like = str(catgy)+'%'
            mdls = ItemCatgy2.select().where(ItemCatgy2.path%like)
            for mdl in mdls:
                for catgy in mdl.path.split('-'):
                    catgySet.add(int(catgy))
        skuDict = {}
        mdls = RelateSku.select(RelateSku.id, RelateSku.name).where(RelateSku.category_id_ng<<list(catgySet))
        for mdl in mdls:
            skuDict[mdl.id] = [mdl.name, 0]
        for skuId, stat in skuDict.iteritems():
            mdl = Koubei.select(fn.COUNT(Koubei.id).alias('kbcount')).where(Koubei.item_id==skuId).get()
            skuDict[skuId][1] = mdl.kbcount
        ranked = sorted(skuDict.iteritems(), key=lambda x:x[1][1], reverse=True)
        fp = open('data/topkoubeisku-%s.csv'%level1Catgy[0], 'w')
        for skuId, stat in ranked[:2000]:
            fp.write('%s, %s, %s\n'%(skuId, stat[0], stat[1]))
        fp.close()
        return []
class TopStat3(Editor):
    batchSize = 10000
    userDict = {}
    def loadCheckPoint(self):
        self.checkPoint = 0
    def loadData(self):
        return Subject.select().where((Subject.id>self.checkPoint)&(Subject.created>'20170101')&(Subject.status==1)).order_by(Subject.id).limit(self.batchSize)
    def edit(self, model):
        if model.user_id not in self.userDict:
            self.userDict[model.user_id] = 0
        self.userDict[model.user_id] += 1
        return 1
    def finish(self):
        addrStat = {}
        for uid, count in self.userDict.iteritems():
            try:
                addr = Address.select().where((Address.user_id==uid)&(Address.is_default==1)).get()
                if addr.prov not in addrStat:
                    prov = Prov.select().where(Prov.id==addr.prov).get()
                    addrStat[addr.prov] = [prov.name, 0]
                addrStat[addr.prov][1] += count
            except:
                continue
        ranked = sorted(addrStat.iteritems(), key=lambda x:x[1][1], reverse=True)
        fp = open('data/topkoubeiprov.csv', 'w')
        for provid, stat in ranked:
            fp.write('%s, %s, %s\n'%(provid, stat[0], stat[1]))
        fp.close()
                
class UserAddr(Editor):
    def loadData(self):
        fp = open('data/useraddr.csv', 'w')
        for line in open('data/tmp.txt'):
            uid = int(line[:-1])
            try:
                mdl = User.select().where(User.id==uid).get()
                addr = Address.select().where((Address.user_id==uid)&(Address.is_default==1)).get()
                prov = Prov.select().where(Prov.id==addr.prov).get()
                city = City.select().where(City.id==addr.city).get()
                area = Area.select().where(Area.id==addr.area).get()
                fp.write('%s, %s, %s, %s, %s, %s, %s, %s, %s\n'%(uid, mdl.username, mdl.nickname, mdl.cell_phone, addr.name, prov.name, city.name, area.name, addr.address))
            except:
                continue
        fp.close()
        title = '【用户信息】'
        addr = ['yandechen@mia.com']
        mail = EmailUtil('exmail.qq.com', 'miasearch@mia.com', 'HelloJack123')
        mail.sendEmail(addr, title, files=['data/useraddr.csv'])
        return []

from picview import *
class PicView(Editor):
    def loadData(self):
        path = '/opt/parsed_data/text_features_4_index/index_terms_input.txt'
        today = datetime.date.today()
        if self.checkPoint == int(today.strftime('%Y%m%d')):
            return []
        count = {'succ':0, 'total':0}
        for line in open(path):
            li = line.split('\t')
            pid = li[1]
            count['total'] += 1
            try:
                mdl = PicFeature.select().where((PicFeature.pid==pid)&(PicFeature.idx==1000)).get()
                continue
            except:
                count['succ'] += 1
                if count['succ']%1000 == 0:
                    logging.info('Processed %s'%str(count))
            mdl = Subject.select().where(Subject.id==pid).get()
            extInfo = None
            if mdl.ext_info:
                extInfo = json.loads(mdl.ext_info)
            if extInfo and 'cover_image' in extInfo:
                cover = extInfo['cover_image']['url']
            elif mdl.image_url:
                cover = mdl.image_url.split('#')[0]
            else:
                continue
            link = 'http://img05.miyabaobei.com/'+cover
            clarity, bright = getPicFeatureFromUrl(link)
            scoreBright = 1-abs(bright-150)/150.0
            scoreClarity = min(100,clarity)/100.0
            score = round(scoreBright*0.7+scoreClarity*0.3, 2)*100
            pushInto(PicFeature, {'pid':pid, 'idx':1000, 'bright':bright, 'clarity':clarity, 'score':score}, ['pid', 'idx'])

        path = '/opt/parsed_data/picture/feature.txt'
        mdls = PicFeature.select().where(PicFeature.score>0)
        fp = open(path, 'w')
        for mdl in mdls:
            fp.write('%s\t%s\t%s\t%s\n'%(mdl.pid, mdl.bright, mdl.clarity, mdl.score))
        fp.close()
        self.checkPoint = int(today.strftime('%Y%m%d'))
        return []

class GroupSku(Editor):
    batchSize = 1000
    clickInfo = {}
    catgyStat = {}
    def loadInitData(self):
        self.checkPoint = 0
#        today = datetime.date.today()
#        for day in range(1,31):
#            date = today - datetime.timedelta(days=day)
#            path = 'data/click-%s/part-00000'%date.strftime('%Y%m%d')
#            for line in open(path):
#                idx = line.find("',")
#                li = line[3:idx].split('_')
#                num = line[idx+2:-2]
#                pid = int(li[0])
#                if pid not in self.clickInfo:
#                    self.clickInfo[pid] = 0
#                self.clickInfo[int(pid)] += 1
    def loadData(self):
        return Subject.select().where((Subject.id>self.checkPoint)&(Subject.status==1)&(Subject.source==1)&(Subject.created>'20170401')&(Subject.created<'20170801')).order_by(Subject.id).limit(self.batchSize)
    def edit(self, model):
        try:
            mdl1 = SubTag.select().where((SubTag.subject_id==model.id)&(SubTag.item_id>0)).get()
            mdl2 = RelateSku.select().where(RelateSku.id==mdl1.item_id).get()
            mdl3 = ItemCatgy.select().where(ItemCatgy.id==mdl2.category_id).get()
            c1 = mdl3.path.split('-')[0]
            mdl4 = ItemCatgy.select().where(ItemCatgy.id==c1).get()
        except:
            return 0
        if mdl4.name not in self.catgyStat:
            self.catgyStat[mdl4.name] = [0, 0]
        self.catgyStat[mdl4.name][0] += 1
        textLen = 0
        for c in model.text:
            if ord(c) > 128:
                textLen += 1
        picNum = len(model.image_url.split('#'))
        if textLen>99 and picNum>2:
            self.catgyStat[mdl4.name][1] += 1
#        if model.id in self.clickInfo:
#            self.catgyStat[mdl4.name][2] += self.clickInfo[model.id]
        return 1
    def finish(self):
        path = 'data/group-catgy-stat.csv'
        fp = open(path, 'w')
        for name, stat in self.catgyStat.iteritems():
            fp.write('%s, %s, %s\n'%(name, stat[0], stat[1]))
        fp.close()
        title = '【蜜芽圈统计】'
        addr = ['yandechen@mia.com']
        mail = EmailUtil('exmail.qq.com', 'miasearch@mia.com', 'HelloJack123')
        mail.sendEmail(addr, title, files=[path])

import xml.etree.ElementTree as ET
class Dump4Baidu(Editor):
    xmlPrefix = 'xml4baidu'
    def loadInitData(self):
        self.eleCount = 0
        self.root = ET.Element('urlset')
        self.root.set('content_method', 'full')
    def loadData(self):
        return SubTag.select().where((SubTag.id>self.checkPoint)&(SubTag.subject_id>0)).order_by(SubTag.id).limit(self.batchSize)
        
    def edit(self, tag):
        try:
            model = Subject.select().where((Subject.id==tag.subject_id)&(Subject.status==1)).get()
            if model.semantic_analys < 2:
                return 0
            textLen = 0
            for c in model.text:
                if ord(c) > 128:
                    textLen += 1
            if textLen < 50:
                return 0
            extInfo = None
            if model.ext_info:
                extInfo = json.loads(model.ext_info)
            if extInfo and 'cover_image' in extInfo:
                cover = extInfo['cover_image']['url']
            elif model.image_url:
                cover = model.image_url.split('#')[0]
            else:
                return 0
            cover = cover if cover[0]!='/' else cover[1:]
            cover = 'http://img05.miyabaobei.com/'+cover
            mdl = RelateSku.select().where(RelateSku.id==tag.item_id).get()
            m1 = ItemBrand.select().where(ItemBrand.id==mdl.brand_id).get()
            m2 = ItemCatgy.select().where(ItemCatgy.id==mdl.category_id).get()
            sku = mdl.name
            title = model.title
            if not title:
                title = m1.chinese_name+m2.name
            mdl = User.select().where(User.id==model.user_id).get()
            username = mdl.nickname
            if not username:
                username = mdl.username
            kword = ''
            if m1.chinese_name:
                kword += m1.chinese_name
            if m2.name:
                kword += m2.name
        except:
            return 0
        link = 'https://m.miyabaobei.com/wx/group_detail/%s.html'%model.id
        cdate = model.created.strftime('%Y-%m-%d')
        self.setXmlItemFrame(link, title, model.id, cdate, username, cover, model.text, kword, m1.chinese_name, sku)

        self.eleCount += 1
        if self.eleCount % 5000 == 0:
            tree = ET.ElementTree(self.root)
            tree.write('data/%s-%s.xml'%(self.xmlPrefix, self.checkPoint), 'utf-8', True)
            self.root = ET.Element('urlset')
            self.root.set('content_method', 'full')
        return 1

    def setXmlItemFrame(self, link, title, workId, cdate, uname, cover, text, kword, bname, sku):
        ele = ET.SubElement(self.root, 'url')
        ET.SubElement(ele, 'loc').text = link
        data = ET.SubElement(ele, 'data')
        disp = ET.SubElement(data, 'display')
        ET.SubElement(disp, 'workId').text = str(workId)
        ET.SubElement(disp, 'wapUrl').text = link
        ET.SubElement(disp, 'originalUrl').text = link
        ET.SubElement(disp, 'headline').text = title
        ET.SubElement(disp, 'datePublished').text = cdate
        provider = ET.SubElement(disp, 'provider')
        ET.SubElement(provider, 'brand').text = '蜜芽'
        author = ET.SubElement(disp, 'author')
        ET.SubElement(author, 'name').text = uname
        ET.SubElement(author, 'tag').text = '普通用户'
        ET.SubElement(author, 'fansCount').text = '0'
        ET.SubElement(author, 'articleCount').text = '0'
        ET.SubElement(disp, 'thumbnailUrl').text = cover
        paragraph = ET.SubElement(disp, 'paragraph')
        ET.SubElement(paragraph, 'contentType').text = 'text'
        ET.SubElement(paragraph, 'text').text = text
        ET.SubElement(disp, 'category').text = '母婴玩具'
        ET.SubElement(disp, 'genre').text = '心得'
        ET.SubElement(disp, 'keywords').text = kword
        prod = ET.SubElement(disp, 'associatedProduct')
        ET.SubElement(prod, 'brand').text = bname
        ET.SubElement(prod, 'spu').text = sku
        ET.SubElement(disp, 'scanCount').text = '0'
        ET.SubElement(disp, 'thumbupCount').text = '0'
        ET.SubElement(disp, 'thumbdownCount').text = '0'
        ET.SubElement(disp, 'commentCount').text = '0'
        ET.SubElement(disp, 'shareCount').text = '0'
        ET.SubElement(disp, 'collectCount').text = '0'
        return prod

    def finish(self):
        tree = ET.ElementTree(self.root)
        tree.write('data/%s-%s.xml'%(self.xmlPrefix, self.checkPoint), 'utf-8', True)
        self.root = ET.Element('urlset')
        self.root.set('content_method', 'full')

        #gen sitemap
        path = 'data/'
        indexpath = path + self.xmlPrefix + '-sitemap.xml'
        if os.path.exists(indexpath):
            tree = ET.parse(indexpath)
            root = tree.getroot()
        else:
            root = ET.Element('sitemapindex')
        for fname in os.listdir(path):
            if fname.find('-sitemap')>0 or fname.find(self.xmlPrefix)<0:
                continue
            xmlstr = re.sub(u"[\x00-\x08\x0b-\x0c\x0e-\x1f]+",u"",open(path+fname).read().decode())
            fp = open(path+fname, 'w')
            fp.write(xmlstr)
            fp.close()
            pubLink = uploadFile(path, fname)
            if pubLink:
                logging.info('Upload %s succ.'%fname)
                smap = ET.SubElement(root, 'sitemap')
                loc = ET.SubElement(smap, 'loc')
                loc.text = pubLink
            else:
                logging.info('Upload %s fail.'%fname)
        os.system('mv data/%s-*.xml data/archive/'%self.xmlPrefix)
        fname = self.xmlPrefix + '-sitemap.xml'
        tree = ET.ElementTree(root)
        tree.write(path+fname)

        #upload sitemap index
        pubLink = uploadFile(path, fname)
        if pubLink:
            logging.info('Sitemap for baidu succ.')
        else:
            logging.info('Sitemap for baidu fail.')

class Prom4Baidu(Dump4Baidu):
    xmlPrefix = 'prom4baidu'
    def loadInitData(self):
        Dump4Baidu.loadInitData(self)
        os.system('rm data/%s-*.xml'%self.xmlPrefix)
        self.checkPoint = 0
    def loadData(self):
        dateStr = datetime.date.today().strftime('%Y%m%d')
        return Prom.select(Prom.id, Prom.create_time, Prom.intro, Prom.exten, Prom.end_time).where((Prom.id>self.checkPoint)&(Prom.end_time>dateStr)).order_by(Prom.id).limit(self.batchSize)
    
    def edit(self, model):
        mdls = PromItem.select().where((PromItem.promotion_id==model.id)&(PromItem.status==0))
        for m in mdls:
            try:
                mdl = RelateSku.select().where(RelateSku.id==m.sku).get()
                m1 = ItemBrand.select().where(ItemBrand.id==mdl.brand_id).get()
                m2 = ItemCatgy.select().where(ItemCatgy.id==mdl.category_id).get()
                kword = model.exten
                title = mdl.activity_short_title
                if not title:
                    title = m1.chinese_name+m2.name
                m3 = ItemPic.select().where((ItemPic.item_id==mdl.id)&(ItemPic.type=='topic')).get()
                cover = 'https://img01.miyabaobei.com/'+m3.local_url
            except Exception, e:
                return 0
            link = 'https://m.mia.com/item-%s.html'%mdl.id
            cdate = model.create_time.strftime('%Y-%m-%d')
            prod = self.setXmlItemFrame(link, title, m.id, cdate, '', cover, mdl.name_added, kword, m1.chinese_name, mdl.name)
            offer = ET.SubElement(prod, 'offers')
            seller = ET.SubElement(offer, 'seller')
            ET.SubElement(seller, 'name').text = '蜜芽'
            price = ET.SubElement(offer, 'pricespecification')
            ET.SubElement(price, 'url').text = 'https://m.mia.com/promotion-%s.html'%model.id
            ET.SubElement(price, 'description').text = model.intro
            ET.SubElement(price, 'validThrough').text = model.end_time.strftime('%Y-%m-%dT%H:%M:%S')
            ET.SubElement(price, 'price').text = str(m.active_price)
            self.eleCount += 1
            if self.eleCount % 5000 == 0:
                tree = ET.ElementTree(self.root)
                tree.write('data/%s-%s.xml'%(self.xmlPrefix, self.checkPoint), 'utf-8', True)
                self.root = ET.Element('urlset')
                self.root.set('content_method', 'full')
        return 1

    def setXmlItemFrame(self, link, title, workId, cdate, uname, cover, text, kword, bname, sku):
        ele = ET.SubElement(self.root, 'url')
        ET.SubElement(ele, 'loc').text = link
        data = ET.SubElement(ele, 'data')
        disp = ET.SubElement(data, 'display')
        ET.SubElement(disp, 'workId').text = str(workId)
        ET.SubElement(disp, 'wapUrl').text = link
        ET.SubElement(disp, 'headline').text = title
        ET.SubElement(disp, 'datePublished').text = cdate
        ET.SubElement(disp, 'dateModified').text = cdate
        provider = ET.SubElement(disp, 'provider')
        ET.SubElement(provider, 'brand').text = '蜜芽'
        author = ET.SubElement(disp, 'author')
        ET.SubElement(author, 'name').text = uname
        ET.SubElement(author, 'tag').text = '编辑'
        ET.SubElement(disp, 'thumbnailUrl').text = cover
        paragraph = ET.SubElement(disp, 'paragraph')
        ET.SubElement(paragraph, 'contentType').text = 'text'
        ET.SubElement(paragraph, 'text').text = text
        key = ET.SubElement(disp, 'keywords').text = kword
        prod = ET.SubElement(disp, 'associatedProduct')
        ET.SubElement(prod, 'brand').text = bname
        ET.SubElement(prod, 'spu').text = sku
        ET.SubElement(disp, 'scanCount').text = '0'
        ET.SubElement(disp, 'thumbupCount').text = '0'
        ET.SubElement(disp, 'thumbdownCount').text = '0'
        ET.SubElement(disp, 'commentCount').text = '0'
        ET.SubElement(disp, 'shareCount').text = '0'
        ET.SubElement(disp, 'collectCount').text = '0'
        return prod
    
class MlibSample(Editor):
    def loadData(self):
        today = datetime.date.today()
        aSample  = {}
        bSample = {}
        stage1 = {}
        stage2 = {}
        yday = today - datetime.timedelta(days=1)
        mid = today - datetime.timedelta(days=15)
        date = yday - datetime.timedelta(days=30)
        while True:
            if date < mid:
                stage = stage1
            else:
                stage = stage2
            dateStr = date.strftime('%Y%m%d')
            logging.info('Process %s'%dateStr)
            path = '/opt/parsed_data/ctr/kblist-%s'%dateStr
            for line in open(path):
                li = line.split('\t')
                pid = int(li[0])
                click = int(li[1])
                expose = int(li[2])
                if pid not in stage:
                    stage[pid] = [0, 0]
                stage[pid][0] += click
                stage[pid][1] += expose
            if date == yday:
                break
            date += datetime.timedelta(days=1)

        count = 0
        inter = [x for x in stage1 if x in stage2]
        for i, pid in enumerate(inter):
            if count % 1000 == 0:
                logging.info('Processing %s/%s'%(count, len(stage1)))
                info = {}
                mdls1 = Koubei.select(Koubei.subject_id, Koubei.score).where(Koubei.subject_id<<inter[i:i+1000])
                mdls2 = Subject.select(Subject.id, Subject.semantic_analys, Subject.text, Subject.image_url).where(Subject.id<<inter[i:i+1000])
                for mdl in mdls1:
                    info[mdl.subject_id] = [mdl.score]
                for mdl in mdls2:
                    if mdl.id not in info:
                        continue
                    info[mdl.id].append(mdl.semantic_analys)
                    info[mdl.id].append(mdl.text)
                    info[mdl.id].append(mdl.image_url)
            count += 1
            if pid not in info:
                continue
            uscore = info[pid][0]
            mscore = info[pid][1]
            if uscore > 0 and uscore < 4:
                continue
            if mscore > 0 and mscore < 2:
                continue
            text = info[pid][2]
            textLen = 0
            for c in text:
                if ord(c) > 128:
                    textLen += 1
            pics = info[pid][3]
            picNum = 0
            if pics:
                picNum = len(pics.split('#'))
            fpic = round(picNum/float(9), 4)
            ftext = round(min(textLen/20, 20)/float(20),4)
            typ = int(bool(stage2[pid][0]))
            if stage1[pid][0] > 0:#aSample
                ctr = round(calcCrt(stage1[pid][0], stage1[pid][1]),4)
                aSample[pid] = [typ, int(bool(picNum)), fpic, ftext, ctr]
            else:#bSample
                bSample[pid] = [typ, int(bool(picNum)), fpic, ftext]
                
        fp = open('data/asample.txt', 'w')
        sampleLen = min(len([x for x in aSample if aSample[x][0]==0]), len([x for x in aSample if aSample[x][0]==1]))
        posCount = 0
        negCount = 0
        for pid, flist in aSample.iteritems():
            if flist[0] == 1:
                count = posCount
                posCount += 1
            else:
                count = negCount
                negCount += 1
            if count < sampleLen:
                fp.write('%s\t%s\n'%(pid,'\t'.join([str(x) for x in flist])))
        fp.close()

        sampleLen = min(len([x for x in bSample if bSample[x][0]==0]), len([x for x in bSample if bSample[x][0]==1]))
        posCount = 0
        negCount = 0
        fp = open('data/bsample.txt', 'w')
        for pid, flist in bSample.iteritems():
            if flist[0] == 1:
                count = posCount
                posCount += 1
            else:
                count = negCount
                negCount += 1
            if count < sampleLen:
                fp.write('%s\t%s\n'%(pid,'\t'.join([str(x) for x in flist])))
        fp.close()
        return []
