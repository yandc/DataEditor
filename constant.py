#!/user/bin/env python
# coding=utf-8

cities = set([
    "中国", "北京","上海","广州","深圳","杭州",
    "重庆","武汉","成都","天津",
    "广东", "山东", "江苏", "河南", "河北", "浙江", "香港", "陕西", "湖南", "福建", "云南", "四川", "广西", "安徽", "海南", "江西", "湖北", "山西", "辽宁", "台湾", "黑龙江", "内蒙古", "澳门", "贵州", "甘肃", "青海", "新疆", "西藏", "吉林", "宁夏",
    "长沙","南昌","福州","贵阳","昆明","南宁","南京","合肥","郑州","太原","西安","兰州","西宁","乌鲁木齐","呼和浩特","济南","石家庄","哈尔滨","长春","沈阳","厦门","海口","拉萨","银川市",
    "东莞市","珠海","汕头","佛山","韶关","河源","梅州","惠州","汕尾","中山","江门","阳江","湛江","茂名","肇庆","清远","潮州","揭阳","云浮",
    "岳阳","张家界","常德","益阳","湘潭","株洲","娄底","怀化","邵阳","衡阳","永州","郴州",
    "十堰","襄樊","随州","荆门","孝感","宜昌","黄冈","鄂州","荆州","黄石","咸宁",
    "九江","景德镇","上饶","鹰潭","抚州","新余","宜春","萍乡","吉安","赣州",
    "宁德","南平","三明","莆田","龙岩","泉州","漳州",
    "三亚",
    "广元","巴中","绵阳","德阳","达州","南充","遂宁","广安","资阳","眉山","雅安","内江","乐山","自贡","泸州","宜宾","攀枝花",
    "遵义","六盘水","安顺",
    "昭通","丽江","曲靖","保山","玉溪","临沧","普洱",
    "桂林","河池","贺州","柳州","百色","来宾","梧州","贵港","玉林","崇左","钦州","防城港","北海",
    "日喀则",
    "连云港","徐州","宿迁","淮安","盐城","泰州","扬州","镇江","南通","常州","无锡","苏州",
    "湖州","嘉兴","绍兴","舟山","宁波","金华","衢州","台州","丽水","温州",
    "淮北","亳州","宿州","蚌埠","阜阳","淮南","滁州","六安","马鞍山","巢湖","芜湖","宣城","铜陵","池州","安庆","黄山",
    "安阳","鹤壁","濮阳","新乡","焦作","三门峡","开封","洛阳","商丘","许昌","平顶山","周口","漯河","南阳","驻马店","信阳",
    "大同","朔州","忻州","阳泉","晋中","吕梁","长治","临汾","晋城","运城",
    "榆林","延安","铜川","渭南","宝鸡","咸阳","商洛","汉中","安康",
    "嘉峪关","酒泉","张掖","金昌","武威","白银","庆阳","平凉","定西","天水","陇南",
    "石嘴山市","吴忠市","中卫市","固原市",
    "克拉玛依"
    "包头","乌海","赤峰","通辽","鄂尔多斯","呼伦贝尔","巴彦淖尔","乌兰察布",
    "德州","滨州","东营","烟台","威海","淄博","潍坊","聊城","泰安","莱芜","青岛","日照","济宁","菏泽","临沂","枣庄",
    "张家口","承德","唐山","秦皇岛","廊坊","保定","沧州","衡水","邢台","邯郸",
    "黑河","伊春","齐齐哈尔","鹤岗","佳木斯","双鸭山","绥化","大庆","七台河","鸡西","牡丹江",
    "白城","松原","吉林","四平","辽源","白山","通化",
    "铁岭","阜新","抚顺","朝阳","本溪","辽阳","鞍山","盘锦","锦州","葫芦岛","营口","丹东","大连"
    ])

abstract_info = {'homepage':u'【项目网站】', 'description':u'【项目描述】', 'stage':u'【融资阶段】', 'founder':u'【创始人】', 'fullname':u'【公司全称】', 'phone':u'【电话】', 'mail':u'【邮箱】', 'address':u'【地址】', 'brief':u'【简介】', 'business_status':u'【运营状态】', 'wechat':'【微信】', 'history':'【历史】', 'property':'【公司性质】', 'foundTime':'【成立时间】'}

rounds = {'尚未获投':'尚未获投', '不明确':'不明确', '不祥':'不明确', '未知':'不明确', 
          '未透露':'不明确', '未融资':'尚未获投',
          '种子':'种子轮', 'pre-a':'Pre-A轮', '天使':'天使轮', 'seed':'种子轮', 
          'pre-b':'Pre-B轮', 'a+':'A+轮', 'b+':'B+轮', 'angel':'天使轮', 'preb':'Pre-B轮', 
          'pre_b':'Pre-B轮', 'pre_a':'Pre-A轮', 'prea':'Pre-A轮',
          'a':'A轮', 'b':'B轮', 'c':'C轮', 'd':'D轮', 'e':'E轮', 'f':'F轮', 'g':'G轮','vc':'VC',
          'buyout':'PE-Buyout', 'growth':'PE-Growth', 'pipe':'PE-Pipe', 'pe':'PE'}

care_stage = set('未知,A,Angel,A轮,A+轮,Pre-A,Pre-A轮,VC,VC-SeriesA,尚未获投,不明确,天使轮,未透露,种子轮'.decode().split(','))

required_field = {'description':[], 'brief':[], 'fullname':[], 'homepage':[], 'founder':[]}
quality = {'itjuzi':5, '36kr_rong':5, 'media':5, 'lagou':5,
           'newseed':4,'angelcrunch':4, 'evervc':4,
           'innotree':3, 'vc':3, '36kr':3,
           'protfolio':2, 'demo8':2, 'next':2,
           'admin':1, 'manual':1, 'xuetang':1, 'unclaimed':1, 'investor_cc':1,
           'platform':6, 'union':6, 'www':6, 'internal':6}

ETHSPY_TYPE = 0
LEADS_TYPE = 1
PROJECT_TYPE = 2
NEWS_TYPE = 3
INVESTMENT_TYPE = 5
OTHER_TYPE = 6

FROM_ID_BASE = 100000000

strategy_list = ['projectName', 'total_projectName', 'host', 'companyName']
ID_SWITCHER = {-8:-120000000,#crawled leads
                8:100000000,#admin leads
                1:200000000,#online
                2:200000000,#offline
                32:300000000,#news
                64:400000000,#admin investment
                'crawled_leads':0,
                'leads':100000000,
                'project':200000000,
                'news':300000000,
                'crawled_investment':500000000}

UNUSABLE_UPPER_BOUND = 20
search_url = 'http://ir.ethercap.com/search?intersect=1&bejson=1&pagesize=%s&nocdg=1&'

SEARCH_PAGESIZE = 20

INIT_STATUS = 'INIT'
UPLOADED_STATUS = 'UPLOADED'
COMPLETE_STATUS = 'COMPLETE'
BROKEN_STATUS = 'BROKEN'
DOWNLOAD_STATUS = 'DOWNLOAD'
