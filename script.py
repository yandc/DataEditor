import os
path = 'data/archive/'

#for fname in os.listdir(path):
#    if fname.find('xml4baidu') < 0:
#        continue
#    os.system("grep -o '<wapUrl>https://m.miyabaobei.com/wx/group_detail/[0-9]*.html</wapUrl>' data/archive/%s  | grep -o 'https://m.miyabaobei.com/wx/group_detail/[0-9]*.html' >> data/sublink.txt"%fname)
#
#count = 0
#fp = None
#for line in open('data/sublink.txt'):
#    if count % 2000 == 0:
#        if fp:
#            fp.close()
#        fp = open('data/sublink-%s.txt'%count, 'w')
#    count += 1
#    fp.write(line)
#fp.close()

for fname in os.listdir('data/'):
    if fname.find('sublink-') < 0:
        continue
    os.system("curl -H 'Content-Type:text/plain' --data-binary @data/%s 'http://data.zz.baidu.com/urls?site=http://m.mia.com/&token=PD2PtFRzTiGVCGDZ&type=officialaccounts'"%fname)
