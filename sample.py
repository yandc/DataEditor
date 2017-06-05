import datetime
import json
import zlib
import os
import pdb

def sampleRate():
    today = datetime.date.today()
    yday = today - datetime.timedelta(days=1)
    date = yday

    stats = {}
    logPrefix = '[INFO] [2017-05-25 02:07:13] '
    while True:
        pva = 0
        pvb = 0
        uva = {}
        uvb = {}
        dateStr = date.strftime('%Y%m%d')
        srcPath = '/root/miagrouptools/pull_log/synclog/access.log%s'%dateStr
        path = 'detail-access.log%s'%(dateStr)
        if not os.path.exists('detail-access.log%s'%dateStr):
            os.system('grep getSingleSubjectById %s>%s'%(srcPath, path))

        print 'Process %s'%path
        for line in open(path):
            try:
                log = json.loads(line[len(logPrefix):])
                if log['request_action'] != 'getSingleSubjectById':
                    continue
                dvcId = log['user_log']['curl_params']['ext_params']['dvc_id']
                subId = log['user_log']['curl_params']['params']['suject_id']
            except:
                continue
            hint = zlib.crc32(dvcId)&0xffffffff
            if hint % 10 == 9:
                pva += 1
                if dvcId not in uva:
                    uva[dvcId] = set()
                uva[dvcId].add(subId)
            else:
                pvb += 1
                if dvcId not in uvb:
                    uvb[dvcId] = set()
                uvb[dvcId].add(subId)

        _uva = sum([len(y) for x, y in uva.iteritems()])
        _uvb = sum([len(y) for x, y in uvb.iteritems()])
        path = 'koubei-abtest.csv'
        fp = open(path, 'a')
        fp.write('%s, %s, %s, %s, %s, %s, %s\n'%(dateStr, pva, pvb, _uva, _uvb, len(uva), len(uvb)))
        fp.close()
        
        if date == yday:
            break
        date += datetime.timedelta(days=1)

if __name__ == '__main__':
    sampleRate()            
