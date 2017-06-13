import json
import redis
import pdb

class DeviceBucket:
    def __init__(self):
        self.dvcInfo = {}
        self.bucket = {}
        self.rds = redis.StrictRedis(host='10.1.60.190')
        path = '/opt/parsed_data/demography/USER_result.txt'
        for line in open(path):
            try:
                info = json.loads(line[:-1])
                self.dvcInfo[info['uid'].lower()] = info
            except:
                pass

    def put(self, dvcId, pSet):
        if dvcId not in self.dvcInfo:
            return len(pSet)
        info = self.dvcInfo[dvcId]
        gender = info['baby_gender']
        age = info['baby_age_month']
        if age < 0:
            age = -1
        if age <= 6:
            age = 6
        elif age > 6 and age <= 12:
            age = 12
        elif age > 12 and age <= 24:
            age = 24
        elif age > 24 and age <=36:
            age = 36
        elif age > 36 and age <= 60:
            age = 60
        else:
            age = 100

        bucket = self.bucket
        if gender not in bucket:
            bucket[gender] = {}
        bucket = bucket[gender]
        if age not in bucket:
            bucket[age] = {}
        bucket = bucket[age]
        for pid in pSet:
            key = 'article_p_%s'%pid
            value = self.rds.get(key)
            if value:
                catgy = value.split()
                for c in catgy[::2]:
                    if c[:2] == 'B_':
                        if c not in bucket:
                            bucket[c] = {}
                        if pid in bucket[c]:
                            bucket[c][pid] += 1
                        else:
                            bucket[c][pid] = 1
        return 0

    def dump(self, path):
        fp = open(path, 'w')
        for gender, bucket1 in self.bucket.iteritems():
            for age, bucket2 in bucket1.iteritems():
                for catgy, bucket3 in bucket2.iteritems():
                    li = sorted(bucket3.iteritems(), key=lambda x:x[1], reverse=True)
                    for pid, count in li[:100]:
                        if count < 5:
                            break
                        fp.write('%s, %s, %s, %s, %s\n'%(gender, age, catgy, count, pid))
        fp.close()
