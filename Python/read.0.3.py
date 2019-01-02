import json
from pprint import pprint
import datetime as dt

import numpy as np



#json형식을 str로 인식후 json으로 변환

data=[]
with open('C:/Users/LS-COM-00044/Desktop/jsondata/activity.json','r') as df:
    for lines in df:
        data.append(json.loads(lines))

#class 생성

class Log(object):

    #unit converter
    
    def dis_kilo(self,unit,value):
        if unit=="KILOMETER" or unit=="km":
            return value
        else:
            return value/1000

    def cal_kilo(self,unit,value):
        if unit=="KILOCALORIE" or unit=="kcal":
            return value
        else:
            return value/1000
        
    #initial
        
    def __init__(self,_id,name,steps,distance,calories,createAt,period):
        self._id = _id
        self.name = name
        self.steps = steps
        self.distance = self.dis_kilo(distance['unit'],distance['value'])
        self.calories = self.dis_kilo(calories['unit'],calories['value'])
        self.createAt = dt.datetime.strptime(createAt['$date'], '%Y-%m-%dT%H:%M:%S.%fZ')
        self.period = dt.datetime.strptime(period['to']['$date'], '%Y-%m-%dT%H:%M:%S.%fZ')

    def batch(self):
        #오래된 데이터 del
        return self._id
    
    def __repr__(self):
        return self._id

log=[]
for d in data:
    log.append(Log(d['_id'],d['recordkey'],d['steps'],d['distance'],d['calories'],d['createAt'],d['period']))

print("log[0]")
pprint(log[0].__dict__)


#log 제거
#del log[i]

#index를 할당하는게 메모리 절약이 될듯
#대신 batch로 오래된 데이터 처리하는건 주차별 할당 이전에 해야 함
#json date 인식 by isocalendar()


#thisweek_iso=dt.datetime.now().isocalendar()
#thisweek 임의 조정
thisweek_iso=list(log[1].period.isocalendar())

lastweek_iso=list(thisweek_iso)
lastweek_iso[1]-=1

#this week & last week log 할당

thisweek_index=[]
lastweek_index=[]
for d in log:
    if list(d.period.isocalendar()[:2])==thisweek_iso[:2]:
        thisweek_index.append(log.index(d))
    elif list(d.period.isocalendar()[:2])==lastweek_iso[:2]:
        lastweek_index.append(log.index(d))

#user 별 class 계산
#for i in thisweek_index:
user={x.name: [[],[]] for x in log}

for i in thisweek_index:
    user[log[i].name][0].append(log[i].calories)

for i in lastweek_index:
    user[log[i].name][1].append(log[i].calories)
