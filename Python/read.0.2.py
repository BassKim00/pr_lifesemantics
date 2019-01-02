import json
from pprint import pprint
import numpy as np

#json형식을 str로 인식후 json으로 변환

data=[]
with open('C:/Users/LS-COM-00044/Desktop/jsondata/activity_simple.json','r') as df:
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
        self.createAt = createAt
        self.period = period

    def __repr__(self):
        return self._id

class Type(object):
    def __init__(self,name,lastweek_avg,thisweek_avg,today):
        self.name = name
        self.lastweek_avg = lastweek_avg
        self.thisweek_avg = thisweek_avg
        self.today = today

    def __repr__(self):
        return self.name

class User(object):
    def __init__(self,name,steps,distance,calories,time):
        self.name = name
        self.steps = steps
        self.distance = distance
        self.calories = calories
        self.time = time
        self.lastweek = []
        self.thisweek = []
        
    def __repr__(self):
        return self.name 

#entire log 할당
log=[]
for d in data:
    log.append(Log(d['_id'],d['recordkey'],d['steps'],d['distance'],d['calories'],d['createAt'],d['period']))

print("log[0]")
pprint(log[0].__dict__)

#log 제거
#del log[i]

#last week log 을 유저에 할당


#할당 후 del해도 될것 같은데? 굳이?
#할당하기보다는 index를 할당하는게 메모리 절약이 될듯
lastweek_index=[]
for d in log:
    lastweek_index.append(log.index(d))

#this week log 할당
thisweek_index=[]
for d in log:
    thisweek_index.append(log.index(d))

#user 별 class 계산

