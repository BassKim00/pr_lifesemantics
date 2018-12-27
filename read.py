import json
from pprint import pprint
import numpy as np

#json형식을 str로 인식후 json으로 변환

data=[]
with open('activity_simple.json','r') as df:
    for lines in df:
        data.append(json.loads(lines))

#class 생성

class Log(object):
    def __init__(self,_id,name,steps,distance,calories,createAt,period):
        self._id = _id
        self.name = name
        self.steps = steps
        self.distance = distance    #kilo 구분 의미있음?
        self.calories = calories
        self.createAt = createAt
        self.period = period

    def __repr__(self):
        return self._id

class User(object):
    def __init__(self,name,steps,distance,calories,time):
        self.name = name
        self.steps = steps
        self.distance = distance
        self.calories = calories
        self.time = time

    def __repr__(self):
        return self._id

    def week(self):
        return self._id #1주일 평균 어떻게 구하지

    def month(self):
        return self._id #1달 평균 어떻게 구하지

#log 할당

log=[]
for d in data:
    log.append(Log(d['_id'],d['recordkey'],d['steps'],d['distance'],d['calories'],d['createAt'],d['period']))

print("log[0]")
pprint(log[0].__dict__)

#user 별 class 계산

