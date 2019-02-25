import time
import numpy as np
import pymongo
from bson.objectid import ObjectId
from pymongo import MongoClient
import datetime
import pandas as pd
from datetime import date
from dateutil.relativedelta import relativedelta
import socks
import socket


#### generate form ####
base_socket = socket.socket
socks.setdefaultproxy(socks.PROXY_TYPE_SOCKS5, "10.109.35.5", 8142)
socket.socket = socks.socksocket

uri = 'mongodb://10.10.10.36:27017/luna_web'
client = MongoClient(uri)

db = client['luna_web']

# 照顧項目價格
pipeline=[
    {
        "$match":{
            "serviceCategory":1
        }
    },
    
    {
    '$unwind': "$serviceOption"
}, {
    '$project': {
#         "name": "$serviceOption.description",
        "seq": "$serviceOption.seq",
        "cost": 1,
        "unit": {
            '$ifNull': ["$unit", 1]
        }
    }
}, {
    '$addFields': {
        'price': {
            '$divide': ["$cost", "$unit"]
        }
    }
},
{
    "$project":{
        "_id":0,
        "price":1,
        "seq":1
    }
},
{
    '$sort':{'seq':1}
},{
        
        '$match':{
            "seq" :{'$regex': '^(?!(AA|CA))'
            }
        } 
},
    {
        '$project':{
            'seq':1,
            'price':{'$trunc':"$price"}
        }
    }
]

servicePrice = db.command('aggregate', 'ServiceItem', pipeline=pipeline)
servicePrice = pd.DataFrame(servicePrice['result'])
servicePrice = servicePrice.set_index('seq')
servicePrice
# serviceItemFormat = serviceItemFormat.join(servicePrice)
# serviceItemFormat

#公司列表
# 'code':'/^ofo/' ：服務全開，所以sidebar是空的，但其實有買居服服務
pipeline = [
    {'$match':{'shortname':{'$nin':["士林靈糧","士林靈糧基隆","童傳盛文教基金會"]}}}, 
    {
    '$match': { 
        '$or': [
            { 'sidebar': {'$in':["case"]} }, 
            {'code':{'$regex':'^ofo'}}
            ] }
},
    {
        '$project':{
            "_id":0,
            "shortname":1
            }
        }
]

companyFormat = db.command('aggregate', 'Company',pipeline=pipeline)
companyFormat = pd.DataFrame(companyFormat['result'])
companyFormat.columns = ['company']
companyFormat = companyFormat.set_index('company')
# companyFormat



# 班表執行
def shiftExec():
    pipeline=[
    {
            '$match':{
                'valid':True
            }
        },
           {
            '$lookup':{
                "from": "Shift",
                "localField": "shiftId",
                "foreignField": "_id",
                "as": "Shift"
                }
            },
        {
            '$unwind':"$Shift"
            }
            ,
        {
            '$project':{
                'clockIn':1,
                'companyId':"$Shift.companyId"
                }
            }
            ,
        { '$match':{
            'clockIn':{
                "$gte":weekStart
                ,"$lt":weekEnd
                }
        }
    },
    {
        '$group':{
            '_id':"$companyId",
            'monthCount':{
                '$sum':1
                }     
            }  
        }
        ,
    {
        '$project':{
            '_id':0,
            'companyId':"$_id",
            'monthCount':1
            }
        },
    {
        '$lookup':{
            "from": "Company",
            "localField": "companyId",
            "foreignField": "_id",
            "as": "Company"
            }
        },
    {
        '$unwind':"$Company"
        },
    {
        '$project':{
            '_id':0,
            'monthCount':1,
            'company':"$Company.shortname"
            }
        }

    ]

    clockIn = db.command('aggregate', 'ShiftRecord', pipeline=pipeline)
    clockIn = pd.DataFrame(clockIn['result'])
    clockIn = clockIn.set_index('company')
    clockIn
    
    clockInResult = companyFormat.join(clockIn)#過濾掉不要的公司
    cal = clockInResult['monthCount'].sum()
    return cal


# 照顧項目執行
def serviceItemExec():
    pipeline=[
    {
        '$lookup': {
            "from": "ServiceRecord",
            "localField": "_id",
            "foreignField": "employeeId",
            "as": "ServiceRecord"
        }
    },
    {
        '$unwind': "$ServiceRecord"
    },
    {
        '$project': {
            "_id": 0,
            "name": 1,
            "companyId": 1,
            "ServiceRecord": "$ServiceRecord.serviceItems",
            "fillinDate":"$ServiceRecord.fillinDate",
            "valid":"$ServiceRecord.valid"
        }
    },
    {
        "$match":{
            "fillinDate" : {"$gte":weekStart,
                           "$lt":weekEnd
                           },
            "valid":True
            }
        },
    {
        '$unwind': "$ServiceRecord"
    },
    {
        '$project': {
            "name": 1,
            "companyId": 1,
            "ServiceRecordTimes": "$ServiceRecord.times",
            "ServiceRecordCount": "$ServiceRecord.count",
            "ServiceRecordSeq": "$ServiceRecord.seq",
            "ex": {
                "$subtract": ["$ServiceRecord.count", "$ServiceRecord.times"]
            }
        }
    },
    {
        '$project': {
            "name": 1,
            "companyId": 1,
            "ServiceRecordTimes": 1,
            "ServiceRecordCount": 1,
            "ServiceRecordSeq": 1,
            "ex": {
                '$ifNull': ["$ex", "$ServiceRecordCount"]
            }
        }
    },
    {
        '$project': {
            "name": 1,
            "companyId": 1,
            "ServiceRecordTimes": 1,
            "ServiceRecordCount": 1,
            "ServiceRecordSeq": 1,
            "ex": {
                '$cond': {
                    'if': {
                        '$gte': ["$ex", 0]
                    },
                    'then': "$ex",
                    'else': 0
                }
            }
        }
    }, 
    {
        '$group': {
            '_id': {
                     "ServiceRecordSeq":"$ServiceRecordSeq",
                      "companyId":"$companyId"       
                     },
            "ServiceRecordTimesTotal": {
                '$sum': "$ServiceRecordTimes"
            },
            "ServiceRecordCountTotal": {
                '$sum': "$ServiceRecordCount"
            },
            "exTotal": {
                '$sum': "$ex"
            }
        }
    },
    {
        '$project': {
            '_id': 0,
            "seq": "$_id.ServiceRecordSeq",
             "companyId":"$_id.companyId",
            "TimesTotal": "$ServiceRecordTimesTotal",
            "CountTotal": "$ServiceRecordCountTotal",
            "exTotal": 1,
            "realCount": {
                "$subtract": ["$ServiceRecordCountTotal", "$exTotal"]
            }
        }
    },
    {
        '$lookup': {
            "from": "Company",
            "localField": "companyId",
            "foreignField": "_id",
            "as": "company"
        }
    },
    {
        '$unwind': "$company"
    },
    {
        '$project':{
            'seq':1,
            'realCount':1,
            'TimesTotal':1,
            'CountTotal':1,
             'company':"$company.shortname"
            }
        },
    {
        "$sort": {
            "company": 1,
            "seq":1
        }
    },
    {
    '$match':{
    'seq':{'$regex':'^(?!(A|C|R))'}
#         'serviceCategory':1,
#         'serviceCode':{'$regex':'^(?!C)'}
        }
    }
    ]

    realExecute = db.command('aggregate', 'Employee', pipeline=pipeline)
    realExecute = pd.DataFrame(realExecute['result'])
    realExecute = realExecute.set_index(['seq','company'])
    realExecute = companyFormat.join(realExecute)
    realExecute = realExecute.reset_index(['company'])
    realExecute

    serviceItemCount = realExecute['CountTotal'].sum()
    serviceItemCount
    
    return realExecute,serviceItemCount


# 啟動程式，把個數值塞進object裡
# 禮拜六凌晨1:00啟動程式 
timeObj = {'startDate':None,'endDate':None,'shiftExec':None,'serviceItemExec':None,'price':None}


#datenow = datetime.datetime(2019,2,18,10,0)
weekStart = datetime.datetime.now() - relativedelta(weeks=+1) + relativedelta(hours=-18) #weekStart 2018-08-03 16:00:00
weekEnd = datetime.datetime.now() + relativedelta(hours=-18)  #weekEnd 2018-08-10 16:00:00
#weekStart = datenow - relativedelta(weeks=+1) + relativedelta(hours=-18) #weekStart 2018-08-03 16:00:00
#weekEnd = datenow + relativedelta(hours=-18)  #weekEnd 2018-08-10 16:00:00
print('weekStart',weekStart)
print('weekEnd',weekEnd)

summaryFormat=[]

shiftExecCount = shiftExec()
serviceItemExecCount = serviceItemExec()
timeObj = {}
timeObj['date']= weekEnd.date()
# timeObj['endDate']= weekEnd #到時候檢查code可看完整時間
# timeObj['startDate']= weekStart
timeObj['班表執行次數(當週)']= shiftExecCount
timeObj['服務項目執行次數(當週)']= serviceItemExecCount[1]
price = serviceItemExecCount[0].join(servicePrice)
price['cal'] = price['CountTotal'] * price['price']
timeObj['產值(K)(當週)'] = price['cal'].sum() / 1000
summaryFormat.append(timeObj)
    
summaryFormat

summary = pd.DataFrame(summaryFormat)


## employee ##
# 居服員256
pipeline=[
    { '$match':{
        'valid':True, 
        '$or': [
            {'leaveOfficeDate': {'$eq': None}},
            {'leaveOfficeDate': {'$gte': datetime.datetime.now()}}],
        'role': {
            '$in': [256]
        }
    }
},
{
    '$unwind':"$role"
    }
    ,
{
    '$match':{
        'role':{
            '$in':[256]
            },
        'companyId':{ '$ne': ObjectId("5966d903f348d51cb8bcf6a9")}  # 切膚之愛 (日照)
        }
    },
{
    '$group':{
        '_id': {
            'companyId':'$companyId',
            'role':'$role'
            },
        'roleCount':{'$sum':1}
     }
},
{
    '$project':{
        '_id':0,
        'companyId':"$_id.companyId",
        'role':{'$substr':["$_id.role",0,4]},  # transform to string
        'roleCount':1
        }
    },
{
    '$group':{
        '_id':"$companyId",
        'data':{
            '$push':{
                'k':"$role",
                'v':"$roleCount"
                }
            }
        }
    },
{
    '$project':{
        'data':{
            '$arrayToObject': "$data"  # k:field,v:key
            }, 
        'companyId':"$_id"
        },
    },
    {
        '$lookup':{
            "from": "Company",
            "localField": "companyId",
            "foreignField": "_id",
            "as": "Company"
            }
        },
     {
        '$unwind':"$Company"  # []的拆解 
        },    
     {
        '$project':{
             '_id':0,
            'data':1,
            'company':"$Company.shortname"
            }
        },
{
    '$addFields': {
        "data.company": "$company"
    }
},
{
    '$replaceRoot': {        # field to new documents
        'newRoot': "$data"
    }
},
{
    '$sort':{"company":1}
}
]
        
employee = db.command('aggregate', 'Employee', pipeline=pipeline)
employee = pd.DataFrame(employee['result'])
employee = employee.set_index('company')
employee.columns = ['居服員(在職)']
employee   # 各公司employee的人數


## case ##
pipeline=[
    { '$match':{
    'valid':True, 
    'caseType':'01', 
    'status':{'$nin':["40","50"]}  #40結案 50轉介
    
    }
},
{
    '$group':{
        '_id': '$companyId',
        'count':{'$sum':1}
     }
},
{
    '$lookup':{
        'from': 'Company',
        'localField': '_id',
        'foreignField': '_id',
        'as': 'company',
        }
},
{
    '$unwind': '$company',
},
{
    '$project': {
        '_id':0,
        'count':1,
        'company':'$company.shortname'}
}
    
]

inServiceCase = db.command('aggregate', 'Case', pipeline=pipeline)
inServiceCase = pd.DataFrame(inServiceCase['result'])
inServiceCase = inServiceCase.set_index('company')
inServiceCase.columns = ['服務中個案']
inServiceCase

result = companyFormat.join(employee).join(inServiceCase)
result = result.fillna(0)
#[result['居服員(在職)'].sum(), result['服務中個案'].sum()]

summary['居服員人數(在職)'] = result['居服員(在職)'].sum()
summary['個案數(服務中)'] = result['服務中個案'].sum()

## outputFormat ##
outputFormat = summary.T

outputFormat1 = pd.DataFrame([
                      outputFormat.loc['居服員人數(在職)'], 
                      outputFormat.loc['個案數(服務中)'],
                      outputFormat.loc['班表執行次數(當週)'],
                      outputFormat.loc['服務項目執行次數(當週)'],
                      round(outputFormat.loc['產值(K)(當週)'].astype(np.double))
                ])

outputFormat1 = outputFormat1.astype('int64')

import win32com.client
from dateutil.relativedelta import relativedelta
from datetime import datetime, timezone


#### send mail ####
olMailItem = 0x0
obj = win32com.client.Dispatch("Outlook.Application")
newMail = obj.CreateItem(olMailItem)

wk = time.strftime("%W")

day = datetime.now()
if len(str(day.month)) < 2 :
    mon = '0' + str(day.month)
else : mon = str(day.month)
dat = str(day.year)[2:] + mon

num_format = lambda x: '{:,}'.format(x)

def build_formatters(df, format):
    return {column:format 
        for (column, dtype) in df.dtypes.iteritems()
        if dtype in [np.dtype('int64'), np.dtype('float64')]}


formatters = build_formatters(outputFormat1, num_format)

form = outputFormat1.to_html(formatters=formatters, header=False).replace('<tr>','<tr align="right">').replace('<th>','<th align="left">')


newMail.Subject = 'SHWQ-Luna統計分析_week' + wk
newMail.HTMLBody = """<p>Dear all,</p>
以下連結為 2019 week""" + wk + """luna統計資料以及1804~""" + dat + """統計資料。
<a href="https://tpeswhqalf01.compal.com/share/page/site/luna/documentlibrary#filter=path%7C%2FData%20Analytics%2F2019">
https://tpeswhqalf01.compal.com/share/page/site/luna/documentlibrary#filter=path%7C%2FData%20Analytics%2F2019</a>
<br>
<br>
Summary
{}<br>

<p>Best regards,<br>
Jacky #17333@Compal</p>

""".format(form)

newMail.To  = "Richard_Hsu@compal.com; Mega_Lin@compal.com; Lawrence_Yeh@compal.com; Tiffany_Chang@compal.com;Hyper_Lin@compal.com; DebbieJH_Chen@compal.com; YuChing_Peng@compal.com; Peggy_Wang@compal.com"
newMail.CC = "Omaca_Huang@compal.com; jackyt_hsieh@compal.com"
newMail.Send()


