import win32com.client
from dateutil.relativedelta import relativedelta
from datetime import datetime, timezone
import time

olMailItem = 0x0
obj = win32com.client.Dispatch("Outlook.Application")
newMail = obj.CreateItem(olMailItem)

wk = time.strftime("%W")

day = datetime.now()
if len(str(day.month)) < 2 :
    mon = '0' + str(day.month)
else : mon = str(day.month)
dat = str(day.year)[2:] + mon

newMail.Subject = 'SHWQ-Luna統計分析_week' + wk
newMail.Body = """Dear all,
以下連結為 2019 week""" + wk + """luna統計資料以及1804~""" + dat + """統計資料。
123123123
123123123

Summary
{}""".format(str(outputFormat1))+ 
"""


Best regards,
Jacky #17333@Compal



"""


#newMail.To  = "KevinL_Lee@compal.com"
#newMail.CC = "Samantha_Hsu@compal.com; Peggy_Wang@compal.com; SophiaST_Wang@compal.com; Richard_Hsu@compal.com; jackyt_hsieh@compal.com"
newMail.To  = "jacky8209022323@gmail.com"
newMail.Send()






import pandas as pd
import numpy as np
df = pd.DataFrame(np.random.randn(6,4),columns=list('ABCD'))
s = df.style.set_properties(**{'text-align': 'right'})
s.render()


