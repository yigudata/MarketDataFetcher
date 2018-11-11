import eikon as ek
import pandas as pd
import numpy
from util import const, auxiliary
import time
import pytz

ek.set_app_key('7e13bd6f2ec24ff8b6090ef4599174d4a68b8075')
# ric = ["000333.SZ", '002520.SZ', '002573.SZ', '002617.SZ', '002668.SZ',
#        '002707.SZ', '300088.SZ', '300108.SZ', '300131.SZ', '300207.SZ',
#        '300209.SZ', '300229.SZ', '300383.SZ', '300413.SZ', '300425.SZ',
#        '300746.SZ']
# ric = ["300088.SZ","300108.SZ",'300131.SZ','300140.SZ','002515.SZ',
#        '002520.SZ','300178.SZ','002573.SZ','300207.SZ','300209.SZ',
#        '300229.SZ','300237.SZ','002607.SZ','002617.SZ','002707.SZ',
#        '002656.SZ','002668.SZ','002669.SZ','000333.SZ',"002711.SZ",
#        '300383.SZ','300413.SZ','300425.SZ','300428.SZ',
#        '002751.SZ','300464.SZ','002765.SZ','002769.SZ','002819.SZ',
#        '300682.SZ','300746.SZ','000526.SZ','000576.SZ','000703.SZ',
#        '000711.SZ','000004.SZ','000713.SZ','000061.SZ','000534.SZ',
#        '002037.SZ','000972.SZ','002189.SZ','002197.SZ','002147.SZ',
#        '000022.SZ','000418.SZ','000622.SZ','002217.SZ','002128.SZ',
#        '000931.SZ','000979.SZ','300018.SZ','002301.SZ','300041.SZ'
#        ]
ric = ['600338.SS']

tz = pytz.timezone("Asia/Shanghai")
today = auxiliary.get_today(tz)
print("today is %s" % today)
for j in range(len(ric)):
    try:
        print(ric[j])
        values = []
        df1 = ek.get_timeseries(ric[j],
                       fields=["OPEN", "HIGH", "LOW", "CLOSE", "VOLUME"],
                       interval="daily",
                       start_date='2006-01-04',
                       end_date='2008-10-31',
                       corax='unadjusted')
        values.append(df1)
        time.sleep(2)
        df2  = ek.get_timeseries(ric[j],
                                     fields=["OPEN", "HIGH", "LOW", "CLOSE", "VOLUME"],
                                     interval="daily",
                                     start_date='2008-11-1',
                                     end_date=today,
                                     corax='unadjusted')
        values.append(df2)
        df = pd.concat(values)
        df.sort_index(inplace=True)
        df.to_csv(ric[j] + '.csv')
    except (Exception, ValueError) as excp:
        print(excp)
        time.sleep(2)
        df2 = ek.get_timeseries(ric[j],
                        fields=["OPEN", "HIGH", "LOW", "CLOSE", "VOLUME"],
                       interval="daily",
                       start_date='2008-11-1',
                       end_date=today,
                       corax='unadjusted')

        df2.to_csv(ric[j]+'.csv')


