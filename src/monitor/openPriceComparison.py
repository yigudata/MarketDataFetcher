import urllib.request
import urllib.parse
import pandas as pd
import eikon as ek
import pandas as pd
import pytz
from datetime import datetime
from util import const



header = 'sec,open,昨日收盘价,当前价格,今日最高价,今日最低价,竞买价,竞卖价,成交股数,成交金额,' \
         '买1手,买1报价,买2手,买2报价,买3手,买3报价,买4手,买4报价,买5手,买5报价,' \
         '卖1手,卖1报价,卖2手,卖2报价,卖3手,卖3报价,卖4手,卖4报价,卖5手,卖5报价,日期,时间,秒 \n'

sina_url_base = 'http://hq.sinajs.cn/list='
ek.set_app_key('7e13bd6f2ec24ff8b6090ef4599174d4a68b8075')
tz = pytz.timezone("Asia/Shanghai")
today_ts = datetime.now(tz).strftime("%b-%d-%y-%H-%M-%S")
xshg_syms = r'D:\predator-eagle\monitor\xshg.log'
xshe_syms = r'D:\predator-eagle\monitor\xshe.log'
xshg_sina_data = r'D:\predator-eagle\monitor\xshg_sina_'+ today_ts + '.csv'
xshe_sina_data = r'D:\predator-eagle\monitor\xshe_sina_'+ today_ts + '.csv'
xshg_comp_data = r'D:\predator-eagle\monitor\xshg_comp_'+ today_ts + '.csv'
xshe_comp_data = r'D:\predator-eagle\monitor\xshe_comp_'+ today_ts + '.csv'
ss = '.SS'
sz = '.SZ'
sh = 'sh'
sz = 'sz'

def get_sina_data(ric_df, sym_prefix,sina_data_file,ric_suffix, comp_data_file ):
    #ric_df = pd.read_csv(sym_file)
    symlist = ''
    for index, row in ric_df.iterrows():
        ric = row['Instrument']
        sym = sym_prefix + ric.split('.')[0] + ','
        symlist += sym

    resource = urllib.request.urlopen(sina_url_base+symlist)
    content = resource.read().decode(resource.headers.get_content_charset())

    text_file = open(sina_data_file, "w")
    text_file.write(header+content)
    text_file.close()
    return compare_ek_data(ric_df, ric_suffix, sina_data_file, comp_data_file)

def compare_ek_data(ric_df, ric_suffix, sina_data_file, comp_data_file):
    ric_df['ric'] = ric_df.Instrument.str[:-4]+ ric_suffix;
    df2, err = ek.get_data( ric_df['ric'].tolist(),
                            ['CF_DATE', 'CF_TIME','OPEN_PRC','CF_NAME'],
                            {'SDate': '0', 'EDate': '0'}
                             )
    df2['secID'] = df2.Instrument.str[:-3]
    df2.fillna(0,inplace=True)

    df = pd.read_csv(sina_data_file, engine='python')
    df['secID'] = df.sec.str[13:19]
    df3 = pd.merge(df, df2, left_on='secID', right_on='secID')

    df3 = df3[abs(df3['open'] - df3['OPEN_PRC']) > 0.0001]
    df3['diff'] = df3['open'] - df3['OPEN_PRC']
    df3.to_csv(comp_data_file)
    return df3.empty

if __name__ == '__main__':
    ric_xshg = pd.read_csv(xshg_syms)
    ric_xshe = pd.read_csv(xshe_syms)
    if not get_sina_data(ric_xshg, sh, xshg_sina_data, ss, xshg_comp_data) or \
        not get_sina_data(ric_xshe, sz, xshe_sina_data, sz, xshe_comp_data):
     print("diff here.")
    else:
     print("not diff.")

