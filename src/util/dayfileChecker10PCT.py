import struct
import os
from util import const, auxiliary
import csv
import math
import pandas as pd

struct_fmt = 'iifffffffff' # int[5], float, byte[255]
struct_len = struct.calcsize(struct_fmt)
struct_unpack = struct.Struct(struct_fmt).unpack_from
struct_pack = struct.Struct(struct_fmt).pack
ex_code ='xhkg'

def isExDivDate(df_all, sym, exDivDate):
    # sec_id = sec_code + const.dot + exchange_code
    #sec_id = sec_code + exchange_code
    df = df_all[df_all.secID2.isin([sym, ''])]
    if df.empty:
        return False
    df1 = df[df['exDivDate'] == exDivDate]
    if df1.empty:
        return False
    return True

def readfile (from_dir):
    results = []
    df = pd.read_csv(r'D:\predator-eagle\global_market\ref\factor\uqer_adj.csv')
    df['secID2'] = df.secID.str[:-5]
    for filename in os.listdir(from_dir):
        if not filename.endswith('.day'):
            continue
        with open(from_dir+'/'+filename, "rb") as f:

            adj_close_1 = None
            close_1 = None
            date_1 = None


            print("filename %s" % filename)
            # if  filename == 'A.day':
            #       print("filename0001.day"  )
            while True:
                data = f.read(struct_len)
                if not data: break
                s = struct_unpack(data)
                if filename == '000587.day' and s[0] ^ 255 == 20110818:
                    print("000587.day")
                dateStr = str(s[0] ^ 255)
                exdate = dateStr[0:4] + "-" + dateStr[4:6] + "-" + dateStr[6:8]
                if adj_close_1 is not None: #and open_prc is not None:
                    adj_close_pct = s[10]/adj_close_1
                    abs_pct = math.fabs(adj_close_pct - 1)
                    close_pct =  math.fabs(s[5]/close_1-1)
                    if abs_pct >= 0.1 and \
                            not math.isclose(abs_pct, close_pct, rel_tol=0.001)\
                            and abs_pct > close_pct: #\
                            #and not isExDivDate(df, filename[:-4],exdate):
                        results.append((*s, filename, abs_pct, adj_close_1, close_pct, close_1, date_1))
                adj_close_1 = s[10]
                close_1 = s[5]
                date_1 = s[0]

        with open(from_dir+"/10PCT_close_enhance"+const.csv_file_ext, mode='w', newline='') as day_csv_file:
            day_csv_writer = csv.writer(day_csv_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            for data in results:
                day_csv_writer.writerow([data[0]^255,
                                       data[1],
                                       data[2],#open
                                       data[3],#high
                                       data[4],#low
                                       data[5],#clsoe
                                       data[6],
                                       data[7], #adj_open_
                                       data[8],
                                       data[9],
                                       data[10],#adj_clsoe
                                        data[11],# filename
                                         data[12],
                                         data[13],
                                         data[14],
                                         data[15],
                                         data[16]^255
                                       ])
            day_csv_file.close()
pass

if __name__ == "__main__":
    filePath = r'D:\predator-eagle\cleaning\data_adj_revised'
    #filePath  = r'D:\predator-eagle\global_market\data\hist_day\cn\xhkg\1177.day'
    readfile(filePath)

