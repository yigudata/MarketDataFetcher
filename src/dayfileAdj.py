import struct
import csv
from cfg.validator import Validator
from cfg.cfgMgr import ConfigManager
from logging.handlers import TimedRotatingFileHandler
import getopt
import sys
import multiprocessing
from util import const
import os
import logging
from recordkeeping.dayFile import DayFileService
import pandas as pd
import math

struct_fmt = 'iifffffffff' # int[5], float, byte[255]
struct_len = struct.calcsize(struct_fmt)
struct_unpack = struct.Struct(struct_fmt).unpack_from
struct_pack = struct.Struct(struct_fmt).pack


def get_accum_adj_factor(df_all, sym, trade_date):
    # sec_id = sec_code + const.dot + exchange_code
    #sec_id = sec_code + exchange_code
    df = df_all[df_all.secID2.isin([sym, ''])]
    if df.empty:
        return const.int_one

    df1 = df[(df[const.from_date] <= trade_date) & (df[const.to_date] > trade_date)]
    if df1.empty:
        df1 = df[(df[const.from_date] < trade_date) & (df[const.to_date] >= trade_date)]
    if df1.empty:
        #self.logger.debug("find nothing for %s upon date:  %s, ", sec_id, trade_date)
        return const.int_one
    return df1.iloc[0][const.accum_adj_factor]

def readfile (from_dir):
    results = []
    df = pd.read_csv(r'D:\predator-eagle\global_market\ref\factor\uqer_adj.csv')
    df['secID2'] =  df.secID.str[:-5]
    to_dir = from_dir + '_adj/'
    for filename in os.listdir(from_dir):
        if not filename.endswith('.day'):
            continue

        with open(from_dir+'/'+filename, "rb") as f:
            content = []
            print("filename %s" % filename)
            while True:
                data = f.read(struct_len)
                if not data: break
                s = struct_unpack(data)
                date = s[0] ^ 255
                sym = filename[:-4]
                dateStr = str(date)
                exdate = dateStr[0:4] + "-" + dateStr[4:6] + "-" + dateStr[6:8]
                aaccum_adj = get_accum_adj_factor(df, sym, exdate)
                #if s[7] / s[2] - aaccum_adj > 0.0:
                if not math.isclose(s[7] / s[2], aaccum_adj, rel_tol=1e-3):
                    results.append((*s, filename))
                tmp = ( s[0], s[1], s[2],
                        s[3], s[4], s[5],
                        s[6],
                        s[2] * aaccum_adj,
                        s[3] * aaccum_adj,
                        s[4] * aaccum_adj,
                        s[5] * aaccum_adj)
                content.append((*tmp, 0))
            pass
        #writedata(content, filename, to_dir)

    with open(to_dir+"/adj_revised"+const.csv_file_ext, mode='w', newline='') as day_csv_file:
            day_csv_writer = csv.writer(day_csv_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            for data in results:
                day_csv_writer.writerow([data[0]^255,
                                       data[1],
                                       data[2],#open
                                       data[3],
                                       data[4],
                                       data[5],
                                       data[6],
                                       data[7], #adj_open_3
                                       data[8],
                                       data[9],
                                       data[10],
                                        data[11] # filename
                                       ])
            day_csv_file.close()
pass

def writedata(content,filename, to_dir):
    with open(to_dir + filename, "ab") as fhw:
        for data in content:
            pack_data = struct_pack(data[0],
                                     data[1],
                                     data[2],  # open
                                     data[3],
                                     data[4],
                                     data[5],
                                     data[6],
                                     data[7],  # adj_open_3
                                     data[8],
                                     data[9],
                                     data[10]
                                     )
            fhw.write(pack_data)
            fhw.flush()
        fhw.close()
pass

if __name__ == '__main__':
    filePath = r'D:/predator-eagle/china-a-stock/data'
    #readfile(conf.get_day_file_path(), day_file_writer, excode)
    readfile(filePath)
    pass


