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
import pytz
import logging
from recordkeeping.dayFile import DayFileService
import pandas as pd
import math
from util import const, auxiliary

struct_fmt = 'iifffffffff' # int[5], float, byte[255]
struct_len = struct.calcsize(struct_fmt)
struct_unpack = struct.Struct(struct_fmt).unpack_from
struct_pack = struct.Struct(struct_fmt).pack




def readfile (from_dir, to_dir):
    results = []
    # df = pd.read_csv(r'D:\predator-eagle\global_market\ref\factor\uqer_adj.csv')
    # df['secID2'] = df.secID.str[:-5]
    tz = pytz.timezone("Asia/Shanghai")
    today = auxiliary.get_today(tz).replace('-','',2)
    today_int = int(today)
    print("today is %s" % today)

    for filename in os.listdir(to_dir):
        if not filename.endswith('.day'):
            continue

        with open(from_dir+'/'+filename, "rb") as fh:
            fh.seek(0, os.SEEK_END)
            file_size = fh.tell()
            fh.seek(file_size - struct_len)
            position = fh.tell()
            data = fh.read(struct_len)
            s = struct_unpack(data)
            fh.close()
        if s[0] ^ 255 == today_int:
         writedata(s, filename, to_dir, position)
    pass
pass

def writedata(data,filename, to_dir,position):
    with open(to_dir + filename, "ab") as fhw:
        fhw.seek(0, os.SEEK_END)
        file_size = fhw.tell()
        if (file_size != position):
            print("filename is %s, filesize: %s, position %s" % (filename,file_size,position))
            return
        print("filename is %s, add data for: %s " % (filename, data[0]^255))
        fhw.truncate(position)
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
    from_dir = r'D:/predator-eagle/china-a-stock/data'
    to_dir = r'D:\predator-eagle\cleaning\data_adj_revised'
    readfile(from_dir,to_dir)
    pass


