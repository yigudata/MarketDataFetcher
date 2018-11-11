import struct
import os
from util import const, auxiliary
import csv
from recordkeeping.dayFile import DayFileService
import pandas as pd

struct_fmt = 'iifffffffff' # int[5], float, byte[255]
struct_len = struct.calcsize(struct_fmt)
struct_unpack = struct.Struct(struct_fmt).unpack_from
struct_pack = struct.Struct(struct_fmt).pack

def procfile (from_filename, to_filename, beginAt=20060104):
    results = []
    with open(from_filename, "rb") as f:
        while True:
            data = f.read(struct_len)
            if not data: break
            s = struct_unpack(data)
            date = s[0] ^ 255
            if date < beginAt:
                continue
            results.append(s)
    f.close()
    with open(to_filename, "ab") as fhw:
        for data in results:
            pack_data = struct_pack(data[0],
                                       data[1],
                                       data[2],
                                       data[3],
                                       data[4],
                                       data[5],
                                       data[6],
                                       data[7],
                                       data[8],
                                       data[9],
                                       data[10]
                                        )
            fhw.write(pack_data)
            fhw.flush()
        fhw.close()
pass

if __name__ == "__main__":
    from_dir= r'D:\predator-eagle\china-a-stock\data_adj\\'
    csv_file = r'D:\predator-eagle\cleaning\list-2018-11-11.csv'
    to_dir = r'D:\predator-eagle\cleaning\\'
    #filename  = r'D:\predator-eagle\global_market\data\hist_day\cn\xhkg\1177.day'
    df= pd.read_csv(csv_file)
    for index, row in df.iterrows():
        dayfile = row['fileName']
        beginAt = row['beginAt']
        print("filename %s beginAt %s" % (dayfile, beginAt))
        procfile(from_dir+dayfile, to_dir+dayfile, beginAt)

