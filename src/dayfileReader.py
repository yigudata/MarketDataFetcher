import struct
import os
from util import const, auxiliary
import csv

struct_fmt = 'iifffffffff' # int[5], float, byte[255]
struct_len = struct.calcsize(struct_fmt)
struct_unpack = struct.Struct(struct_fmt).unpack_from
struct_pack = struct.Struct(struct_fmt).pack


def readfile (filename):
    results = []
    with open(filename, "rb") as f:
        while True:
            data = f.read(struct_len)
            if not data: break
            s = struct_unpack(data)
            results.append(s)
            for index in range(len(s)):
                if index == 0 :
                    print ('date:', s[index]^255)
                else:
                    print( 'data:', s[index])
                pass
            pass
        with open(filename+const.csv_file_ext, mode='w', newline='') as day_csv_file:
            day_csv_writer = csv.writer(day_csv_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            for data in results:
                day_csv_writer.writerow([data[0]^255,
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
                                       ])
            day_csv_file.close()
pass

if __name__ == "__main__":
    filename = r'D:\predator-eagle\cleaning\002506.day'
    #filename  = r'D:\predator-eagle\global_market\data\hist_day\cn\xshe\300383.day'
    readfile(filename)

