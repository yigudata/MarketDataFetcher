import struct
import os
from util import const, auxiliary
import csv
import math

struct_fmt = 'iifffffffff' # int[5], float, byte[255]
struct_len = struct.calcsize(struct_fmt)
struct_unpack = struct.Struct(struct_fmt).unpack_from
struct_pack = struct.Struct(struct_fmt).pack
ex_code ='xhkg'

def readfile (from_dir):
    results = []
    for filename in os.listdir(from_dir):
        if not filename.endswith('.day'):
            continue
        with open(from_dir+'/'+filename, "rb") as f:
            adj_open_3 = None
            adj_open_2 = None
            adj_open_1 = None
            open_prc = None
            adj_cum_f_1 = None
            date_1 = 0

            print("filename %s" % filename)
            # if  filename == 'A.day':
            #       print("filename0001.day"  )
            while True:
                data = f.read(struct_len)
                if not data: break
                s = struct_unpack(data)
                date_2 = s[0] ^ 255
                if date_1 > 0 and date_2 - date_1 > 1000 :
                    results.append((*s, filename, date_1, date_2,date_2 - date_1))
                date_1 = date_2



        with open(from_dir+"/date_checker"+const.csv_file_ext, mode='w', newline='') as day_csv_file:
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
                                        data[11],# filename
                                         data[12],
                                         data[13],
                                         data[14]
                                       ])
            day_csv_file.close()
pass

if __name__ == "__main__":
    filePath = r'D:/predator-eagle/china-a-stock/'
    #filePath  = r'D:\predator-eagle\global_market\data\hist_day\cn\xhkg\1177.day'
    readfile(filePath)

