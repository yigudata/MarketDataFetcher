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

            print("filename %s" % filename)
            # if  filename == 'A.day':
            #       print("filename0001.day"  )
            while True:
                data = f.read(struct_len)
                if not data: break
                s = struct_unpack(data)
                if filename == 'A.day' and s[0] ^ 255 == 20141104:
                      print("filename0001.day")
                adj_cum_f_2 = s[7]/s[2]
                if adj_open_3 is not None and open_prc is not None:
                    adj_open_pct = s[7]/adj_open_3
                    open_pct = s[2] / open_prc
                    if adj_cum_f_1 is not None \
                            and adj_cum_f_2 is not None \
                            and not math.isclose(adj_cum_f_2, adj_cum_f_1, rel_tol=1e-5) \
                            and adj_open_pct - open_pct > 0.0 \
                            and adj_cum_f_2 -  adj_cum_f_1 > 0.0 \
                            and adj_open_2 > adj_open_3 < s[7]:
                                results.append((*s, filename, adj_cum_f_2, adj_cum_f_1, adj_open_pct, open_pct,
                                                adj_open_2,adj_open_3, s[7],
                                                (adj_open_2 - adj_open_3)/adj_open_2,
                                                (s[7] - adj_open_3) / adj_open_2)
                                               )
                adj_open_1 = adj_open_2
                adj_open_2 = adj_open_3
                adj_open_3 = s[7]
                open_prc = s[2]
                adj_cum_f_1 = adj_cum_f_2


        with open(from_dir+"/adj_checker"+const.csv_file_ext, mode='w', newline='') as day_csv_file:
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
                                         data[14],
                                         data[15],
                                         data[16],
                                         data[17],
                                         data[18],
                                         data[19],
                                         data[20]
                                       ])
            day_csv_file.close()
pass

if __name__ == "__main__":
    filePath = r'D:/predator-eagle/global_market/data/hist_day/cn/'+ex_code
    #filePath  = r'D:\predator-eagle\global_market\data\hist_day\cn\xhkg\1177.day'
    readfile(filePath)

