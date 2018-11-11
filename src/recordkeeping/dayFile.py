import struct
from util import const
from refdata.adjFactor import AdjFactorService
import os
import os.path


struct_fmt = const.struct_fmt
struct_len = struct.calcsize(struct_fmt)
struct_unpack = struct.Struct(struct_fmt).unpack_from
struct_pack = struct.Struct(struct_fmt).pack

class DayFileService:

    def __init__(self, logger, conf):
        self.logger = logger
        self.conf = conf
        self.adj_factor = AdjFactorService(logger, conf)

    def get_accum_adj (self, ric, date):
        code = None
        if "XNYS" == self.conf.get_exchange_id():
            code = ric[:-1]  # one letter ric code
        else:
            code = ric[:-2] #two letter ric code
        #code = ric.split(const.dot)[0]
        #ts_str = str(t.time).split(const.white_space)
        return self.adj_factor.get_accum_adj_factor(code, self.conf.get_exchange_id(), date)

    def getfilename(self, to_dir, ric):
        if ric.startswith(const.dot):
            return to_dir+ric + const.day_file_ext
        return to_dir+ric.split(const.dot)[0]+const.day_file_ext

    def write_data(self, filename, t, accumAdj=1, remain_size=0):
        with open(filename, "ab") as fhw:
            fhw.truncate(remain_size)

            ts_str = str(t.time).split(const.white_space)
            date = int(ts_str[0].replace(const.dash, ''))
            time = int(ts_str[1].replace(const.colon, '')[:-5])
            pack_data = struct_pack(date^255,
                                    time,
                                    t.OPEN_PRC,
                                    t.CF_HIGH,
                                    t.CF_LOW,
                                    t.CF_LAST,
                                    t.CF_VOLUME,
                                    t.OPEN_PRC*accumAdj,
                                    t.CF_HIGH*accumAdj,
                                    t.CF_LOW*accumAdj,
                                    t.CF_LAST*accumAdj
                                    )
            fhw.write(pack_data)
            fhw.flush()
            fhw.close()
        pass

    def appendfile(self, to_dir, ric, t):
        filename = self.getfilename(to_dir, ric)
        self.logger.debug("write ric %s into file %s," % (ric, filename))
        ts_str = str(t.time).split(const.white_space)
        accum_adj = self.get_accum_adj(ric, ts_str[0])

        if not os.path.exists(filename):
            self.write_data(filename, t, accum_adj)
            return

        with open(filename, "rb") as fh:
            fh.seek(0, os.SEEK_END)
            file_size = fh.tell()
            if file_size == 0:
                fh.close()
                self.write_data(filename, t, accum_adj)
                return
            pass
            mod = file_size % struct_len
            if mod != 0:
                fh.close()
                print('day corrupt')
                return
            pass
            fh.seek(file_size - struct_len)
            position = fh.tell()

            data = fh.read(struct_len)
            s = struct_unpack(data)

            ts_str = str(t.time).split(const.white_space)
            date = int(ts_str[0].replace(const.dash, ''))

            remain_size = None
            if s[0] ^ 255 != date:
                remain_size = file_size
            else:
                remain_size = position
            pass
            fh.close()
        pass
        self.write_data(filename, t, accum_adj, remain_size)
    pass

    def write_hist_data(self, filename, row, accumAdj=1):
        date = int(row['Date'].replace(const.dash, ''))
        pack_data = struct_pack(date^255,
                                1500,
                                row['OPEN'],
                                row['HIGH'],
                                row['LOW'],
                                row['CLOSE'],
                                row['VOLUME'],
                                row['OPEN'] * accumAdj,
                                row['HIGH'] * accumAdj,
                                row['LOW'] * accumAdj,
                                row['CLOSE'] * accumAdj
                                )
        with open(filename, "ab") as fhw:
            fhw.write(pack_data)
            fhw.flush()
            fhw.close()
    pass
    def hist_appendfile(self, to_dir, ric, row):
        filename = self.getfilename(to_dir, ric)
        accum_adj = self.get_accum_adj(ric, row['Date'])
        self.logger.debug("ric %s, date %s, accum_adj %s" % (ric, row['Date'], accum_adj))
        self.write_hist_data(filename, row, accum_adj)
    pass

    def convert_filename(self, to_dir, hist_filename):
        return to_dir+hist_filename
    pass
