from teafiles import *
from pathlib import Path
from util import const, auxiliary


class TickPersister:

    def __init__(self, logger, conf):
        self.logger = logger
        self.conf = conf
    pass

    def write_tick (self, tickdata, exchange):
        tick = tickdata.split(',')
        symbol = tick[1]
        values = []
        t = DateTime.parse(tick[0][:-6], const.tea_file_ts_fmt)
        open_prc = auxiliary.str_to_float(tick[3])
        high = auxiliary.str_to_float(tick[4])
        low = auxiliary.str_to_float(tick[5])
        close = auxiliary.str_to_float(tick[6])
        volume = auxiliary.str_to_float(tick[7])
        last = auxiliary.str_to_float(tick[8])
        hst_close = auxiliary.str_to_float(tick[9])
        adj_close = auxiliary.str_to_float(tick[10])
        values.append((t, open_prc, high, low, close, volume, last, hst_close, adj_close))
        filename = self.conf.get_tick_file_path()+symbol + const.tea_file_ext
        if Path(filename).is_file():
            with TeaFile.openwrite(filename) as tf:
                for item in reversed(values):
                    tf.write(*item)
        else:
            with TeaFile.create(filename,
                                const.tea_file_header,
                                const.tea_file_format, symbol,
                                {'exchange': exchange, 'decimals': const.tea_file_decimal}
                                ) as tf:
                for item in reversed(values):
                    tf.write(*item)
        pass
