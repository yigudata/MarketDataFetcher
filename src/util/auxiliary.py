import csv
from datetime import datetime
from util import const
import time
import pytz


def fitem(item):
    item = item.strip()
    try:
        item = float(item)
    except ValueError:
        pass
    return item
    pass

def str_to_float(val):
    val = val.strip()
    try:
        item = float(val)
        return item
    except ValueError:
        pass
    return float('-1.0')
    pass


def read_rics_to_dict(file, enable_flag=True):
    with open(file, 'r') as csvin:
        reader = csv.DictReader(csvin)
        data = {k.strip(): [fitem(v)] for k, v in next(reader).items()} #initial here, first row always included.
        for line in reader:
            if line['Enable'].strip() == 'N' and enable_flag:
                continue
            for k, v in line.items():
                k = k.strip()
                data[k].append(fitem(v))
    return data
    pass

def get_conf_local_time_zone(conf):
    tz = conf.get_time_zone()
    return pytz.timezone(tz)
    pass

def get_conf_open_time_am(conf):
    open_am = conf.get_open_time_am()
    return time.strftime(open_am)
    pass

def get_conf_close_time_am(conf):
    close_am = conf.get_close_time_am()
    return time.strftime(close_am)
    pass

def get_conf_open_time_pm(conf):
    open_pm = conf.get_open_time_pm()
    return time.strftime(open_pm)
    pass

def get_conf_close_time_pm(conf):
    close_pm = conf.get_close_time_pm()
    return time.strftime(close_pm)
    pass

def get_now(timezone):
    return datetime.now(timezone).strftime(const.time_fmt)
    pass

def get_today(timezone):
    return datetime.now(timezone).strftime(const.date_fmt)
    pass

if __name__ == "__main__":
    #filename = r'D:\predator-eagle\global_market\ref\instruments\bak\us-xnys-instrument-list1.csv'
    filename = r'D:\predator-eagle\global_market\ref\instruments\cn-xhkg-instrument-list.csv'
    ric_dict = read_rics_to_dict(filename)
    print (ric_dict['Instrument'])
    print (ric_dict['Enable'])