import logging
from util import const
from cfg.validator import Validator
from cfg.cfgMgr import ConfigManager
from logging.handlers import TimedRotatingFileHandler
import os
from util import const, auxiliary
import time
from teafiles import *
from recordkeeping.dayFile import DayFileService
from refdata.instrument import InstrumentService
from datetime import timedelta


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter(const.log_format)
conf = ConfigManager(Validator(const.schema_file, logger), const.config_file)
rics_fetcher = InstrumentService(logger, conf)

def read_from_tea(from_dir, to_dir):
    day_file_writer = DayFileService(logger, conf)
    rics = rics_fetcher.get_instrument_list()
    #for filename in os.listdir(from_dir):
    for ric in rics:
        try:
            with TeaFile.openread(from_dir+ric+const.tea_file_ext) as tf:
                item_count = int(tf.itemcount)
                last_item = list(tf.items(item_count - 1, item_count))
                tf.close()
                day_file_writer.appendfile(to_dir, ric, last_item[0])
        except Exception as excp:
            logger.error(excp, exc_info=True)
        pass
pass

def write_ticks(excode):
    handler = TimedRotatingFileHandler(const.day_log_path + excode+"/"+ str(os.getpid()) + const.day_file_log,
                                       when='midnight', backupCount=10)
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    if excode is None:
        logger.fatal("exchange code is None")
        raise SystemExit(0)


    conf.get_exchange(excode)

    close_pm = auxiliary.get_conf_close_time_pm(conf)
    open_am = auxiliary.get_conf_open_time_am(conf)
    local_tz = auxiliary.get_conf_local_time_zone(conf)

    close_am = auxiliary.get_conf_close_time_am(conf)
    open_pm = auxiliary.get_conf_open_time_pm(conf)
    tick_file_path = conf.get_tick_file_path()
    day_file_path = conf.get_day_file_path()

    while True:
        now = auxiliary.get_now(local_tz)
        if now < open_am:
            logger.debug(const.exchange_not_open_text)
            time.sleep(const.interval*4)
            continue
        elif close_am < now < open_pm:
            logger.debug(const.exchange_lunch_break)
            time.sleep(const.interval*4)
            continue
        elif now > close_pm:
            logger.info(const.exchange_close_text)
            #time.sleep(const.interval * 5)  # wait for 5 mins
            read_from_tea(tick_file_path, day_file_path) # last read
            raise SystemExit(0)
        pass

        try:
            read_from_tea(tick_file_path, day_file_path)
            time.sleep(const.interval*5) #5 mins
        except Exception as excp:
            logger.error(excp, exc_info=True)
            time.sleep(const.interval*5) #5 mins
    pass
if __name__ == '__main__':
    excode='XSHG'
    write_ticks(excode)