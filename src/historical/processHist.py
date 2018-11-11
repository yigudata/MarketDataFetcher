import logging
from util import const
from cfg.validator import Validator
from cfg.cfgMgr import ConfigManager
from logging.handlers import TimedRotatingFileHandler
import os
from util import const, auxiliary
import time
from recordkeeping.dayFile import DayFileService
from refdata.instrument import InstrumentService
from datetime import timedelta
import pandas as pd

#logger = logging.getLogger(__name__)

def process(conf, logger):
    day_file_writer = DayFileService(logger, conf)
    rics_fetcher = InstrumentService(logger, conf)
    rics = rics_fetcher.get_instrument_list(False)
    logger.info('total rics  %s', len(rics))
    hist_dir = conf.get_hist_file_path()
    hist_day_dir = conf.get_hist_day_file_path()
    for ric in rics:
        try:
            df = pd.read_csv(hist_dir + ric +const.csv_file_ext)
            for index, row in df.iterrows():
                day_file_writer.hist_appendfile(hist_day_dir, ric, row)
            logger.info('done for %s', ric)
        # time.sleep(5)
        except Exception as excp:
            logger.error(excp, exc_info=True)
    pass
pass