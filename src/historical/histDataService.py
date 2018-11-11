from upstream.eikonDataService import EikonDataService
import logging

from util import const, auxiliary
import pytz
import pandas as pd
import os
from refdata.instrument import InstrumentService
from datetime import datetime
from datetime import timedelta
import time


class HistoricalDataService:

    def __init__(self, logger, conf, exchange_code):
        self.logger = logger
        self.conf = conf
        conf.get_exchange(exchange_code)
        key = self.conf.get_eikon_key()
        self.logger.info("key: %s", key)
        self.ek = EikonDataService(key, logger)
        self.rics_fetcher = InstrumentService(logger, conf)

    def fetch_hist_data(self, row):
            try:
                today = datetime.now()
                #start = datetime.strptime(row['First Trade Date'], const.date_fmt)
                start = datetime.strptime('2006-01-04', const.date_fmt)
                end = None
                values = []
                while True:
                    end = start + timedelta(days=365*10)
                    if end > today:
                        end = today
                    data_df = self.ek.retireve_hist_data(row['Instrument'],
                                                         start.strftime(const.date_fmt),
                                                         end.strftime(const.date_fmt))
                    values.append(data_df)
                    start = end
                    if(today == end):
                        break
                    #time.sleep(5)

                df = pd.concat(values)
                df.sort_index(inplace=True)
                df.to_csv(self.conf.get_hist_file_path() + row['Instrument'] + const.csv_file_ext,
                              encoding='utf-8', index=True)
            except Exception as e:
                self.logger.error(e, exc_info=True)

            pass

    def process(self):
       # rics = self.rics_fetcher.get_instrument_list()
        ric_df = pd.read_csv(self.conf.get_instruments_file())
        for index, row in ric_df.iterrows():
            self.fetch_hist_data(row)
            self.logger.info('done for %s', row['Instrument'])
            time.sleep(3)
        pass
        self.logger.info('all done')
    pass
