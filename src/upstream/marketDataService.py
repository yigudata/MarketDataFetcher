from upstream.eikonDataService import EikonDataService
import logging
from cfg.validator import Validator
from cfg.cfgMgr import ConfigManager
from util import const, auxiliary
import pytz
import pandas as pd
import datetime
import time
from refdata.instrument import InstrumentService
import numpy as np
from logging.handlers import TimedRotatingFileHandler
import os
import math


class MarketDataService:

    def __init__(self, logger, conf):
        self.logger = logger
        self.conf = conf
        key = self.conf.get_eikon_key()
        self.logger.info("key: %s", key)
        self.ek = EikonDataService(key, logger)
        self.local_tz = self.get_local_time_zone()
        self.today = datetime.datetime.now(self.local_tz)
        self.today_str = self.today.strftime(const.date_fmt)
        self.ric_fetcher = InstrumentService(logger, conf)

    pass

    def get_ric_list(self):
        return self.ric_fetcher.get_instrument_list()
    pass

    def is_today_holiday(self):
        if self.today.isoweekday() > 5:
            return True

        holiday_file = self.conf.get_holiday_file()
        self.logger.info("holiday_file: %s", holiday_file)
        holiday_df = pd.read_csv(holiday_file)
        ccy = self.conf.get_ccy()
        self.logger.info("ccy: %s, today: %s", ccy, self.today_str)
        holiday_df = holiday_df[holiday_df.CCY.isin([ccy, ''])]
        holiday_df= holiday_df[holiday_df.DT.isin([self.today_str, ''])]
        self.logger.debug(holiday_df)
        self.logger.info("holiday_df.empty: %s ", holiday_df.empty)
        return (not holiday_df.empty)
    pass


    # @staticmethod
    # def get_snapshot_data_fields():
    #     return const.fields_snapshot
    #     pass

    @staticmethod
    def get_snapshot_params():
        return {const.str_start_date: const.int_zero, const.str_end_date: const.int_zero}
    pass

    def fetch_snapshot_data(self, rics, fields, pramas):
        try:
            data_df = self.ek.retireve_data(rics, fields, pramas)
            return self.process_snapshot_data(data_df)
        except Exception as e:
            logging.error(e, exc_info=True)
        pass

    def process_snapshot_data(self, df2):
        #convert to local time and filter out unexpected data
        df2[const.field_loc_dt] = pd.to_datetime(df2[const.field_cf_date] + " " + df2[const.field_cf_time])
        df2.set_index(const.field_loc_dt, drop=False, inplace=True)
        df2.index = df2.index.tz_localize(pytz.utc).tz_convert(self.local_tz)
        df2.drop(df2.columns[[len(const.fields_snapshot)+1]], axis=1, inplace=True)

        #drop CF_TIME but not CF_DATE
        df2.drop(df2.columns[[2]], axis=1, inplace=True)
        df2 = df2[df2.CF_DATE.isin([self.today_str, ''])]
        #df2.drop(df2.columns[[1]], axis=1, inplace=True)
        self.logger.debug(df2)
        return df2.fillna(const.minus_one_float)
        pass

    def get_local_time_zone(self):
        tz = self.conf.get_time_zone()
        self.logger.info("timezone is %s ", tz)
        return pytz.timezone(tz)
        pass

    def get_open_time_am(self):
        open_am = self.conf.get_open_time_am()
        self.logger.info("open_am: %s", open_am)
        return time.strftime(open_am)
    pass

    def get_close_time_am(self):
        close_am = self.conf.get_close_time_am()
        self.logger.info("close_am: %s", close_am)
        return time.strftime(close_am)
    pass

    def get_open_time_pm(self):
        open_pm = self.conf.get_open_time_pm()
        self.logger.info("open_pm: %s", open_pm)
        return time.strftime(open_pm)
    pass

    def get_close_time_pm(self):
        close_pm = self.conf.get_close_time_pm()
        self.logger.info("close_pm: %s", close_pm)
        return time.strftime(close_pm)
    pass

    def fetch_rics(self):
        try:
            chain_list = self.conf.get_chains()
            if len(chain_list) > 0:
                data_df = self.ek.retireve_rics_chain(chain_list)
            else:
                data_df = self.ek.retireve_rics(self.conf.get_exchange_id())
            #return self.process_data(data_df)
            proc_data_df = self.process_rics_data(data_df)
            modifiedTime = os.path.getmtime(self.conf.get_instruments_file())
            timeStamp = datetime.datetime.fromtimestamp(modifiedTime).strftime("%b-%d-%y-%H-%M-%S")
            os.rename(self.conf.get_instruments_file(), self.conf.get_instruments_file()+timeStamp)
            proc_data_df.to_csv(self.conf.get_instruments_file(), index=False)
            self.logger.info("get latest rics")
        except Exception as e:
            logging.error(e, exc_info=True)
            #proc_data_df.to_csv(self.conf.get_instruments_file(), index=False)
            self.logger.info("get latest rics")
        pass

    def fetch_adj_factors(self):
            ric_df = pd.read_csv(self.conf.get_instruments_file())
            values = []
            for index, row in ric_df.iterrows():
                try:
                    df_portion = self.ek.retireve_adj_factor(row['Instrument'])
                    if df_portion is None:
                        logging.error("ric %s is none adj factor", (row['Instrument']))
                        continue
                    df_portion = self.process_adj_data(df_portion)
                    values.append(df_portion)
                except Exception as e:
                    logging.error(e, exc_info=True)
            #return self.process_data(data_df)
            #data_df.columns = ['Instrument', 'exDivDate','adjFactor']
            data_df = pd.concat(values)

            modifiedTime = os.path.getmtime(self.conf.get_adj_factor_file())
            timeStamp = datetime.datetime.fromtimestamp(modifiedTime).strftime("%b-%d-%y-%H-%M-%S")
            os.rename(self.conf.get_adj_factor_file(), self.conf.get_adj_factor_file() + timeStamp)

            data_df.to_csv(self.conf.get_adj_factor_file(), index=False)
            logging.info("all done for adj factor")
    pass

    def check_trd_status (self,row):
        try:
            if row['TRD_STATUS'] == None:
                return 'Y'
            elif type(row['TRD_STATUS']) == str and  row['TRD_STATUS'].strip() == 'N':
                return 'Y'
            elif type(row['TRD_STATUS']) == float and math.isnan(row['TRD_STATUS']):  # == float('nan'):
                return 'Y'
            return 'N'
        except (Exception, ValueError) as excp:
            self.logger.error(excp, exc_info=True)
            return 'E'

    def process_adj_data (self, df):
        self.logger.debug('before process for size %s' % (len(df.index)))
        df = df[df['adjFactor'].notnull()]
        df = df[df['exDivDate'].notnull()]
        df = df[df['adjFactor'] != 1]
        df.sort_values(['exDivDate'], ascending=False, inplace=True)
        df['endDate'] = df['exDivDate']
        df.endDate = df.endDate.shift(1)
        df['accumAdjFactor'] = 1 / df['adjFactor']
        df.endDate.fillna("2999-01-01", inplace=True)
        df.sort_values(['exDivDate'], ascending=True, inplace=True)
        df['accumAdjFactor'] = df['accumAdjFactor'].cumprod()
        if "XNYS" == self.conf.get_exchange_id():
            df['secID'] = df.Instrument.str[:-1] + self.conf.get_exchange_id()
        else:
            df['secID'] = df.Instrument.str[:-2] + self.conf.get_exchange_id()
        #df['secID'] = df.Instrument.split(const.dot)[0] + const.dot + self.conf.get_exchange_id()
        self.logger.debug('after process for size %s' % (len(df.index)))
        return df

    def process_rics_data(self, df2):
        supplement_rics_list = self.conf.get_supplement_rics()
        if len(supplement_rics_list) > 0:
            for i in range(0, len(supplement_rics_list)):
                ric = supplement_rics_list[i]['value']
                df2 = df2.append([{'Instrument': ric}], ignore_index=True)
        # convert to local time and filter out unexpected data

        df2['Enable'] = df2.apply(self.check_trd_status, axis=1)
        df2.sort_values('Instrument')
        self.logger.debug(df2)
        return df2
        pass
if __name__ == "__main__":
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter(const.log_format)
    excode = 'XHKG'
    handler = TimedRotatingFileHandler(const.up_log_path + excode + "/"
                                       + str(os.getpid()) + '_adj_factor.log', when='midnight')
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    conf = ConfigManager(Validator(const.schema_file, logger), const.config_file)
    conf.get_exchange(excode)
    mds = MarketDataService(logger, conf)
    #mds.fetch_rics()
    mds.fetch_adj_factors()
   # instrumentlist = mds.get_ric_list()
    # print(instrumentlist)

    #is_holidays = mds.is_today_holiday()
    #print(is_holidays)

    # df = mds.fetch_snapshot_data(
    #     instrumentlist,
    #     MarketDataService.get_snapshot_data_fields(),
    #     MarketDataService.get_snapshot_params()
    # )
    #print(df)
    #df.to_csv("df_test.csv")
    pass
