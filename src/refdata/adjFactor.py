import pandas as pd
from util import const
import logging
from cfg.validator import Validator
from cfg.cfgMgr import ConfigManager


class AdjFactorService:

    def __init__(self, logger, conf):
        self.logger = logger
        self.conf = conf
        self.repo = pd.read_csv(conf.get_adj_factor_file())
    pass

    def get_accum_adj_factor(self, sec_code, exchange_code, trade_date):
        # sec_id = sec_code + const.dot + exchange_code
        sec_id = sec_code + exchange_code
        df = self.repo[self.repo.secID.isin([sec_id, ''])]
        if df.empty:
            return const.int_one

        df1 = df[(df[const.from_date] <= trade_date) & (df[const.to_date] > trade_date)]
        if df1.empty:
            df1 = df[(df[const.from_date] < trade_date) & (df[const.to_date] >= trade_date)]
        if df1.empty:
            self.logger.debug("find nothing for %s upon date:  %s, ", sec_id, trade_date)
            return const.int_one
        return df1.iloc[0][const.accum_adj_factor]


if __name__ == "__main__":
    logger = logging.getLogger(__name__)
    conf = ConfigManager(Validator('../cfg/cfg.xsd', logger), '../cfg/cfg.xml')
    conf.get_exchange('XSHG')
    adjServ = AdjFactorService(logger, conf)
    adj_factor = adjServ.get_accum_adj_factor('1', 'abc', '123-123-123')
    print(adj_factor)
    adj_factor = adjServ.get_accum_adj_factor('600000', 'XSHG', '2018-12-31')
    print(adj_factor)
