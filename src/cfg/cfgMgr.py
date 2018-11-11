from cfg.validator import Validator
from bs4 import BeautifulSoup
from util import const
import logging


class ConfigManager:
    """ This class is responsible for loading and validating the content of the XML configuration file """
    def __init__(self, validator, config_file):
        self.soup = BeautifulSoup(open(config_file), features=const.str_lxml)
        self.validator = validator
        self.exchange = None
    pass

    def is_valid(self, config_file):
       return self.validator.validate(config_file)

    def get_exchange(self, ex_code):
        self.exchange = self.soup.find(const.str_exchange, attrs={const.str_id: ex_code})
        if self.exchange['enable'] == const.false:
            raise ValueError('Exchange %s is disabled', ex_code)
        return self.exchange
    pass

    def get_eikon_key(self):
        return self.soup.global_market_config[const.str_eikon_key]

    def get_holiday_file(self):
        return self.soup.global_market_config[const.str_holiday_file]
    pass

    # def get_index(self):
    #     return self.exchange['index']
    #
    # def is_using_index(self):
    #     if self.exchange['using_index'] == const.true:
    #         return True
    #     return False

    def get_ccy(self):
        try:
            return self.exchange['ccy']
        except (Exception, ValueError) as excp:
            return self.exchange.parent['ccy']
    pass

    def get_instruments_file(self):
        return self.exchange['instruments_file']
    pass

    def get_tick_file_path(self):
        return self.exchange['tick_file_path']
    pass

    def get_day_file_path(self):
        return self.exchange['day_file_path']
    pass

    def get_hist_file_path(self):
        return self.exchange['hist_file_path']
    pass

    def get_hist_day_file_path(self):
        return self.exchange['hist_day_file_path']
    pass

    def get_adj_factor_file(self):
        return self.exchange['adj_factor_file']
    pass

    def get_time_zone(self):
        return self.exchange.parent['timezone']
    pass

    def get_port_num(self):
        return self.exchange['port']
    pass

    def get_open_time_am(self):
        return self.exchange['open_time_am']
    pass

    def get_open_time_pm(self):
        return self.exchange['open_time_pm']
    pass

    def get_close_time_am(self):
        return self.exchange['close_time_am']
    pass

    def get_close_time_pm(self):
        return self.exchange['close_time_pm']
    pass

    def get_exchange_id(self):
        return self.exchange['id']
    pass

    def get_chains(self):
        return self.exchange.findAll('chain')
    pass

    def get_supplement_rics(self):
        return self.exchange.findAll('ric')
    pass

def testLoadAndValidateXml():
    print ("testLoadAndValidateXml")
    config = ConfigManager(Validator(const.schema_file, logging.getLogger(__name__)), const.config_file)
    print("XML IS: ", config.is_valid(const.config_file))
    print("testLoadAndValidateXml done")
    print(config.get_eikon_key())
    exchange = config.get_exchange('XHKG')

    print(config.get_holiday_file())
    print(config.get_ccy())

    if(exchange != None ):
        print(exchange['id'])
        print(exchange['enable'])
        print(exchange['open_time_am'])
        print(exchange['close_time_pm'])
        print(exchange.parent['timezone'])
        print(exchange['instruments_file'])
        print(exchange['day_file_path'])
        print(exchange['tick_file_path'])
        list = exchange.findAll('chain')
        for i in range(0, len(list)):
            print(list[i]['value'])
        list2 = exchange.findAll('ric')
        for i in range(0, len(list2)):
            print(list2[i]['value'])
        print(exchange.find('index'))
    #print(config.get_chains())

    #exchange = config.get_exchange('XSHG')

if __name__ == "__main__":
    testLoadAndValidateXml()

pass
