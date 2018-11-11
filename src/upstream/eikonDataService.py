import eikon as tr
import pandas as pd
from util import const
import time

class EikonDataService():

    def __init__(self, app_key, logger):
        self.logger = logger
        tr.set_app_key(app_key),
    pass

    def retireve_data(self, rics, fields, params):
        length = len(rics)
        index = 0
        values = []
        for i in rics[0:length]:
            #if index == 0:
            j = index
            #else:
            #    j = index + 1
            index += const.ek_portion_num
            if j >= length:
                break
            if index > length:
                index = length
            try:
                self.logger.debug("rics length %s, now from %s to %s ", length, j, index)
                df_portion, err = tr.get_data(rics[j:index], fields, params )
                values.append(df_portion)
            except (Exception, ValueError) as excp:
                #print("Unable to get data for: %s. Is it a valid Equity RIC?" % ric)
                #print("Exception message: %s" % excp)
                self.logger.error("err happened for index from %s to %s", j, index)
                self.logger.error(excp, exc_info=True)
                #self.logger.error(err, exc_info=True)
        pass
        self.logger.debug("loop finish, index is  %s ", index)
        return pd.concat(values)
    pass

    def retireve_hist_data(self, ric, sdate, edate, cols='*', freq="daily"):
        try:
            self.logger.debug('%s , %s, %s' % (ric, sdate, edate))
            df = tr.get_timeseries( ric,
                                   fields=cols,
                                   interval = freq,
                                   start_date = sdate,
                                   end_date = edate,
                                   corax='unadjusted')
            time.sleep(2)
            return df
        except (Exception, ValueError) as excp:
            self.logger.error(excp, exc_info=True)
            time.sleep(2)

    def retireve_rics_chain(self, chain_list):
            try:
                values = []
                for i in range(0, len(chain_list)):
                    data_df, err = tr.get_data(chain_list[i]['value'],
                                            ['HALT_DATE', 'TRD_STATUS','EXPIR_DATE']
                                            )
                    values.append(data_df)
                return pd.concat(values)
            except (Exception, ValueError) as excp:
                self.logger.error(excp, exc_info=True)
                time.sleep(2)

    def retireve_rics(self, exchange_code, index=None, using_index=False):
            try:
                rics_list = None
                if using_index :
                    rics_list = '0#'+ index
                else:
                    rics_list =  'SCREEN(U(IN(Equity(active,public,primary,countryprimaryquote))/*UNV:Public*/), IN(TR.ExchangeMarketIdCode,"'+exchange_code+'"))'

                self.logger.debug('get instrument ric for %s' % (rics_list))

                df, err = tr.get_data( rics_list,
                    #['TR.CommonName', 'TR.IsDelistedQuote', 'TR.FirstTradeDate']
                     ['HALT_DATE', 'TRD_STATUS','TR.FirstTradeDate']
                     )
                return df
            except (Exception, ValueError) as excp:
                self.logger.error(excp, exc_info=True)
                time.sleep(2)

    def retireve_adj_factor(self, ric):
        try:
            self.logger.debug('get adj_factor for %s' % (ric))
            df, err = tr.get_data(ric,
                 ['TR.AdjmtFactorAdjustmentDate',
                                              'TR.AdjmtFactorAdjustmentFactor',
                                           'TR.AdjmtFactorAdjustmentType',
                                           'TR.AdjmtFactorIsApplied',
                                           'TR.AdjmtFactorUnderlyingEventId'],
                #     [ 'TR.CACorpActCurrency',
                #        'TR.CAExDate',
                #        'TR.CAAdjustmentFactor',
                #         'TR.CACorpActDesc'
                #     ],
                {'SDate': '0D', 'EDate': '-50AY'}
            )
            df.columns = ['Instrument','exDivDate', 'adjFactor','TR.AdjmtFactorAdjustmentType','TR.AdjmtFactorIsApplied','TR.AdjmtFactorUnderlyingEventId']
            self.logger.debug('adj_factor for %s size %s' % (ric, len(df.index)))
            return df
        except (Exception, ValueError) as excp:
            self.logger.error(excp, exc_info=True)
            time.sleep(5)
            df, err = tr.get_data(ric,
                                  ['TR.AdjmtFactorAdjustmentDate',
                                   'TR.AdjmtFactorAdjustmentFactor',
                                   'TR.AdjmtFactorAdjustmentType',
                                   'TR.AdjmtFactorIsApplied',
                                   'TR.AdjmtFactorUnderlyingEventId'],
                                  #     [ 'TR.CACorpActCurrency',
                                  #        'TR.CAExDate',
                                  #        'TR.CAAdjustmentFactor',
                                  #         'TR.CACorpActDesc'
                                  #     ],
                                  {'SDate': '0D', 'EDate': '-50AY'}
                                  )
            df.columns = ['Instrument', 'exDivDate', 'adjFactor', 'TR.AdjmtFactorAdjustmentType',
                          'TR.AdjmtFactorIsApplied', 'TR.AdjmtFactorUnderlyingEventId']
            self.logger.debug('adj_factor for %s size %s' % (ric, len(df.index)))
            return df