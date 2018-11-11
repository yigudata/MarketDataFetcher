from util import const, auxiliary


class InstrumentService:

    def __init__(self, logger, conf):
        self.logger = logger
        self.conf = conf
    pass

    def get_instrument_list(self,  enable_flag=True):
        instruments_file = self.conf.get_instruments_file()
        self.logger.info("instruments_file: %s", instruments_file)
        ric_dict = auxiliary.read_rics_to_dict(instruments_file, enable_flag)
        # ric_df = pd.read_csv(instruments_file)
        # ric_dict = dict([(i, [a, b]) for i, a, b in zip(ric_df.No, ric_df.Instrument, ric_df.Company)])
        # ric_dict = ric_df.to_dict(orient="index")
        return ric_dict['Instrument']

    pass
    # def get_hist_instrument_list(self, instruments_file):
    #     #instruments_file = self.conf.get_instruments_file()
    #     self.logger.info("instruments_file: %s", instruments_file)
    #     ric_dict = auxiliary.read_rics_to_dict(instruments_file)
    #     # ric_df = pd.read_csv(instruments_file)
    #     # ric_dict = dict([(i, [a, b]) for i, a, b in zip(ric_df.No, ric_df.Instrument, ric_df.Company)])
    #     # ric_dict = ric_df.to_dict(orient="index")
    #     return ric_dict['Instrument']
