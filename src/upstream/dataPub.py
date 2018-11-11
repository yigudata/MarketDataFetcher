import zmq
import time
from upstream.marketDataService import MarketDataService
import logging
from util import const, auxiliary
from cfg.validator import Validator
from cfg.cfgMgr import ConfigManager
from logging.handlers import TimedRotatingFileHandler
import os


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter(const.log_format)


def pub(excode, port="5558"):
    handler = TimedRotatingFileHandler(const.up_log_path + excode + "/"
                                       + str(os.getpid()) + const.data_pub_log, when='midnight')
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    if excode is None:
        logger.fatal("exchange code is None")
        raise SystemExit(0)

    conf = ConfigManager(Validator(const.schema_file, logger), const.config_file)
    conf.get_exchange(excode)

    context = zmq.Context()
    socket = context.socket(zmq.PUB)

    port = int(conf.get_port_num())
    socket.bind("tcp://*:%s" % port)

    logger.info("Running server on port: %s", port)

    mds = MarketDataService(logger, conf)
    mds.fetch_rics()
    if mds.is_today_holiday():
        socket.send_multipart([str.encode(excode), str.encode(const.holiday_text)])
        logger.fatal("It is holiday, Quit")
        raise SystemExit(0)

    open_pm = mds.get_open_time_pm()
    close_pm = mds.get_close_time_pm()
    open_am = mds.get_open_time_am()
    local_tz = mds.get_local_time_zone()
    close_am = mds.get_close_time_am()


    while True:
        now = auxiliary.get_now(local_tz)
        if now < open_am:
            logger.debug(const.exchange_not_open_text)
            time.sleep(const.interval)
            continue
        elif close_am < now < open_pm:
            logger.debug(const.exchange_lunch_break)
            time.sleep(const.interval)
            continue
        elif now > close_pm:
                # socket.send_multipart([str.encode(excode), str.encode(const.exchange_close_text)])
                logger.info(const.exchange_close_text)
                # raise SystemExit(0)
        pass

        rics = mds.get_ric_list()

        # Wait for next request from client
        df = mds.fetch_snapshot_data(rics, const.fields_snapshot,
                                     MarketDataService.get_snapshot_params())
        #logger.debug(df)
        if (df is None):
            logger.debug("empty df!")
            continue
        try:
            messagedatas = df.to_csv(header=None, index=True).strip('\n').split('\n')
            for messagedata in messagedatas:
                logger.debug("%s %s", excode, messagedata)
                socket.send_multipart([str.encode(excode), str.encode(messagedata)])

            if now > close_pm:
                socket.send_multipart([str.encode(excode), str.encode(const.exchange_close_text)])
                logger.info(const.exchange_close_text)
                raise SystemExit(0)
            pass
            time.sleep(const.interval)
        except Exception as excp:
            logger.error(excp, exc_info=True)
            time.sleep(const.interval)
