import zmq
import logging
from util import const
from cfg.validator import Validator
from cfg.cfgMgr import ConfigManager
from downstream.tickPersister import TickPersister
from logging.handlers import TimedRotatingFileHandler
import os

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter(const.log_format)


def sub(excode, port_push="5556", port_sub="5558"):
    handler = TimedRotatingFileHandler(const.down_log_path + excode+"/"+ str(os.getpid()) + const.data_sub_log,
                                       when='midnight', backupCount=10)
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    if excode is None:
        logger.fatal("exchange code is None")
        raise SystemExit(0)

    conf = ConfigManager(Validator(const.schema_file, logger), const.config_file)
    conf.get_exchange(excode)

    port_sub = int(conf.get_port_num())
    context = zmq.Context()
    socket_sub = context.socket(zmq.SUB)
    socket_sub.connect("tcp://localhost:%s" % port_sub)
    socket_sub.setsockopt(zmq.SUBSCRIBE, str.encode(excode))
    logger.info("Connected to publisher with port %s", port_sub)


    tp = TickPersister(logger, conf)
    while True:
        try:
            topic, msg = socket_sub.recv_multipart()
            logger.debug('Topic: %s, msg:%s' % (topic, msg))
            if const.holiday_text == msg.decode("utf-8"):
                logger.fatal("It is holiday, Quit")
                raise SystemExit(0)
            elif const.exchange_close_text == msg.decode("utf-8"):
                logger.info("Exchange closed, Quit")
                raise SystemExit(0)
            pass
            tp.write_tick(msg.decode("utf-8"), topic.decode("utf-8"))

        except (Exception, ValueError) as excp:
            logger.exception(excp, exc_info=True)
        pass
    pass