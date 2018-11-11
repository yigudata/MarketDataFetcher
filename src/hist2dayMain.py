import getopt
import sys
import multiprocessing
from util import const
import os
import logging
from cfg.validator import Validator
from cfg.cfgMgr import ConfigManager
from logging.handlers import TimedRotatingFileHandler
from historical import processHist

if __name__ == '__main__':
    multiprocessing.log_to_stderr(logging.DEBUG)
    opts, args = getopt.getopt(sys.argv[1:], 'he:', ['excode=', 'help'])

    excode = None

    for key, value in opts:

        if key in ['-h', '--help']:
            print('download tick data')
            print('arg：')
            print('-h\t help')
            print('-e\t exchange code as ISO10383_MIC')
            raise SystemExit(0)
        if key in ['-e', '--excode']:
            excode = value.upper()

    # Process(target=server_push, args=(server_push_port,)).start()

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter(const.log_format)

    handler = TimedRotatingFileHandler(const.hist_log_path + excode + "/"
                                       + str(os.getpid()) + const.hist_2_day_file_log, when='midnight')
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    logger.info("%r main pid is %r" % (excode, os.getpid()))
    conf = ConfigManager(Validator(const.schema_file, logger), const.config_file)
    conf.get_exchange(excode)

    processHist.process(conf, logger)

    pass
