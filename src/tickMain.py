import getopt
import sys
from multiprocessing import Process
import multiprocessing
from upstream import dataPub
from downstream import dataSub
from recordkeeping import processTicks
from util import const
import os
import logging
import time


if __name__ == '__main__':
    multiprocessing.freeze_support()
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
    pub = Process(target=dataPub.pub, args=(excode, const.server_pub_port,))
    sub = Process(target=dataSub.sub, args=(excode, const.server_push_port, const.server_pub_port,))
    pt = Process(target=processTicks.write_ticks, args=(excode,))
    print("%r main pid is %r" % (excode, os.getpid()))
    sys.stdout.flush()
    pub.start()
    sub.start()
    
    time.sleep(const.interval*5) #5min
    
    if pub.is_alive () and sub.is_alive():        
        pt.start()
    else:
        print("pub or sub terminated and no need start day file writer." )
    
    pub.join()
    sub.join()
    pt.join()
    pass
