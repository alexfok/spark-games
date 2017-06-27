from __future__ import print_function
#
#

import sys
from random import random
from operator import add

import time
import sys
import os
import re
import collections
import json
import shutil
import glob
import argparse
import datetime
import logging
import logging.handlers
#import timeit
from timeit import default_timer as timer


# Global Configuration Parameters
LOGGER = None
LOG_FILENAME = '/var/log/cpdiag/cpdiag_files_feeder.log'

input_folder_nfs = "/mapr/cpdiag-stg.checkpoint.com/user/mapr/cp_in/data_in"
output_folder_nfs = "/mapr/cpdiag-stg.checkpoint.com/user/mapr/cp_in/data_in1"

def my_print(a):
    print (a)

def write_log(s, loglevel=logging.DEBUG):
    if loglevel==logging.DEBUG:
        logging.getLogger(__name__).debug(s)
    elif loglevel==logging.INFO:
        logging.getLogger(__name__).info(s)
    elif loglevel == logging.WARNING:
        logging.getLogger(__name__).warning(s)
    elif loglevel == logging.ERROR:
        logging.getLogger(__name__).error(s)
    elif loglevel == logging.CRITICAL:
        logging.getLogger(__name__).critical(s)

def init_logger(loglevel=logging.INFO):
    global LOGGER
#    logging.basicConfig(filename='cpdiag_parser.log', level=loglevel)
    logger = logging.getLogger(__name__)
    logger.setLevel(loglevel)
    # Add the log message handler to the logger
    # Console logger
    logger.addHandler(logging.StreamHandler())
    # Rotating file logger
    handler = logging.handlers.RotatingFileHandler(
        LOG_FILENAME, maxBytes=2000000000, backupCount=5)
    # create a logging format
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.info('Logger Initiated __name__: ' + __name__)
#    logger.info('Logger Initiated __name__: ')
    write_log('Process Starting', logging.INFO)
    LOGGER = logger

def parse_args():
    """Parse the command line for options."""
    parser = argparse.ArgumentParser(description='This is CPDiag files feeder job')
    parser.add_argument('--page_files_num', dest='page_files_num', action='store', default='100',help='page_files_num (100)')
    parser.add_argument('--sleep_interval', dest='sleep_interval', action='store', default='10',help='sleep_interval (10)')
    parser.add_argument('--loglevel', dest='loglevel', action='store', default=logging.WARNING,help='Log level (WARNING)')
    parser.add_argument('--input_folder_nfs', dest='input_folder_nfs', action='store', default='/mapr/cpdiag-stg.checkpoint.com/user/mapr/cp_in/data_in',help='input folder (/mapr/cpdiag-stg.checkpoint.com/user/mapr/cp_in/data_in)')
    parser.add_argument('--output_folder_nfs', dest='output_folder_nfs', action='store', default='/mapr/cpdiag-stg.checkpoint.com/user/mapr/cp_in/data_in1',help='output_folder_nfs (/mapr/cpdiag-stg.checkpoint.com/user/mapr/cp_in/data_in1)')
    parser.add_argument('--copy_mode', dest='copy_mode', action='store', default='c',help='copy (c) or move (m) files (c)')
    try:
        options = parser.parse_args()
        write_log(options, logging.INFO)
        # positional arguments are ignored
        return options
    except msg:
        parser.error(str(msg))

def is_interactive():
    import __main__ as main
    return not hasattr(main, '__file__')

def chunk_list(l,n):
   return [l[x: x+n] for x in xrange(0, len(l), n)]


if __name__ == "__main__":
    """
        Usage: pi [partitions]
    """
#    loglevel = logging.INFO
    loglevel = logging.DEBUG
#    params = []
    options = parse_args()
    init_logger(loglevel)
    write_log(options, logging.INFO)
    page_files_num = int(options.page_files_num)
    sleep_interval = int(options.sleep_interval)
    input_folder_nfs = options.input_folder_nfs
    output_folder_nfs = options.output_folder_nfs
    copy_mode = options.copy_mode

    print('Python version:', sys.version_info)
    print("Starting at: " + str(datetime.datetime.now()) + '\ninput_folder_nfs=' + input_folder_nfs + '\noutput_folder_nfs=' + output_folder_nfs)
    while True:
        try:
            done_files_numb = 0
            found_files_numb = 0
            start_time = timer()
            file_list_all = glob.glob(input_folder_nfs + '/*.json')
            found_files_numb = len(file_list_all)
            file_pages_list = chunk_list(file_list_all, page_files_num)
            write_log(str(datetime.datetime.now()) + 'START: found_files_numb=' + str(len(file_list_all)) + ', file_chunks=' + str(len(file_pages_list)), logging.INFO)
            for file_list in file_pages_list:
              done_files_numb += len(file_pages_list)
              for file in file_list:
                  filepath, filename = os.path.split(file)
                  if copy_mode == 'c':
                    write_log("Copy file: " + file + 'to: ' + os.path.join(output_folder_nfs, filename), logging.DEBUG)
                    shutil.copy(file, os.path.join(output_folder_nfs, filename))
                  else:
                    write_log("Move file: " + file + 'to: ' + os.path.join(output_folder_nfs, filename), logging.DEBUG)
                    shutil.move(file, os.path.join(output_folder_nfs, filename))
            end_time = timer()
            process_time = end_time - start_time
            write_log(str(datetime.datetime.now()) + ' END: found_files_numb=' + str(found_files_numb) + ' ,file_chunks=' + str(len(file_pages_list)) + ' ,done_files_numb=' + str(done_files_numb) + ' process_time=' + str(process_time), logging.INFO)
            if found_files_numb == 0:
                write_log(str(datetime.datetime.now()) + ' END2: process_time=' + str(process_time) + ' going to sleep for: '+ str((sleep_interval - process_time)), logging.INFO)
                time.sleep((sleep_interval - process_time))
        except:
            write_log(str(datetime.datetime.now()) + ' Main loop failed, trying again', logging.CRITICAL)
            time.sleep(10)

    write_log(str(datetime.datetime.now()) + ' Job finished Ok', logging.INFO)
