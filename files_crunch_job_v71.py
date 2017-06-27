from __future__ import print_function
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
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
import subprocess
from multiprocessing import Process
import socket


from pyspark.sql import SparkSession

input_folder_hd = "hdfs://cpdiag-stg.checkpoint.com/user/mapr/cp_in/current"

# Global Configuration Parameters
LOGGER = None
LOG_FILENAME = '/var/log/cpdiag/cpdiag_crunch_job.log'

input_folder_nfs = "/mapr/cpdiag-stg.checkpoint.com/user/mapr/cp_in/data_in"
input_folder_tmp_nfs = "/mapr/cpdiag-stg.checkpoint.com/user/mapr/cp_in/current"
input_folder_tmp_hd = "hdfs://cpdiag-stg.checkpoint.com/user/mapr/cp_in/current"
#input_folder_tmp_nfs = "/tmp/cpdiag/current"
#input_folder_tmp_hd = "file://tmp/cpdiag/current"
output_folder_hd = "hdfs://cpdiag-stg.checkpoint.com/user/mapr/cp_out/current"
done_folder_nfs = "/mapr/cpdiag-stg.checkpoint.com/user/mapr/cp_in/done"
bad_folder_nfs = "/mapr/cpdiag-stg.checkpoint.com/user/mapr/cp_in/bad"
skip_folder_nfs = "/mapr/cpdiag-stg.checkpoint.com/user/mapr/cp_in/skip"

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
#    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    _format = '[%(asctime)s][%(process)d][%(levelname)s][%(filename)s:%(lineno)d] %(message)s'
    _date_format = '%d/%m/%y %H:%M:%S'
    #        logging.basicConfig(filename=log_path, format=_format, datefmt=_date_format, level=logging.DEBUG)
    formatter = logging.Formatter(fmt=_format, datefmt=_date_format)

    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.info('Logger Initiated __name__: ' + __name__)
#    logger.info('Logger Initiated __name__: ')
    write_log('Process Starting', logging.INFO)
    LOGGER = logger

def parse_args(params):
    """Parse the command line for options."""
    parser = argparse.ArgumentParser(description='This is CPDiag Files Crunch job')
    parser.add_argument('--page_files_num', dest='page_files_num', action='store', default='1000',help='page_files_num (1000)')
    parser.add_argument('--sleep_interval', dest='sleep_interval', action='store', default='10',help='sleep_interval (10)')
    parser.add_argument('--loglevel', dest='loglevel', action='store', default=logging.WARNING,help='Log level (WARNING)')
    try:
        options = parser.parse_args(params)
        write_log(options, logging.INFO)
        # positional arguments are ignored
        return options
    except msg:
        parser.error(str(msg))



def add_json_to_hadoop(json_dict, filename, table_name):
    try:
    #    print("Start add_json_to_hadoop filename: " + filename + "table_name: " + table_name)

        json_dict[table_name] += filename + "\n"
#        with open(filename, 'r') as f:
#            json_dict[table_name] += f.read() + "\n"
    #df = sqlContext.read.json(os.path.join(input_folder_hadoop, filename))
    #df.write.mode('append').partitionBy('id').parquet(os.path.join(output_folder_hadoop, table_name))
    #df.write.mode('append').parquet(os.path.join(output_folder_hadoop, table_name))
    except:
        return False
    return True
	
def loop_on_dir(spark, input_folder_nfs, input_folder_tmp_hd, input_folder_tmp_nfs, output_folder_hd, file_list):
    file_count=0
    failed_count=0
    bad_files=[]
    done_files=[]
    found_files=[]
    write_log("Start loop_on_dir " + input_folder_nfs)
#    json_dict = {}
    json_dict = collections.defaultdict(str)
    json_regex = re.compile(r'(.+)_\d+_\d+\.json')
    # Discover json tables loop
    for filename in file_list:
        path, file_n = os.path.split(filename)
        m = json_regex.match(file_n)
        if m:
            table_name = m.group(1)
            write_log("loop_on_dir: Parse filename + table_name: " + filename + ' ' + table_name)
            if add_json_to_hadoop(json_dict, filename, table_name):
                file_count+=1
                found_files.append(filename)
            else:
                bad_files.append(filename)
                write_log("add_json_to_hadoop: Failed to parse file - bad file: " + filename, logging.ERROR)
#            print (m.group(1))
        else:
            bad_files.append(filename)
            write_log("loop_on_dir: Bad file name - skip file: " + filename, logging.ERROR)

    # Save json to parquet tables loop
    for key, value in json_dict.items():
        try:
            filename = input_folder_tmp_nfs + '/' + key + '.json'
            write_log("loop_on_dir: writing temp JSON filename + key: " + filename + ' ' + key)
    #        json_files = value.split('\n')
            json_files = value.split()
            for json_file in json_files:
    #            write_log("loop_on_dir: cat json_file: " + json_file + " to temp JSON filename + key: " + filename + ' ' + key, logging.DEBUG)
                os.system("cat " + json_file + " >> " + filename)
    #        with open(filename, 'w+') as f:
    #            f.write(value)
            hd_file_in = input_folder_tmp_hd + '/' + key + '.json'
            hd_file_out = output_folder_hd + '/' + key + '.parquet'
            write_log("loop_on_dir: saving to parquet file hd_file_in + hd_file_out: " + hd_file_in + ' ' + hd_file_out, logging.DEBUG)
            write_log("loop_on_dir: start read json saving to parquet file hd_file_in + hd_file_out: " + hd_file_in + ' ' + hd_file_out, logging.DEBUG)
            df = spark.read.json(hd_file_in)
            df.coalesce(2)
            # df.write.mode('append').partitionBy('id').parquet(os.path.join(output_folder_hadoop, table_name))
    #        df.write.mode('overwrite').parquet(hd_file_out)
            write_log("loop_on_dir: start save to parquet saving to parquet file hd_file_in + hd_file_out: " + hd_file_in + ' ' + hd_file_out, logging.DEBUG)
#            df.repartition(20)
    #        df.write.mode('append').partitionBy('cpdiag_date').parquet(hd_file_out)
#            df.write.mode('append').parquet(hd_file_out)

            try:
                df.write.mode('append').partitionBy('cpdiag_date').parquet(hd_file_out)
            except:
#                try:
                write_log("failed to write partitioned file: " + os.path.join(input_folder_nfs, filename) + ' trying to write without partition', logging.WARNING)
                df.write.mode('append').parquet(hd_file_out)
                write_log("succeeded write of parquet without partition hd_file_out=" + hd_file_out, logging.INFO)
#                except:
#                    write_log('failed aggregation of parquet hd_file_out=' + hd_file_in + ' reason=parquet write failed', logging.ERROR)
#                    failed_count += len(json_files)
#                    bad_files += json_files
#                    write_log('Save json to parquet tables loop failed, key=' + key + ', failed_count=' + str(failed_count), logging.ERROR)

            write_log("loop_on_dir: finish save to parquet saving to parquet file hd_file_in + hd_file_out: " + hd_file_in + ' ' + hd_file_out, logging.DEBUG)
            done_files += json_files
    #        write_log("loop_on_dir: remove temp JSON filename + key: " + filename + ' ' + key)
    #        shutil.rmtree(filename)
        except:
            failed_count += len(json_files)
            bad_files += json_files
            write_log('Save json to parquet tables loop failed, key='+ key + ', failed_count=' + str(failed_count), logging.ERROR)

    write_log("Finished loop_on_dir " + input_folder_nfs + ", file_count=" + str(file_count) + ", failed_count=" + str(failed_count))
    write_log("Finished loop_on_dir, found_files=" + str(len(found_files)) + ", bad_files=" + str(len(bad_files)) + ", done_files=" + str(len(done_files)))
    return bad_files, done_files, found_files
    
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
    params = []
    options = parse_args(params)
    init_logger(loglevel)
    page_files_num = int(options.page_files_num)
    sleep_interval = int(options.sleep_interval)
    spark = SparkSession\
        .builder\
        .appName("FilesCrunchingJob")\
        .getOrCreate()
		
    print('Python version:', sys.version_info)
    print("Starting at: " + str(datetime.datetime.now()))
    '''
    partitions = int(sys.argv[1]) if len(sys.argv) > 1 else 2
    n = 100000 * partitions
    def f(_):
        x = random() * 2 - 1
        y = random() * 2 - 1
        return 1 if x ** 2 + y ** 2 < 1 else 0
    count = spark.sparkContext.parallelize(range(1, n + 1), partitions).map(f).reduce(add)
    print("Pi is roughly %f" % (4.0 * count / n))
    '''
    while True:
        try:
            bad_files_numb = 0
            done_files_numb = 0
            found_files_numb = 0
            skip_files_numb = 0
            bad_files = []
            done_files = []
            found_files = []

            start_time = timer()
#            input_folder_tmp_nfs = input_folder_tmp_nfs + '_' + socket.gethostname()
            write_log('Starting: Remove input_folder_tmp_nfs=' + input_folder_tmp_nfs, logging.INFO)
            # shutil.rmtree(input_folder_tmp_nfs + '/*')
            os.system("rm -f " + input_folder_tmp_nfs + "/*")
            file_list_all = glob.glob(input_folder_nfs + '/*.json')
            found_files_numb = len(file_list_all)
            file_pages_list = chunk_list(file_list_all, page_files_num)
#            write_log('Found: ' + len(file_list_all) + 'number of file to proceed in munber of chunks: ' + len(file_pages_list), logging.INFO)
            write_log('START: found_files_numb=' + str(len(file_list_all)) + ', file_chunks=' + str(len(file_pages_list)), logging.INFO)
            # Loop on files page
            for file_list in file_pages_list:
              bad_files, done_files, found_files = loop_on_dir(spark, input_folder_nfs, input_folder_tmp_hd, input_folder_tmp_nfs, output_folder_hd, file_list)
              write_log("Return from loop_on_dir, found_files=" + str(len(found_files)) + ", bad_files=" + str(len(bad_files)) + ", done_files=" + str(len(done_files)), logging.DEBUG)
              bad_files_numb += len(bad_files)
              done_files_numb += len(done_files)
              # Move processed files to done_folder_nfs
              write_log("Return1 from loop_on_dir, found_files=" + str(len(found_files)) + ", bad_files=" + str(len(bad_files)) + ", done_files=" + str(len(done_files)), logging.DEBUG)
              for file in done_files:
                  filepath, filename = os.path.split(file)
                  write_log("Moving done file: " + file + 'to: ' + os.path.join(done_folder_nfs, filename), logging.DEBUG)
                  shutil.move(file, os.path.join(done_folder_nfs, filename))
              write_log("Return2 from loop_on_dir, found_files=" + str(len(found_files)) + ", bad_files=" + str(len(bad_files)) + ", done_files=" + str(len(done_files)), logging.DEBUG)
              # Move bad files to bad_folder_nfs
              for file in bad_files:
                  filepath, filename = os.path.split(file)
                  write_log("Moving bad file: " + file + 'to: ' + os.path.join(bad_folder_nfs, filename))
                  shutil.move(file, os.path.join(bad_folder_nfs, filename))
              write_log('PAGE_END: found_files_numb=' + str(found_files_numb) + ' ,file_chunks=' + str(len(file_pages_list)) + ' ,done_files_numb=' + str(done_files_numb) + ' ,bad_files_numb=' + str(bad_files_numb), logging.INFO)
              # Fool proof - Remove all temporary files in input_folder_tmp_nfs
              write_log('Remove input_folder_tmp_nfs=' + input_folder_tmp_nfs, logging.DEBUG)
#              shutil.rmtree(input_folder_tmp_nfs + '/*')
              os.system("rm -f " + input_folder_tmp_nfs + "/*")
            #            skip_files = glob.glob(input_folder_nfs + '/*')
#            for file in skip_files:
#                filepath, filename = os.path.split(file)
#                write_log("Moving skip_file: " + file + 'to: ' + os.path.join(skip_folder_nfs, filename), logging.DEBUG)
#                shutil.move(file, os.path.join(skip_folder_nfs, filename))
            end_time = timer()
            process_time = end_time - start_time
#            print('process_time: ' + str(process_time) + ' end_time ' + str(end_time) + ' start_time ' + str(start_time))
            write_log('END1: found_files_numb=' + str(found_files_numb) + ' ,file_chunks=' + str(len(file_pages_list)) + ' ,done_files_numb=' + str(done_files_numb) + ' ,bad_files_numb=' + str(bad_files_numb) + ' process_time=' + str(process_time), logging.INFO)
#            if (sleep_interval - process_time) > 0:
            if found_files_numb == 0:
                write_log('END2: process_time=' + str(process_time) + ' going to sleep for: '+ str((sleep_interval - process_time)), logging.INFO)
                time.sleep((sleep_interval - process_time))
        except:
            write_log('Main loop failed, trying again', logging.CRITICAL)
#            spark.stop()
#            exit(-1)
            time.sleep(10)

    write_log('Job finished Ok', logging.INFO)
    spark.stop()
