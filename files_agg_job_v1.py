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
from datetime import date, timedelta
import subprocess
from multiprocessing import Process


from pyspark.sql import SparkSession
from pyspark.sql import Row

input_folder_nfs = "/mapr/cpdiag-stg.checkpoint.com/user/mapr/cp_out/current"
input_folder_hd = "hdfs://cpdiag-stg.checkpoint.com/user/mapr/cp_out/current"
output_folder_hd = "hdfs://cpdiag-stg.checkpoint.com/user/mapr/cp_out/agg"
output_folder_nfs = "/mapr/cpdiag-stg.checkpoint.com/user/mapr/cp_out/agg"
done_folder_nfs = "/mapr/cpdiag-stg.checkpoint.com/user/mapr/cp_out/done_agg"
# Global Configuration Parameters
LOGGER = None
LOG_FILENAME = '/var/log/cpdiag/files_agg_job.log'

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
#    def init_logger(log_path):
    _format = '[%(asctime)s][%(process)d][%(levelname)s][%(filename)s:%(lineno)d] %(message)s'
    _date_format = '%d/%m/%y %H:%M:%S'
#        logging.basicConfig(filename=log_path, format=_format, datefmt=_date_format, level=logging.DEBUG)

    formatter = logging.Formatter(fmt=_format, datefmt=_date_format)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.info('Logger Initiated __name__: ' + __name__)
#    logger.info('Logger Initiated __name__: ')
    write_log('Process Starting', logging.INFO)

def parse_args():
    """Parse the command line for options."""
    parser = argparse.ArgumentParser(description='This is CPDiag Files Crunch job')
    parser.add_argument('--report_interval', '-i', dest='report_interval', action='store', default='1',help='report_interval (1)')
    parser.add_argument('--sleep_interval', dest='sleep_interval', action='store', default='10',help='sleep_interval (10)')
    parser.add_argument('--loglevel', dest='loglevel', action='store', default=logging.WARNING,help='Log level (WARNING)')
    parser.add_argument('--mode', '-m', choices=('aggregate', 'report', 'all', 'test','schema_stats'), default='report', help="Enter the required mode - 'report' - generate reports, 'aggregate' - aggregate partitioned data, 'all' - perform ALL operations (report)")
    parser.add_argument('--overwrite_rep_file', dest='overwrite_rep_file', action='store_true', default=False, help='Overwrite or report sumamry file (False)')

    try:
#        options = parser.parse_args(params)
        options, unknown = parser.parse_known_args()
        print(options)
        print(unknown)
        write_log(options, logging.INFO)
        write_log(unknown, logging.INFO)

        write_log(options, logging.INFO)
        # positional arguments are ignored
        return options
    except msg:
        parser.error(str(msg))

def flat_map_json(x):
 return [each for each in json.loads(x[1])]

def report(options, spark):
    try:
        # Creates a temporary view using the DataFrame
        #        hd_file_in="hdfs://cpdiag-stg.checkpoint.com/user/mapr/cp_out/current/cpdiag_data.parquet"
#        hd_file_in = "hdfs://cpdiag-stg.checkpoint.com/user/mapr/cp_in/data_in2/cpdiag_data*.json"
        hd_file_res = "hdfs://cpdiag-stg.checkpoint.com/user/mapr/cp_out/tmp/reports"
        nfs_file_res = "/mapr/cpdiag-stg.checkpoint.com/user/mapr/cp_out/tmp/reports"
        nfs_file_summary_res = "/mapr/cpdiag-stg.checkpoint.com/user/mapr/cp_out/tmp/summary"
        rep_sum_file_name = nfs_file_summary_res + "/rep_sum_file.csv"

        report_start_date = date.today() - timedelta(int(options.report_interval))
        report_start_date_str = report_start_date.strftime('%Y-%m-%d')

        write_log("remove results directory nfs_file_res=" + nfs_file_res, logging.DEBUG)
        ##########################################################
        # Clean and init report directories
        os.system("rm -rf " + nfs_file_res + "/*")
#        os.system("rm -f " + nfs_file_summary_res + "/fw_version.csv")

        ##############################################
        # Initiate  rep_sum_file summary file
        # report_time, total_reports_count, last_reports_count
        report_time_str = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        write_log("initializing rep_sum_file_name=" + rep_sum_file_name , logging.DEBUG)
        write_log("initializing rep_sum_file_name=" + rep_sum_file_name + " options.overwrite_rep_file=" + str(options.overwrite_rep_file), logging.DEBUG)
        write_log("initializing rep_sum_file_name=" + rep_sum_file_name + " options.overwrite_rep_file=" + str(options.overwrite_rep_file) + " report_time_str=" + report_time_str, logging.DEBUG)

        if options.overwrite_rep_file:
            write_log("overwrite and open rep_sum_file_name=" + rep_sum_file_name, logging.DEBUG)
            rep_sum_file = open(rep_sum_file_name, "w")
            write_log("write header line to rep_sum_file_name=" + rep_sum_file_name, logging.DEBUG)
            rep_sum_file.write("report_time, total_reports_count, last_reports_count" + "\n")
        else:
            write_log("open rep_sum_file_name=" + rep_sum_file_name, logging.DEBUG)
            rep_sum_file = open(rep_sum_file_name, "a+")


        ########################################################
        #  Cache tables in Spark and use tables. Result - total_reports_count
        table_name = 'cpdiag_data'
        query_str = "SELECT count(*) as total_reports_count FROM " + table_name
        # Use cached tables - total_reports_count
        results, tables = query([table_name], query_str, True, spark)
        total_reports_count = results[0]['total_reports_count']
        write_log(">> query returned total_reports_count=" + str(total_reports_count), logging.INFO)

        #  Use cached tables. Result -  last_reports_count
        ##############################################
        # select today reports counter.
        write_log("select reports_count since report_start_date_str=" + report_start_date_str, logging.DEBUG)
        query_str = "SELECT count(*) as last_reports_count FROM " + table_name +  " WHERE cpdiag_date >= " + '\'' + report_start_date_str + '\''
        results = query_cached_tables(tables, query_str, spark)
        last_reports_count = results[0]['last_reports_count']
        write_log(">> query returned last_reports_count=" + str(last_reports_count), logging.INFO)

        '''
        # Read Data input from file
        hd_file_in = "hdfs://cpdiag-stg.checkpoint.com/user/mapr/cp_out/current/cpdiag_data.parquet"
        write_log("start read from hd_file_in=" + hd_file_in, logging.DEBUG)
#        dfp = spark.read.json(hd_file_in)
        dfp = spark.read.parquet(hd_file_in)

        dfp.createOrReplaceTempView("cpdiag_data_view")
        #        dfp.printSchema()
        ##############################################
        # select total reports counter.
        write_log("select total_reports_count", logging.DEBUG)
        results = spark.sql("SELECT count(*) as total_reports_count FROM cpdiag_data_view")
        results.show()
        total_reports_count = results.rdd.map(lambda x: x.total_reports_count).first()
        write_log("total_reports_count=" + str(total_reports_count), logging.DEBUG)
        # hd_file_out = hd_file_res + "/total_reports_count.csv"
        # write_log("write to hd_file_out=" + hd_file_out, logging.DEBUG)
        # results.repartition(1).write.option("header", "true").csv(hd_file_out)

        ##############################################
        # select today reports counter.
        write_log("select reports_count since report_start_date_str=" + report_start_date_str, logging.DEBUG)
#        results = dfp.filter(dfp['cpdiag_date'] >= report_start_date_str)
#        results = dfp.filter(dfp['cpdiag_date'] >= report_start_date_str).count()
        results = spark.sql("SELECT count(*) as today_reports_count FROM cpdiag_data_view WHERE cpdiag_date >= " + '\'' + report_start_date_str     + '\'')
#        results = spark.sql("SELECT count(*) as today_reports_count FROM cpdiag_data_view WHERE cpdiag_date >= '2017-03-20'")
        results.show()
        last_reports_count = results.rdd.map(lambda x: x.today_reports_count).first()
        write_log("last_reports_count=" + str(last_reports_count), logging.DEBUG)
        # hd_file_out = hd_file_res + "/today_reports_count.csv"
        # write_log("write to hd_file_out=" + hd_file_out, logging.DEBUG)
        # results.repartition(1).write.option("header", "true").csv(hd_file_out)
        '''
        ##############################################
        # select fw_version reports counter.
#        write_log("select fw_version groupBy(fw_version)", logging.DEBUG)
#        results = spark.sql("SELECT fw_version FROM cpdiag_data_view WHERE cpdiag_date = '2017-03-27'").groupBy(
#            "fw_version").count()  # group by fw_version,mac")
#        results.show()
        dfp=tables['cpdiag_data']
        write_log("filter fw_version by cpdiag_date groupBy(fw_version) report_start_date_str=" + report_start_date_str, logging.DEBUG)
        # '2017-03-27'
        results = dfp.filter(dfp['cpdiag_date'] >= report_start_date_str ).groupBy("fw_version").count()  # group by fw_version,mac")
        #        results1 = spark.sql("SELECT fw_version FROM cpdiag_data_view WHERE cpdiag_date = '2017-03-27'").groupBy("fw_version").count()# group by fw_version,mac")
        results.show()
        hd_file_out = hd_file_res + "/fw_version.csv"
        write_log("write to hd_file_out=" + hd_file_out, logging.DEBUG)
        results.repartition(1).write.option("header", "true").csv(hd_file_out)
        fw_sum_file_name = nfs_file_summary_res + "/fw_version.csv"
#        write_log("copy nfs_file_res=" + nfs_file_res + "/fw_version.csv/part*" + " file to fw_sum_file_name=" + fw_sum_file_name, logging.DEBUG)
#        shutil.copy(nfs_file_res + "/fw_version.csv/part*", fw_sum_file_name)
        write_log("copy nfs_file_res=" + nfs_file_res + "/fw_version.csv/part*" + " file to fw_sum_file_name=" + fw_sum_file_name,logging.DEBUG)
#        shutil.copy(nfs_file_res + "/hardware.csv/part*", fw_sum_file_name)
        os.system("cat " + nfs_file_res + "/fw_version.csv/*.csv" + " > " + fw_sum_file_name)

#        file_list = glob.glob(nfs_file_res + "/fw_version.csv/*.csv")
#        for file in file_list:
#            write_log("cat file=" + file + " to fw_sum_file_name=" + fw_sum_file_name,logging.DEBUG)
#            os.system("cat " + file + " >> " + fw_sum_file_name)


        ##############################################
        hd_file_in = "hdfs://cpdiag-stg.checkpoint.com/user/mapr/cp_out/current/flat_info_data.parquet"
        write_log("start read from hd_file_in=" + hd_file_in, logging.DEBUG)
        #        dfp = spark.read.json(hd_file_in)
        df_fid = spark.read.parquet(hd_file_in)

        df_fid.createOrReplaceTempView("flat_info_data")
        df_fid.printSchema()
        ##############################################
        # select fw_version reports counter.
#        write_log("select fw_version groupBy(fw_version)", logging.DEBUG)
#        results = spark.sql("SELECT fw_version FROM cpdiag_data_view WHERE cpdiag_date = '2017-03-27'").groupBy(
#            "fw_version").count()  # group by fw_version,mac")
#        results.show()
        write_log("filter hardware by cpdiag_date groupBy(hardware) report_start_date_str=" + report_start_date_str, logging.DEBUG)
        # '2017-03-27'
        results1 = df_fid.filter(df_fid['cpdiag_date'] >= report_start_date_str ).groupBy("hardware").count()
#        results = df_fid.filter(dfp['cpdiag_date'] >= report_start_date_str ).groupBy("hardware")  # group by fw_version,mac")
        #        results1 = spark.sql("SELECT fw_version FROM cpdiag_data_view WHERE cpdiag_date = '2017-03-27'").groupBy("fw_version").count()# group by fw_version,mac")
        results1.show()
        hd_file_out = hd_file_res + "/hardware.csv"
        write_log("write to hd_file_out=" + hd_file_out, logging.DEBUG)
        results1.repartition(1).write.option("header", "true").csv(hd_file_out)
        hw_sum_file_name = nfs_file_summary_res + "/hardware.csv"
        write_log("copy nfs_file_res=" + nfs_file_res + "/hardware.csv/part*" + " file to hw_sum_file_name=" + hw_sum_file_name,logging.DEBUG)
#        shutil.copy(nfs_file_res + "/hardware.csv/part*", fw_sum_file_name)
        os.system("cat " + nfs_file_res + "/hardware.csv/*.csv" + " > " + hw_sum_file_name)

        write_log("write data line to rep_sum_file_name=" + rep_sum_file_name, logging.DEBUG)
        rep_sum_file.write(report_time_str +',' + str(total_reports_count) +',' + str(last_reports_count) + "\n")

        # joins example
        # Queries can then join DataFrame data with data stored in Hive.
        # spark.sql("SELECT * FROM records r JOIN src s ON r.key = s.key").show()
    except:
        write_log(str(datetime.datetime.now()) + ' report failed', logging.CRITICAL)
    return

def query(table_names, query, cache_tables, spark):
    result_list = []
    df_tables = {}
    try:
        # compose parquet file FQDN
        # compose SQL query
        # extract result set into result_set
        write_log('query: ' + query + ' table_names: ' + str(table_names), logging.DEBUG)
        for table_name in table_names:
            ##############################################
            hd_file_in = "hdfs://cpdiag-stg.checkpoint.com/user/mapr/cp_out/current/" + table_name + ".parquet"
            write_log("start read from hd_file_in=" + hd_file_in, logging.DEBUG)
            if cache_tables:
                df_tables[table_name] = spark.read.parquet(hd_file_in).cache()
            else:
                df_tables[table_name] = spark.read.parquet(hd_file_in)
            df_tables[table_name].createOrReplaceTempView(table_name)
#            df_tables[table_name].printSchema()
        result_set = spark.sql(query)
#        result_set.show()
        columns = result_set.schema.names
        write_log('query columns: ' + str(columns), logging.INFO)
        for result in result_set.collect():
            result_list.append(result.asDict())
        write_log("len(result_list)=" + str(len(result_list)), logging.DEBUG)
        write_log('result_list[0]: ' + str(result_list[0]), logging.DEBUG)
        ##############################################
    except:
        write_log(str(datetime.datetime.now()) + ' query failed:' + query + 'table_names:' + str(table_names), logging.CRITICAL)
        return None, None
    return result_list, df_tables

def query_cached_tables(df_tables, query, spark):
    result_list = []
    try:
        # compose parquet file FQDN
        # compose SQL query
        # extract result set into result_set
        write_log('query_cached_tables:' + query, logging.DEBUG)
        for table_name, df_table  in df_tables.iteritems():
            write_log('query_cached_tables: ' + query + ' table_name: ' + str(table_name), logging.DEBUG)
#            df_tables[table_name].printSchema()
        result_set = spark.sql(query)
#        result_set.show()
        columns = result_set.schema.names
        write_log('query_cached_tables columns: ' + str(columns), logging.INFO)
        for result in result_set.collect():
#            for col in columns:
            result_list.append(result.asDict())
        write_log("len(result_list)=" + str(len(result_list)), logging.DEBUG)
        write_log('result_list[0]: ' + str(result_list[0]), logging.DEBUG)
    except:
        write_log(str(datetime.datetime.now()) + 'query_cached_tables failed: ' + query, logging.CRITICAL)
    return result_list

# Add result_set as page to weekly
def add_page_to_weekly(result_set, options, spark):
    try:
        # Add result_set as page to weekly
        write_log('add_page_to_weekly', logging.DEBUG)
    except:
        write_log(str(datetime.datetime.now()) + ' add_page_to_weekly failed', logging.CRITICAL)
    return result_set

def aggregate(options, spark):
    try:
        os.system("rm -rf " + output_folder_nfs + "/*")
        os.system("rm -rf " + done_folder_nfs + "/*")
        file_list = glob.glob(input_folder_nfs + '/*')
        write_log('START: in input_folder_nfs=' + input_folder_nfs + ' found_files_numb=' + str(len(file_list)), logging.INFO)
        working_file=""
        bad_files=[]
        good_files=[]
        for hd_file_in in file_list:
            try:
                # Read parquet to DF
                # Repartition DF
                # Write to output_folder_hd
                #
                columns = ""
                filepath, filename = os.path.split(hd_file_in)
                hd_file_in = input_folder_hd + '/' + filename
                working_file = hd_file_in
                write_log("read parquet hd_file_in=" + hd_file_in, logging.DEBUG)
                df = spark.read.parquet(hd_file_in)
                columns = df.columns
#                write_log("finish read from parquet hd_file_in=" + hd_file_in, logging.DEBUG)
                hd_file_out = output_folder_hd + '/' + filename
                # df.write.mode('append').partitionBy('id').parquet(os.path.join(output_folder_hadoop, table_name))
                #        df.write.mode('overwrite').parquet(hd_file_out)
                write_log("save parquet hd_file_out=" + hd_file_out, logging.DEBUG)
    #            df.repartition(2)
                df.coalesce(10)
                try:
                    df.write.mode('overwrite').partitionBy('cpdiag_date').parquet(hd_file_out)
                    good_files.append(filename)
#                    df.write.mode('overwrite').parquet(hd_file_out)
#                    write_log("finish aggregation of parquet hd_file_out=" + hd_file_out, logging.DEBUG)
                except:
                    try:
                        write_log("failed to write partitioned file: " + os.path.join(input_folder_nfs, filename) + ' trying to write without partition', logging.WARNING)
                        df.write.mode('overwrite').parquet(hd_file_out)
                        write_log("succeeded aggregation of parquet without partition hd_file_out=" + hd_file_out, logging.INFO)
                        good_files.append(filename)
                    except:
                        write_log('failed aggregation of parquet hd_file_out=' + hd_file_in + ' reason=parquet write failed', logging.ERROR)
                        bad_files.append((filename, columns))
            except:
                write_log('failed aggregation of parquet hd_file_out=' + hd_file_in + ' reason=parquet read failed', logging.ERROR)
                bad_files.append((filename, columns))

        for good_file_name in good_files:
            # TODO:
            # move original file to tmp directory
            # move aggregated file to original
            write_log("moving original file: " + os.path.join(input_folder_nfs, good_file_name) + 'to: ' + os.path.join(done_folder_nfs, good_file_name), logging.DEBUG)
            shutil.move(os.path.join(input_folder_nfs, good_file_name), os.path.join(done_folder_nfs, good_file_name))
            write_log("moving done file: " + os.path.join(output_folder_nfs, good_file_name) + 'to: ' + os.path.join(input_folder_nfs, good_file_name), logging.DEBUG)
            shutil.move(os.path.join(output_folder_nfs, good_file_name), os.path.join(input_folder_nfs, good_file_name))

        write_log('\n********************************\nFailed files list:\n', logging.ERROR)
        for bad_file_t in bad_files:
            write_log('failed aggregation of parquet hd_file_out=' + bad_file_t[0] + ' table columns:  ' + str(bad_file_t[1]), logging.ERROR)
    except:
        write_log(' aggregate failed: working_file='+ working_file, logging.CRITICAL)
    write_log('\n********************************\n', logging.ERROR)
    # spark.stop()
    #            exit(-1)
    return

def all(options, spark):
    report(options, spark)
    aggregate(options, spark)
    test(options,spark)
    schema_stats(options, spark)
    return

def schema_stats(options, spark):
    try:
        file_list = glob.glob(input_folder_nfs + '/*')
        write_log(str(
            datetime.datetime.now()) + 'START: in input_folder_nfs=' + input_folder_nfs + ' found_files_numb=' + str(
            len(file_list)), logging.INFO)
        working_file = ""
#        tables_dict = {}
        tables_dict = collections.defaultdict(dict)
        for hd_file_in in file_list:
            # Read parquet to DF
            # Repartition DF
            # Write to output_folder_hd
            #
            filepath, filename = os.path.split(hd_file_in)

            hd_file_in = input_folder_hd + '/' + filename
            working_file = hd_file_in
#                write_log("start read from parquet hd_file_in=" + hd_file_in, logging.DEBUG)

            table_name = filename.split('.')[0]
            write_log("table_name: " + table_name, logging.DEBUG)
            results, tables = query([table_name], "SELECT count(*) as count FROM " + table_name,
                                    False, spark)
            #        results, tables = query(["cpdiag_userspace_crash_full"], "SELECT * FROM cpdiag_userspace_crash_full WHERE cpdiag_date >= " + '\'' + report_start_date_str + '\'', True, spark)
            if results != None:
                write_log("table_name: " + table_name + ' count: '+ str(results[0]['count']), logging.DEBUG)
                tables[table_name].printSchema()
                print(str(tables[table_name].columns))
                tables_dict[table_name]['count'] = results[0]['count']
                tables_dict[table_name]['columns'] = tables[table_name].columns
            else:
                write_log("table_name: " + table_name + ' count: 0', logging.DEBUG)
                tables_dict[table_name]['count'] = -1
                tables_dict[table_name]['columns'] = []

#        db_tables = spark.tableNames()
#        write_log("db_tables number: " + str(len(db_tables)) + ' ' + str(db_tables), logging.DEBUG)
        write_log("tables number: " + str(len(table_name)), logging.DEBUG)
#        for table_name, count, columns in tables_dict.iteritems():
        for table_name, table in tables_dict.iteritems():
#            print("table_name: " + table_name)
#            print(' count: ' + str(table['count']))
#            print(' columns: ' + str(table['columns']))
            write_log("table_name: " + table_name + ' count: ' + str(table['count']) + ' columns: ' + str(table['columns']), logging.DEBUG)

            #                df = spark.read.parquet(hd_file_in)
#                write_log("finish read from parquet hd_file_in=" + hd_file_in, logging.DEBUG)
#                hd_file_out = output_folder_hd + '/' + filename
            # df.write.mode('append').partitionBy('id').parquet(os.path.join(output_folder_hadoop, table_name))
            #        df.write.mode('overwrite').parquet(hd_file_out)
#                write_log("start save to parquet hd_file_out=" + hd_file_out, logging.DEBUG)
#                df.repartition(2)
#                df.write.mode('overwrite').partitionBy('cpdiag_date').parquet(hd_file_out)
#                df.write.mode('overwrite').parquet(hd_file_out)
#                write_log("finish save to parquet hd_file_out=" + hd_file_out, logging.DEBUG)
#                write_log("Moving done file: " + os.path.join(input_folder_nfs, filename) + 'to: ' + os.path.join(
#                    done_folder_nfs, filename), logging.DEBUG)
#                shutil.move(os.path.join(input_folder_nfs, filename), os.path.join(done_folder_nfs, filename))
    except:
        write_log(str(datetime.datetime.now()) + ' schema_stats failed: working_file=' + working_file, logging.CRITICAL)
    return

def test(options, spark):
    try:
        # Test
        write_log("************* Starting test *************", logging.INFO)
        ###############################################
        '''
        # "cpdiag_userspace_crash_full         cpdiag_kernel_crash"
        table_name="cpdiag_userspace_crash_full"
        write_log("table_name: " + table_name, logging.DEBUG)
        #        results = query_cached_tables(tables, "SELECT * FROM cpdiag_userspace_crash_full WHERE cpdiag_date >= " + '\'' + report_start_date_str + '\'', spark)
        results, tables = query([table_name], "SELECT count(cpdiag_id) FROM " + table_name,
                                True, spark)
        #        results, tables = query(["cpdiag_userspace_crash_full"], "SELECT * FROM cpdiag_userspace_crash_full WHERE cpdiag_date >= " + '\'' + report_start_date_str + '\'', True, spark)
        write_log("enter loop: len(results) " + str(len(results)), logging.DEBUG)
        for idx in range(len(results)):
            write_log("query returned results[" + str(idx) + "]=" + str(results[idx]), logging.DEBUG)
            if idx == 2:
                break
        write_log("finished loop: len(results) " + str(len(results)), logging.DEBUG)
#        return
            #            for key, value in results[idx].iteritems():
            #                today_reports_count = results[0]['today_reports_count']
            #                write_log("query returned today_reports_count=" + str(today_reports_count), logging.DEBUG)
        '''

        ########################################################
        #  Cache tables in Spark and use tables. Result - total_reports_count
        table_name = 'cpdiag_data'
        query_str = "SELECT count(*) as total_reports_count FROM " + table_name
        # Use cached tables - total_reports_count
        results, tables = query([table_name], query_str, True, spark)
        total_reports_count = results[0]['total_reports_count']
        write_log(">> query returned total_reports_count=" + str(total_reports_count), logging.INFO)
        for idx in range(len(results)):
            write_log(">> query returned results[" + str(idx) + "]=" + str(results[idx]), logging.INFO)
            if idx == 2:
                break

        ########################################################
        #  Use cached tables. Result - total_reports_count
        query_str = "SELECT count(*) as total_reports_count FROM " + table_name
        results = query_cached_tables(tables, query_str, spark)
        total_reports_count = results[0]['total_reports_count']
        write_log(">> query returned total_reports_count=" + str(total_reports_count), logging.INFO)

        ########################################################
        # Use cached tables. Result - today_reports_count
        report_start_date = date.today() - timedelta(int(options.report_interval))
        report_start_date_str = report_start_date.strftime('%Y-%m-%d')
        query_str = "SELECT count(*) as today_reports_count FROM cpdiag_data WHERE cpdiag_date >= " + '\'' + report_start_date_str + '\''
        results = query_cached_tables(tables, query_str, spark)
        today_reports_count = results[0]['today_reports_count']
        write_log(">> query returned today_reports_count=" + str(today_reports_count), logging.INFO)

        # multi column\row\tables select examples
        ###############################################
        write_log("************* test finished Ok *************", logging.INFO)


    except:
        write_log(str(datetime.datetime.now()) + 'test failed', logging.CRITICAL)
    return

# map the inputs to the function blocks
modes = {'report' : report,
            'aggregate' : aggregate,
            'test': test,
             'schema_stats': schema_stats,
             'all': all
         }

if __name__ == "__main__":
    """
        Usage: -m run mode
    """
#    loglevel = logging.INFO
    loglevel = logging.DEBUG
    params = []
#    options = parse_args(params)
    options = parse_args()
#    options, unknown = parse_known_args()

    init_logger(loglevel)
#    page_files_num = int(options.page_files_num)
#    sleep_interval = int(options.sleep_interval)
    spark = SparkSession\
        .builder\
        .appName("FilesAggregationJob")\
        .getOrCreate()
		
    print('Python version:', sys.version_info)
    print("Starting at: " + str(datetime.datetime.now()))
    try:
        start_time = timer()
        modes[options.mode](options, spark)
        end_time = timer()
        process_time = end_time - start_time
        write_log(str(datetime.datetime.now()) + 'files_agg_job END: process_time=' + str(process_time), logging.INFO)
    except:
        write_log(str(datetime.datetime.now()) + ' Main loop failed', logging.CRITICAL)
        spark.stop()
        exit(-1)

    write_log(str(datetime.datetime.now()) + ' Job finished Ok', logging.INFO)
    spark.stop()
