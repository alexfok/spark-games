import glob
import logging
import os
import json
import time
import collections
import re

import shutil
from pyspark.sql import SparkSession


def make_dir(dir_name):
    if not os.path.isdir(dir_name):
        os.mkdir(dir_name)


def init_parser(hadoop_config):
    init_logger(hadoop_config["log_file"])

    # Create folder dictionary
    folder_dictionary = dict()
    folder_dictionary["cpdiag_dir_mapr"] = os.path.join(hadoop_config["mapr_prefix"], hadoop_config["mapr_folder"])
    folder_dictionary["cpdiag_dir_hdfs"] = os.path.join(hadoop_config["hdfs_prefix"], hadoop_config["mapr_folder"])
    folder_dictionary["raw_dir"] = os.path.join(folder_dictionary["cpdiag_dir_mapr"], hadoop_config["raw_dir"])
    folder_dictionary["working_dir_mapr"] = os.path.join(folder_dictionary["cpdiag_dir_mapr"], hadoop_config["working_dir"])
    folder_dictionary["working_dir_hdfs"] = os.path.join(folder_dictionary["cpdiag_dir_hdfs"], hadoop_config["working_dir"])
    folder_dictionary["data_dir_mapr"] = os.path.join(folder_dictionary["cpdiag_dir_mapr"], hadoop_config["data_dir"])
    folder_dictionary["data_dir_hdfs"] = os.path.join(folder_dictionary["cpdiag_dir_hdfs"], hadoop_config["data_dir"])
    folder_dictionary["bad_dir"] = os.path.join(folder_dictionary["cpdiag_dir_mapr"], hadoop_config["bad_dir"])

    # Create all mandatory dirs
    make_dir(folder_dictionary["cpdiag_dir_mapr"])
    make_dir(folder_dictionary["raw_dir"])
    make_dir(folder_dictionary["working_dir_mapr"])
    make_dir(folder_dictionary["data_dir_mapr"])
    make_dir(folder_dictionary["bad_dir"])

    return folder_dictionary


def init_logger(log_path):
    _format = '[%(asctime)s][%(process)d][%(levelname)s][%(filename)s:%(lineno)d] %(message)s'
    _date_format = '%d/%m/%y %H:%M:%S'
    logging.basicConfig(filename=log_path, format=_format, datefmt=_date_format, level=logging.INFO)


def unify_raw_json_files(hadoop_config, file_list):
    logging.info("Start loop_on_dir " + hadoop_config["raw_dir"])
    json_regex = re.compile(r'(.+)_\d+_\d+\.json')
    for json_file in file_list:
        path, filename = os.path.split(json_file)
        m = json_regex.match(filename)
        if m:
            table_name = m.group(1)
            os.system("cat " + json_file + " >> " + os.path.join(hadoop_config["working_dir_mapr"], table_name + '.json'))
            os.remove(json_file)
            logging.info("loop_on_dir: Parse filename + table_name: " + filename + ' ' + table_name)
        else:
            shutil.move(filename, os.path.join(hadoop_config["bad_dir"], filename))
            logging.error("loop_on_dir: Bad file name - skip file: " + filename)


def insert_json_to_hadoop(spark, hadoop_config):
    for json_file in os.listdir(hadoop_config["working_dir_mapr"]):
        try:
            logging.info("Working on file: " + json_file)
            json_full_path = os.path.join(hadoop_config["working_dir_mapr"], json_file)
            hd_file_out = hadoop_config["data_dir_hdfs"] + '/' + json_file[:-5] + '.parquet'
            df = spark.read.json(json_full_path)
            df.coalesce(2)

            try:
                df.write.mode('append').partitionBy('cpdiag_date').parquet(hd_file_out)
                logging.info("Successfully added: " + hd_file_out)
            except:
                logging.exception("Failed inserting to Hadoop with partition, file:" + hd_file_out)
                df.write.mode('append').parquet(hd_file_out)
                logging.info("succeeded write of parquet without partition hd_file_out=" + hd_file_out)

            os.remove(json_full_path)
        except:
            logging.exception("Error in file: " + json_file)
            shutil.move(json_full_path, os.path.join(hadoop_config["bad_dir"], json_file))


def loop_on_dir(spark, hadoop_config, file_list):
    unify_raw_json_files(hadoop_config, file_list)

    insert_json_to_hadoop(spark, hadoop_config)


def chunk_list(l, n):
    return [l[x: x + n] for x in xrange(0, len(l), n)]


def main_loop(spark, hadoop_config):
    logging.info("Removing old data")

    # Remove old files from working directory
    os.system("rm -f " + hadoop_config["working_dir_mapr"] + "/*")
    file_list_all = glob.glob(hadoop_config["raw_dir"] + '/*.json')
    file_pages_list = chunk_list(file_list_all, 3000)

    for file_list in file_pages_list:
        loop_on_dir(spark, hadoop_config, file_list)


if __name__ == "__main__":
    running_folder = os.path.dirname(os.path.realpath(__file__))
    with open(os.path.join(running_folder, "hadoop_parser_config.json")) as f:
        hadoop_config_dict = json.load(f)

    folder_dictionary = init_parser(hadoop_config_dict)

    spark_job = SparkSession.builder.appName("AdamTest").getOrCreate()
    #while True:
    try:
        main_loop(spark_job, folder_dictionary)
        logging.error("Main loop finished, sleeping for 10 seconds")
        time.sleep(10)
    except:
        logging.exception("Error in main loop")
        time.sleep(10)

    spark_job.stop()
