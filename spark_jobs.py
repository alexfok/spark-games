## Spark Code gists

#########################
## Loop on directory of json files and convert them to corresponding parquet files
#####################################################
# Data Frames Usage Examples
from pyspark.sql import SQLContext
from pyspark.sql.types import Row, StructField, StructType, StringType, IntegerType
from pyspark.sql.functions import explode
import os
import re
import collections


input_folder_hadoop = "hdfs://172.23.92.97/user/mapr/cp_in/json"
output_folder_hadoop = "hdfs://172.23.92.97/user/mapr/cp_out/"


def my_print(a):
    print (a)


def add_json_to_hadoop(json_dict, filename, table_name):
    print("Start parquet queries " + table_name)
    with open(os.path.join(input_folder_hadoop, filename), 'r') as f:
        json_dict[table_name] += f.read() + "\n"
    #df = sqlContext.read.json(os.path.join(input_folder_hadoop, filename))
    #df.write.mode('append').partitionBy('id').parquet(os.path.join(output_folder_hadoop, table_name))
    #df.write.mode('append').parquet(os.path.join(output_folder_hadoop, table_name))


def loop_on_dir(sql_context, dir_path):
    json_dict = collections.defaultdict(str)
    json_regex = re.compile(r'(\D+)_\d+_\d+\.json')
    for filename in os.listdir(dir_path):
        m = json_regex.match(filename)
        if m:
            print (m.group(1))
            add_json_to_hadoop(json_dict, os.path.join(dir_path, filename), m.group(1))

    for key, value in json_dict.items():
        print ("working on " + key)
        with open(os.path.join(dir_path, key), 'w+') as f:
            f.write(value)

        df = sqlContext.read.json(os.path.join(dir_path, key))
        # df.write.mode('append').partitionBy('id').parquet(os.path.join(output_folder_hadoop, table_name))
        df.write.mode('append').parquet(os.path.join(output_folder_hadoop, key))

# Define schema
schemaString = "attributes keyname tag text"
fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
schema = StructType(fields)

sqlContext = SQLContext(sc)

#loop_on_dir(sc, "/user/mapr/cp_in/json")
#########################

################################

"""
 Counts words in new text files created in the given directory
 Usage: hdfs_wordcount.py <directory>
   <directory> is the directory that Spark Streaming will use to find and read new text files.
 To run this on your local machine on directory `localdir`, run this example
    $ bin/spark-submit examples/src/main/python/streaming/hdfs_wordcount.py localdir
    $ unset PYSPARK_DRIVER_PYTHON; spark-submit examples/src/main/python/streaming/hdfs_wordcount.py localdir
    $ [mapr@il-mapr-stg0 ~]$ unset PYSPARK_DRIVER_PYTHON; spark-submit --master yarn --deploy-mode client --num-executors 2 --executor-memory 512m --executor-cores 2 /opt/mapr/spark/spark-2.0.1/examples/src/main/python/streaming/hdfs_wordcount.py localdir
 Then create a text file in `localdir` and the words in the file will get counted.
"""
from __future__ import print_function

import sys
import os
import re
import collections
import json
import shutil
import glob

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext
from pyspark.sql.types import Row, StructField, StructType, StringType, IntegerType

#input_folder_hadoop = "hdfs://172.23.92.97/user/mapr/cp_in/json"
output_folder_hadoop = "hdfs://172.23.92.97/user/mapr/cp_out/"
input_folder = "hdfs://cpdiag-stg.checkpoint.com/user/mapr/cp_in/current"
done_folder = "hdfs://cpdiag-stg.checkpoint.com/user/mapr/cp_in/done"

#hadoop fs -ls hdfs://cpdiag-stg.checkpoint.com/user/mapr/cp_in
#/mnt/cpdiag-stg.checkpoint.com/user/mapr/cp_in/current/json/cpdiag_flat_info_data_3509456_0.json
#/mnt/cpdiag-stg.checkpoint.com/user/mapr/cp_in/current/json/cpdiag_flat_info_data_3509459_0.json
#/mnt/cpdiag-stg.checkpoint.com/user/mapr/cp_in/current/json/cpdiag_flat_info_data_3901291_0.json
#/mnt/cpdiag-stg.checkpoint.com/user/mapr/cp_in/current/json/cpdiag_flat_info_data_3901371_0.json

def my_print(a):
    print (a)


def add_json_to_hadoop(json_dict, filename, table_name):
    print("Start parquet queries " + table_name)
    with open(os.path.join(input_folder, filename), 'r') as f:
        json_dict[table_name] += f.read() + "\n"
    #df = sqlContext.read.json(os.path.join(input_folder_hadoop, filename))
    #df.write.mode('append').partitionBy('id').parquet(os.path.join(output_folder_hadoop, table_name))
    #df.write.mode('append').parquet(os.path.join(output_folder_hadoop, table_name))
def loop_on_dir(dir_path):
    json_dict = collections.defaultdict(str)
    json_regex = re.compile(r'(\D+)_\d+_\d+\.json')
    for filename in os.listdir(dir_path):
        m = json_regex.match(filename)
        if m:
            print (m.group(1))
            add_json_to_hadoop(json_dict, os.path.join(dir_path, filename), m.group(1))

    for key, value in json_dict.items():
        print ("working on " + key)
        with open(os.path.join(dir_path, key), 'w+') as f:
            f.write(value)

        df = sqlContext.read.json(os.path.join(dir_path, key))
        # df.write.mode('append').partitionBy('id').parquet(os.path.join(output_folder_hadoop, table_name))
        df.write.mode('append').parquet(os.path.join(output_folder_hadoop, key))

def loop_on_dir_and_print_table_name(line):
    try:
        data_json = json.loads(line)
        table_name = data_json['cpdiag_table']
        return table_name
#     json_dict[table_name]++
#    json_dict[table_name] += line + "\n"
    except:
        print('\n*********\nBad JSON: ' + line + '\n*********\n')
    return line

if __name__ == "__main__":
#    if len(sys.argv) != 2:
#        print("Usage: hdfs_wordcount.py <directory>", file=sys.stderr)
#        exit(-1)
    spark = SparkSession\
        .builder\
        .appName("FilesStreamingJob")\
        .getOrCreate()

    sc = spark.sparkContext
    ssc = StreamingContext(sc, 10)

def filter_table(filter_table_name, line):
    try:
        data_json = json.loads(line)
        table_name = data_json['cpdiag_table']
        return filter_table_name == table_name
    except:
        return False
    return False

def convert_lines_to_kv(line):
    try:
        data_json = json.loads(line)
        table_name = data_json['cpdiag_table']
        return (table_name, line)
    except:
        return ("no_table_name_error", line)
    return line


#    lines = ssc.textFileStream(sys.argv[1])
#        counts = lines.flatMap(lambda line: line.split(" "))\
#                      .map(lambda x: (x, 1))\
#                      .reduceByKey(lambda a, b: a+b)
#    counts = lines.map(lambda x: (x, 1))\
#                      .reduceByKey(lambda a, b: a+b)
    lines = ssc.textFileStream(input_folder)
    # Run on all lines, extract cpdiag_table name, count number of lines per cpdiag_table
    tables = lines.map(loop_on_dir_and_print_table_name)
    for table in tables.collect():
        print('Found table: ' + table)
        table_lines = lines.filter(lambda x: filter_table(table, x)
        table_lines.pprint()
#    tables.pprint()
#    lines_kv = lines.map(convert_lines_to_kv)
    
    
#    counts = lines.map(loop_on_dir_and_print_table_name).map(lambda x: (x, 1)).reduceByKey(lambda a, b: a+b)
#    counts.pprint()
    
    for file in glob.glob(input_folder + '/*.json'):
        print("Moving file: " + file + 'to: ' + done_folder)
        shutil.move(file, done_folder)
    
    ssc.start()
    ssc.awaitTermination()
    ################################


#########################
## Count number of different CPDiag configurations
output_folder_hadoop = "hdfs://172.23.92.97/user/mapr/cp_out/"
table_name = "cpdiag_data"
table_name_2 = "cpdiag_flat_info_data"
sqlContext = SQLContext(sc)
df_new = sqlContext.read.parquet(output_folder_hadoop + table_name)
df_new.registerTempTable(table_name)

df_new_2 = sqlContext.read.parquet(output_folder_hadoop + table_name_2)
df_new_2.registerTempTable(table_name_2)

bb = sqlContext.sql("select b.configuration, count(distinct a.mac)  from " + table_name + " as a inner join " + table_name_2 + " as b on a.cpdiag_id=b.cpdiag_id group by b.configuration")
#bb = sqlContext.sql("select * from cpdiag_flat_info_data")
bb.show()
#########################
