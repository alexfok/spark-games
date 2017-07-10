#####################################################
# Data Frames Schema Discovery
#from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
import json
import glob

from pyspark.sql.types import Row, StructField, StructType, StringType, IntegerType, DateType, LongType, TimestampType
#from pyspark.sql import HiveContext
#from pyspark.sql.types import *

# DataFrame SQL Access
# Compatible with Spark 1.6.1 and Python 3.5
# Initialize sqlContext
#sqContext = HiveContext(sc)
spark = SparkSession\
        .builder\
        .appName("parquet_parser")\
        .getOrCreate()

# Create the DataFrame from local files
#df = sqlContext.read.json("file:///home/mapr/test*.json")
#df = sqlContext.read.json("file:///home/mapr/my_test.json")
# Or load JSON files from HDFS
# df = sqlContext.read.json("hdfs://home/mapr/2016/07/12/test*.json/") 
#hd_file_in="hdfs://il-cpdiag-hwstg.checkpoint.com/user/mapr/cp_out/current/hw_sensor_event.parquet/cpdiag_date\=2017-06-14"
#hd_file_out="hdfs://il-cpdiag-hwstg.checkpoint.com/user/mapr/cp_out/agg/hw_sensor_event.parquet"

hd_file_in="hdfs://il-cpdiag-hwstg.checkpoint.com/user/mapr/cp_out/current/license_data.parquet"
hd_file_out="hdfs://il-cpdiag-hwstg.checkpoint.com/user/mapr/cp_out/agg/license_data.parquet"
hd_file_in="hdfs://il-cpdiag-hwstg.checkpoint.com/user/mapr/cp_out/current/compliance_best_practice.parquet"
hd_file_out="hdfs://il-cpdiag-hwstg.checkpoint.com/user/mapr/cp_out/agg/compliance_best_practice.parquet"
hd_file_in="hdfs://il-cpdiag-hwstg.checkpoint.com/user/mapr/cp_out/bad/ngfw_database_stats.parquet/cpdiag_date\=2017-06-22/part-00000-fcba8a79-ab91-4533-897f-ba29c034e49b.snappy.parquet"
hd_file_out="hdfs://il-cpdiag-hwstg.checkpoint.com/user/mapr/cp_out/ngfw_database_stats.parquet/cpdiag_date\=2017-06-22/part-00000-fcba8a79-ab91-4533-897f-ba29c034e49b.snappy.parquet"
# Bad file
hd_file_in="hdfs://il-cpdiag-hwstg.checkpoint.com/user/mapr/cp_out/bad/ngfw_database_stats.parquet/cpdiag_date\=2017-06-13/part-00000-003164a3-577c-4583-89c6-e5d13eaddd03.snappy.parquet"

#hd_file_in="hdfs://il-cpdiag-hwstg.checkpoint.com/user/mapr/cp_out/bad/ngfw_database_stats.parquet/cpdiag_date\=2017-06-13"
hd_file_out="hdfs://il-cpdiag-hwstg.checkpoint.com/user/mapr/cp_out/ngfw_database_stats.parquet/cpdiag_date=2017-06-13"
#hd_file_out="hdfs://il-cpdiag-hwstg.checkpoint.com/user/mapr/cp_out/ngfw_database_stats.parquet/cpdiag_date=2017-06-13/part-00000-003164a3-577c-4583-89c6-e5d13eaddd03.snappy.parquet"

#hd_file_in="hdfs://il-cpdiag-hwstg.checkpoint.com/user/mapr/cp_out/bad/ngfw_database_stats.parquet/cpdiag_date\=2017-06-13/part-00000-b3966767-980d-4d32-bcb2-6be6fe11f7ba.snappy.parquet"
#hd_file_out="hdfs://il-cpdiag-hwstg.checkpoint.com/user/mapr/cp_out/ngfw_database_stats.parquet/cpdiag_date=2017-06-13/part-00000-c67975dd-04b9-442e-beb5-3a3df96c19d8.snappy.parquet"
#hd_file_out="hdfs://il-cpdiag-hwstg.checkpoint.com/user/mapr/cp_out/ngfw_database_stats.parquet/cpdiag_date=2017-06-13/part-00000-c973b08b-5ab2-498f-b45f-b1c2bc3795d8.snappy.parquet"
#hd_file_out="hdfs://il-cpdiag-hwstg.checkpoint.com/user/mapr/cp_out/ngfw_database_stats.parquet/cpdiag_date=2017-06-13/part-00000-cad20659-09a9-4b7e-bdce-1b6e68c4af83.snappy.parquet"


input_folder_nfs="/mapr/il-cpdiag-hwstg.checkpoint.com/user/mapr/cp_out/current"
input_folder_hd="hdfs://il-cpdiag-hwstg.checkpoint.com/user/mapr/cp_out/current"
table_schema_dir="/mapr/il-cpdiag-hwstg.checkpoint.com/user/mapr/cp_out/schema"
# Test Folders
input_folder_nfs="/mapr/il-cpdiag-hwstg.checkpoint.com/user/mapr/cp_out/bad"
input_folder_hd="hdfs://il-cpdiag-hwstg.checkpoint.com/user/mapr/cp_out/bad"
table_schema_dir="/mapr/il-cpdiag-hwstg.checkpoint.com/user/mapr/cp_out/bad/schema"
# Compare two schemas
# if they are different - report it and fix the relevant column
def init_table_schema(table_name):
    table_schema = {}
    file_name=table_schema_dir + '/' + table_name + '.schema.json'
    try:
        with open(file_name, 'a+') as data_file:
            table_schema = json.load(data_file)
    except:
        print("\nNo report schema file, create one: " + file_name)
    return table_schema

def merge_table_schema(table_schema_disc, table_name):
    #######################
    #  Merge discovered report schema
    fields_to_fix=[]
    table_schema = table_schema_disc
    file_name=table_schema_dir + '/' + table_name + '.schema.json'
    print("\nmerge_table_schema table_schema_disc: " + str(table_schema_disc))
    try:
        table_schema = init_table_schema(table_name)
        # overwrite file_name
        with open(file_name, "w") as data_file:
#            print('2')
            # Merge discovered table schema
            for key_to_add, type_to_add in table_schema_disc.iteritems():
#                print('key: ' + str(key_to_add) + ' type: ' + str(type_to_add))
                if key_to_add not in table_schema:
#                    print('3')
                    print('key not in schema: ' + str(key_to_add) + ' type: ' + str(type_to_add))
                    table_schema[key_to_add]=type_to_add
                elif table_schema[key_to_add] != type_to_add:
#                    print('4')
                    print('Schema validation error - table_name: ' + table_name + ' ' + key_to_add + ' old type: ' + table_schema[key_to_add] + ' new type: ' + str(type_to_add))
                    fields_to_fix.append({'table_name':table_name, 'key_to_add':key_to_add, 'old_type':table_schema[key_to_add], 'new_type':str(type_to_add)})
            # Save the report_schema to the file
            print("merged table_schema: " + str(table_schema))
#            data_file.write(str(table_schema)+ "\n")
#            data_file.write(json.dumps(table_schema) + "\n")
            json.dump(table_schema, data_file, separators=(',', ': '))
    except:
        print("\nError in table schema file merge: " + file_name)
    return table_schema, fields_to_fix

print('\n********** Schema discovery and validation **********\n')
# Schema discovery and validation
'''
file_list = glob.glob(input_folder_nfs + '/*')
print(file_list)
for nfs_file in file_list:
    filepath, filename = os.path.split(nfs_file)
    hd_file_in = input_folder_hd + '/' + filename
    df = spark.read.parquet(hd_file_in)
    # What to do with columns?
    # df.columns
    # Save the table_schema to file
    types_dict = {}
    for f in df.schema.fields:
        types_dict[f.name] = str(f.dataType)
    #print(types_dict)
#    table_name = 'ngfw_database_stats'
    table_name = filename
    init_table_schema(table_name)
    table_schema, fields_to_fix = merge_table_schema(types_dict, table_name)
    print(str(fields_to_fix))

def get_type_name(s: str) -> str:
    """
    >>> get_type_name("int")
    'integer'
    """
    _map = {
        'int': IntegerType().typeName(),
        'timestamp': TimestampType().typeName(),
        # ...
    } 
    return _map.get(s, StringType().typeName())
'''
def get_type_from_name(s):
    """
    >>> get_type_name("int")
    'integer'
    """
    _map = {
        'int': IntegerType(),
        'timestamp': TimestampType(),
        'DateType': DateType(),
        'LongType': LongType(),
        # ...
    } 
#    print('get_type_from_name: ' + s)
    return _map.get(s, StringType())

#"/mapr/il-cpdiag-hwstg.checkpoint.com/user/mapr/cp_out/schema/cpdiag_data.parquet.schema.json"
#"{"ck": "StringType","userspace_crashes": "LongType","report_source": "StringType","cpdiag_sub_id": "LongType","cpdiag_id": "StringType","ip": "StringType","fw_version": "StringType","cpdiag_date": "DateType","cpdiag_table": "StringType","mac": "StringType","report_version": "StringType","timestamp": "LongType","timezone": "StringType","kernel_crashes": "LongType","cpview_history": "LongType","report_size_mb": "LongType"}"
#schemaString = "pod_site file_type file_name @timestamp emulation_type pod_name file_size image_revision client_key confidence file_url_source severity result_from environment score email file_sha256 pod_result_time file_md5 image_id client_id pod_query_time region_name ip continent_code country_code3 country_code2 city_name timezone country_name postal_code real_region_name file_sha1 source_ip client_type verdict"
#fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
#schema = StructType(fields)
hd_file_in = "hdfs://il-cpdiag-hwstg.checkpoint.com/user/mapr/cp_out/current/cpdiag_data.parquet"
table_schema_dir="/mapr/il-cpdiag-hwstg.checkpoint.com/user/mapr/cp_out/schema"
file_name = "cpdiag_data.parquet"
try:
    table_schema = init_table_schema(file_name)
    print(table_schema)
    for filed_name, filed_type in table_schema.iteritems():
        print(filed_name +':'+ filed_type)
        print(str(get_type_from_name(filed_type)))
    print('1')
    fields = [StructField(filed_name, get_type_from_name(filed_type), True) for filed_name, filed_type in table_schema.iteritems()]
    print('2')
    print('fields: ' + str(fields))
    schema = StructType(fields)
#    df = spark.read.parquet(hd_file_in, schema)

    df1 = spark.read.parquet(hd_file_in)
    print(df1.dtypes)
    for f in df1.schema.fields:
        types_dict[f.name] = str(f.dataType)
    print(types_dict)
#    table_name = 'ngfw_database_stats'
    table_name = file_name
    init_table_schema(table_name)
    table_schema, fields_to_fix = merge_table_schema(types_dict, table_name)
    print(str(fields_to_fix))

    print('3')
except:
        print("\nError in table schema validation: " + file_name)
    
