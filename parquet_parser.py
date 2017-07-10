#####################################################
# Data Frames Usage Examples
#from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
import json
import glob

from pyspark.sql.types import Row, StructField, StructType, StringType, IntegerType
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

#sqlContext = SQLContext(sc)

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

# Displays the content of the DataFrame to stdout
#df.show()
#types = [f.dataType for f in df.schema.fields]
#print(df.schema)
#types = [(f.name, f.dataType) for f in df.schema.fields]
#print('\n' + str(types) +'\n')

'''
print('\n********** Try to fix schema **********\n')
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
def string_to_float(x):
     return str(x)
#     return float(x)
udfstring_to_float = udf(string_to_float, StringType())
#new_df=df.withColumn("max_inline_rec_level",udfstring_to_float("max_inline_rec_level") )

#for field_to_fix in fields_to_fix:
#    new_df=df.withColumn(field_to_fix['key_to_add'], str(df.max_inline_rec_level))
#    new_df=df.max_inline_rec_level.cast("string")
    
new_df=df
print(new_df.schema)
new_df.collect()

# Print the schema in a tree format
#new_df.printSchema()
#new_df.coalesce(10)
#df.printSchema()
#df.coalesce(10)
write_file_flag=True

if write_file_flag:
#    print('succeeded aggregation of parquet hd_file_out=' + hd_file_in)
    print('Query hd_file_out=' + hd_file_in)
    # 1 Infer the schema, and register the DataFrame as a table.
    # Manipulate data as a temporary table
    new_df.registerTempTable("cpDiagTempTable")
    rdd1 = spark.sql("select max_inline_rec_level from cpDiagTempTable")
    # The results of SQL queries are RDDs and support all the normal RDD operations.
    print('max_inline_rec_level values')
    for i in rdd1.collect():
        print(i[0])

print('\n***End of Test***\n')
try:
    df.write.mode('overwrite').partitionBy('cpdiag_date').parquet(hd_file_out)
    print("succeeded aggregation of parquet with partition hd_file_out=" + hd_file_out)
except:
    try:        
        print("failed to write partitioned file: " + os.path.join(input_folder_nfs, filename) + ' trying to write without partition')
        df.write.mode('overwrite').parquet(hd_file_out)
        print("succeeded aggregation of parquet without partition hd_file_out=" + hd_file_out)
    except:
        print('failed aggregation of parquet hd_file_out=' + hd_file_in + ' reason=parquet write failed')
        write_file_flag=False
'''




#df.toJSON().first()

#df.write.mode('overwrite').parquet("file:///home/mapr/stats.parquet")
# Read parquet from HDFS
#df = sqlContext.read.format('parquet').load("/home/mapr/stats.parquet")
#df_new = sqlContext.read.parquet("file:///home/mapr/stats.parquet")
#df_new = sqlContext.read.parquet("file:///home/mapr/stats_new.parquet")
#df.write.mode('append').parquet("/home/mapr/stats.parquet")
#df_new.printSchema()
#df_new.show()
#df_new.write.mode('overwrite').partitionBy('date', 'field3').parquet("file:///home/mapr/stats_new.parquet")
#df_new.write.mode('append').partitionBy('date', 'field3').parquet("file:///home/mapr/stats_new.parquet")

# Select cpu1.cPU_IDL_TIME column values
#df.select("cpu1.cPU_IDL_TIME").show()
#df.select(df['cpu1.cPU_IDL_TIME']).show()
#df.select(df['cpu1.cPU_IDL_TIME'] + 1).show()
# Select rows with cpu1.cPU_IDL_TIME > 30
#df.filter(df['cpu1.cPU_IDL_TIME'] > 30).show()

## Write data to HDFS
#df.write.save("/home/mapr/stats.parquet", format="parquet")


#####################################################
# Option 2 - define predifined JSON schema
# parquet option
    # HDFS directory structure?
    # parquet partitioning
# example - RDD to DF
# streaming on directory
# local directory creation
# save DF to file
    # df.write.format('json').save(os.path.join(tempfile.mkdtemp(), 'data'))
    # append mode
    # df.write.mode('append').parquet(os.path.join(tempfile.mkdtemp(), 'data'))
    # partitioned file:
    # df.write.partitionBy('year', 'month').parquet(os.path.join(tempfile.mkdtemp(), 'data'))
# Data analysis examples?
    # read parquet files
    # df = sqlContext.read.format('parquet').load('/home/mapr/stats.parquet')
    # manipulate parquet files
    # append new files to the existing one
    # df = sqlContext.read.parquet('python/test_support/sql/parquet_partitioned')
    # df.registerTempTable('tmpTable')
    # sqlContext.read.table('tmpTable').dtypes

# df.toJSON().first()
# df.toPandas()
