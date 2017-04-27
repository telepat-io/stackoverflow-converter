from __future__ import print_function
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark import SparkContext, SQLContext
from dateutil.parser import parser
from pyspark.sql.functions import *
from pyspark.sql.types import *
import xml.etree.ElementTree as xml
from pyspark.sql import Row
import sys

spark = SparkSession \
        .builder \
        .appName("Convert to parquet") \
        .getOrCreate()
sql = SQLContext(spark)


def f(x):
	d = {}
	root = xml.fromstring(x.encode('utf-8'))
	#root = tree.getroot()
	for name, value in root.attrib.items():
		d[name] = value
	for key in broadcast_keys.value:
		if (key not in d):
			d[key] = ""
	return d	

if len(sys.argv) < 3:
	print (len(sys.argv))
	print ("Usage: %s dummy_row %s input_file %s output_file")
	exit(1)



dummy_row_path = sys.argv[1]
input_file_path = sys.argv[2]
output_file_path = sys.argv[3]
file = open(dummy_row_path, 'r')
initial_string = file.read()
if ('row' not in initial_string):
	exit(1)


root = xml.fromstring(initial_string)
fields = [StructField(field_name, StringType(), True) for field_name, value in root.attrib.items()]
schema = StructType(fields)
broadcast_keys_names = []
for field_name, value in root.attrib.items(): 
	broadcast_keys_names.append(field_name)
broadcast_keys = spark.sparkContext.broadcast(broadcast_keys_names)


lines = spark.read.text(input_file_path).rdd.map(lambda r: r[0]).filter(lambda s: "row" in s)
to_df = lines.map(lambda x: Row(**f(x)))
df = spark.createDataFrame(to_df, schema)

df.write.format('parquet').mode("overwrite").save(output_file_path)
