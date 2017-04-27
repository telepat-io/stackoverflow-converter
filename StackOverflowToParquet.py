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
	for name, value in root.attrib.items():
		d[name] = value
	for key in broadcastKeys.value:
		if (key not in d):
			d[key] = ""
	return d	

if len(sys.argv) < 3:
	print (len(sys.argv))
	print ("Usage: %s dummy_row %s input_file %s output_file")
	exit(1)



dummyRowPath = sys.argv[1]
inputFilePath = sys.argv[2]
outputFilePath = sys.argv[3]
file = open(dummyRowPath, 'r')
initialString = file.read()
if ('row' not in initialString):
	exit(1)


root = xml.fromstring(initialString)
fields = [StructField(field_name, StringType(), True) for field_name, value in root.attrib.items()]
schema = StructType(fields)
broadcastKeysNames = []
for field_name, value in root.attrib.items(): 
	broadcastKeysNames.append(field_name)
broadcastKeys = spark.sparkContext.broadcast(broadcastKeysNames)


lines = spark.read.text(inputFilePath).rdd.map(lambda r: r[0]).filter(lambda s: "row" in s)
toDf = lines.map(lambda x: Row(**f(x)))
df = spark.createDataFrame(toDf, schema)

df.write.format('parquet').mode("overwrite").save(outputFilePath)
