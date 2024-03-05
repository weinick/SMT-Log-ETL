from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructType
from pyspark.sql import Row
input_path = "s3://siplacedemo/result/csv/Year=2021/Month=12/Day=12/*.csv" 
output_path = "s3://siplacedemo/result/parquet/Year=2021/Month=12/Day=12/"
partition_nums = 1

spark = SparkSession.builder.getOrCreate()
sc = spark._sc
hadoop_conf = sc._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
hadoop_conf.set("fs.s3a.endpoint", "s3.cn-north-1.amazonaws.com.cn")

data = spark.read.option("multiLine", "true").csv(input_path,header=True)
#data.show()
data.write.parquet(output_path) 