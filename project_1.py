## Imports for compare, csv
import os
import difflib

# import pandas as pd
# import pandas as pd1
# import pandas as pd2
# import pandas as pandas_df1
# import glob

## Imports
import sys
import csv
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql.types import *

from pyspark.sql import HiveContext,SparkSession
from pyspark.sql.types import *
from pyspark.sql import Row

from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType

from pyspark.sql.functions import *
from pandas import *
## CONSTANTS
appname="full_load_cust"

##OTHER FUNCTIONS/CLASSES

def extract(table,typefull_load):
        if (typefull_load == "full_load"):
            print(table)
            file_name=table+'.txt'
            print(table)
            inputfilenamesrc = spark.sparkContext.textFile("C:\\Users\\a\\Desktop\\"+file_name)
            print(inputfilenamesrc.collect())
            # inputfilenamesrc=open("/user/hadoop/"+file_name, "r").read()
            print(inputfilenamesrc)
            tgtTblora=inputfilenamesrc.first()
            conf1 = SparkConf()
            conf1.setMaster("local")
            conf1.setAppName("Oracle_imp_exp")
            sqlContext = SQLContext(sc)
            ora_tmp=sqlContext.read.format("jdbc").option("url","jdbc:oracle:thin:@//someshdb.cgicwhxvk9b9.ap-south-1.rds.amazonaws.com:1521/SOMESHDB").option("user","someshdb").option("password","someshdb").option("driver","oracle.jdbc.OracleDriver").option('query',tgtTblora).load()
            ora_tmp.write.csv("C:\\Users\\a\\Desktop\\kapil1\\"+table)

##MAIN FUNCTIONALITY

if __name__ == "__main__":

    spark = SparkSession.builder.master("local").appName("testing").getOrCreate()
    sc = spark.sparkContext

    sql_sc = SQLContext(sc)

    df = spark.read.csv("C:\\Users\\a\\Desktop\\input1.csv")  # if no header
    s_df=df.select(col('_c0').alias('tblnm'), col('_c1').alias('full_loadtype'))
    s_df.show()

    for dfnew in s_df.rdd.collect():
        extract(dfnew.tblnm,dfnew.full_loadtype)

        print("End Oracle To Spark")
