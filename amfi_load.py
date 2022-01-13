from pyspark.sql import functions as F
from pyspark import SparkFiles
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
import requests
import os
import datetime

spark=SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel('WARN')

def download_file(url):
        local_filename = url.split('/')[-1]
        with requests.get(url, stream=True) as r:
                r.raise_for_status()
                with open(local_filename, 'wb') as f:
                        for chunk in r.iter_content(chunk_size=8192):
                                f.write(chunk)
        return local_filename

try:
        tmp_df=spark.read.parquet("/tmp/output/mf2.parquet")
        max_dt=tmp_df.select(F.max(tmp_df.Date)).collect()[0][0]
        print("Incremental fetch will take place")
except:
        max_dt="01-Jan-1980"
        print("Full fetch will take place")

print("Current max_date is:{0}".format(max_dt))

begin=datetime.datetime.now()

url = "https://www.amfiindia.com/spages/NAVAll.txt?t=11012022102943"
fl_path=download_file(url)
df1 = spark.read.csv(fl_path, sep=";", header=True, inferSchema= True)
df1=df1.withColumn("Date",F.to_date(df1.Date,"dd-MMM-yyyy"))

df2=df1.where((df1.Date.isNotNull())&(df1.Date>F.to_date(F.lit(max_dt),"dd-MMM-yyyy")))
new_col= list(map(lambda x: x.replace(" ", "_"), df2.columns))
df3 = df2.toDF(*new_col)

df3=df3.withColumn("year",F.year(df3.Date)).withColumn("month",F.month(df3.Date))
df3.persist()

df3.write.partitionBy("year","month","Scheme_Name").mode("append").parquet("/tmp/output/mf2.parquet")
end=datetime.datetime.now()

print("Data fetch time(H:MM:SS): {0}".format(end-begin))
print("Count of new records fetched:{0}".format(df3.count()))
df3.unpersist()
print("Data fetch complete")
os.remove(fl_path)
