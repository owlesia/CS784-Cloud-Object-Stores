
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
#from delta import *
import shutil

# Clear any previous runs
shutil.rmtree("/tmp/delta-table", ignore_errors=True)

# Enable SQL commands and Update/Delete/Merge for the current spark session.
# we need to set the following configs
spark = SparkSession.builder \
    .appName("quickstart") \
    .master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars", "/home/sunil.mallireddy/gcs-connector-1.9.4-hadoop3.jar,/home/sunil.mallireddy/delta-contribs_2.12-1.1.0.jar") \
    .getOrCreate()

spark._jsc.hadoopConfiguration().set('fs.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem')
#spark.hadoop.fs.gs.auth.service.account.email <client_email>
#spark.hadoop.fs.gs.project.id <project_id>
#spark.hadoop.fs.gs.auth.service.account.private.key <private_key>
#spark.hadoop.fs.gs.auth.service.account.private.key.id <private_key_id>
    #.config("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    #.config("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
    #.getOrCreate()

# Create a table
print("############# Creating a table ###############")
data = spark.range(0, 5)
data.write.mode("append").format("delta").save('gs://784databucket/test2')

