import boto3 as boto3
import findspark
import os

import pandas as pd
from flask import session

from pyspark.sql.types import *
os.environ["JAVA_HOME"] = "/Library/Java/JavaVirtualMachines/jdk1.8.0_202.jdk/Contents/Home"

findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
spark = SparkSession.builder.master("local[*]").getOrCreate()



colab_file = "flaskapp/Data/Collobrative/all*"
all_userRecoms = spark.read.parquet(colab_file)

all_userRecoms.cache()
all_userRecoms.show(1, truncate=False)

new_business_file = "flaskapp/Data/new*"
business_new_df = spark.read.parquet(new_business_file)


def getCollabRecom(u_id):
    userFlatRec = spark.createDataFrame(all_userRecoms.filter(col("user_id") == u_id).rdd.flatMap(lambda p: p[1]))
    a = userFlatRec.alias("a")
    b = business_new_df.alias("b")

    return a.join(b, col("a.businessId") == col("b.businessId"), 'inner') \
        .select([col('b.business_id'), col('a.rating'), col('b.business_name'), col('b.categories'),
                 col('b.stars'), col('b.review_count'),
                 col('b.latitude'), col('b.longitude')]) \
        .orderBy("rating", ascending=False)

