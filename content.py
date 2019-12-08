import boto3
import findspark

#findspark.init("spark-2.4.4-bin-hadoop2.7")  # SPARK_HOME
import os
import io

from flask import session
from folium import folium
import html
import numpy as np
import pandas as pd
from pip._vendor.pyparsing import col
from pyspark.sql.functions import *

from pyspark.sql.types import *

from operator import add
from pyspark.ml import Pipeline, PipelineModel
import pyarrow

os.environ["JAVA_HOME"] = "/Library/Java/JavaVirtualMachines/jdk1.8.0_202.jdk/Contents/Home"
#os.environ["SPARK_HOME"] = "/content/spark-2.4.4-bin-hadoop2.7"

findspark.init()
from pyspark.sql import SparkSession

s3client = boto3.client(
    's3',
    aws_access_key_id='***',
    aws_secret_access_key='***'
)

obj = s3client.get_object(Bucket= 'yelpsuggest', Key= 'Data/Yelp_Business.csv')
business_df = pd.read_csv(obj['Body'])
business_df['address']=business_df['address'].fillna("")
business_df['postal_code']=business_df['postal_code'].fillna(0)

spark = SparkSession.builder.master("local[*]").getOrCreate()

business = spark.createDataFrame(business_df)

trf_file = "flaskapp/Data/Content/trf*"
reviews_by_business_trf_df = spark.read.parquet(trf_file)

import pyarrow.parquet as pq
import s3fs
s3 = s3fs.S3FileSystem(anon=False, key='***', secret='***')

review_df = pq.ParquetDataset('s3://yelpsuggest/Data/Yelp_Reviews.parquet', filesystem=s3).read_pandas().to_pandas()


review_df.createOrReplaceTempView("reviews")
business.createOrReplaceTempView("business")



def contentRecommed(u_id):
    content_recom_df = getContentRecoms(u_id)
    print(content_recom_df.toPandas())
    return content_recom_df.toPandas()

def CosineSim(vec1, vec2):
    return np.dot(vec1, vec2) / np.sqrt(np.dot(vec1, vec1)) / np.sqrt(np.dot(vec2, vec2))

def getSimilarBusinesses(b_ids, sim_bus_limit=10):
    all_business_vecs = reviews_by_business_trf_df.select('business_id', 'word_vec').rdd.map(
        lambda x: (x[0], x[1])).collect()
    schema = StructType([
        StructField("business_id", StringType(), True)
        , StructField("score", IntegerType(), True)
        , StructField("input_business_id", StringType(), True)
    ])

    similar_businesses_df = spark.createDataFrame([], schema)

    for b_id in b_ids:
        input_vec = [(r[1]) for r in all_business_vecs if r[0] == b_id][0]
        # input_vec = reviews_by_business_trf_df.select('word_vec')\
        # .filter(reviews_by_business_trf_df['business_id'] == b_id)\
        # .collect()[0][0]

        similar_business_rdd = spark.sparkContext.parallelize(
            (i[0], float(CosineSim(input_vec, i[1]))) for i in all_business_vecs)

        similar_business_df = spark.createDataFrame(similar_business_rdd) \
            .withColumnRenamed('_1', 'business_id') \
            .withColumnRenamed('_2', 'score') \
            .orderBy("score", ascending=False)
        similar_business_df = similar_business_df.filter(col("business_id") != b_id).limit(sim_bus_limit)
        similar_business_df = similar_business_df.withColumn('input_business_id', lit(b_id))

        similar_businesses_df = similar_businesses_df \
            .union(similar_business_df)

    return similar_businesses_df


def getBusinessDetails(in_bus):
    a = in_bus.alias("a")
    b = business.alias("b")

    return a.join(b, col("a.business_id") == col("b.business_id"), 'inner') \
        .select([col('a.' + xx) for xx in a.columns] + [col('b.business_name'), col('b.categories'),
                                                        col('b.stars'), col('b.review_count'),
                                                        col('b.latitude'), col('b.longitude')])


def getContentRecoms(u_id, sim_bus_limit=5):
    # select restaurants previously reviewed (3+) by the user
    query = """
    SELECT distinct r.business_id,b.business_name FROM reviews r join business b on r.business_id = b.business_id  
    where r.stars >= 3.0 
    and r.user_id = "{}"
    """.format(u_id)

    usr_rev_bus = spark.sql(query)
    #session['previousbusiness'] = usr_rev_bus.select('business_name').collect()
    # from these get sample of 5 restaurants
    usr_rev_bus = usr_rev_bus.limit(5)
    print(usr_rev_bus.select('business_name').collect())


    usr_rev_bus_det = getBusinessDetails(usr_rev_bus)

    # show the sample details
    #print('\nBusinesses previously reviewed by user:', u_id)
    #usr_rev_bus_det.select(['business_id', 'business_name', 'categories']).show(truncate=False)

    bus_list = [i.business_id for i in usr_rev_bus.collect()]

    # get restaurants similar to the sample
    sim_bus_df = getSimilarBusinesses(bus_list, sim_bus_limit)

    # filter out those have been reviewd before by the user
    s = sim_bus_df.alias("s")
    r = usr_rev_bus.alias("r")
    j = s.join(r, col("s.business_id") == col("r.business_id"), 'left_outer') \
        .where(col("r.business_id").isNull()) \
        .select([col('s.business_id'), col('s.score')])

    a = j.orderBy("score", ascending=False).limit(sim_bus_limit)

    return getBusinessDetails(a)


def getKeyWordsRecoms(key_words, sim_bus_limit):
    pipeline_mdl = PipelineModel.load('/flaskapp/Data/pipe_txt')
    print('\nBusinesses similar to key words: "' + key_words + '"')
    all_business_vecs = reviews_by_business_trf_df.select('business_id', 'word_vec').rdd.map(
        lambda x: (x[0], x[1])).collect()

    input_words_df = spark.sparkContext.parallelize([(0, key_words)]).toDF(['business_id', 'text'])

    # transform the the key words to vectors
    input_words_df = pipeline_mdl.transform(input_words_df)

    # choose word2vec vectors
    input_key_words_vec = input_words_df.select('word_vec').collect()[0][0]

    # get similarity
    sim_bus_byword_rdd = spark.sparkContext.parallelize(
        (i[0], float(CosineSim(input_key_words_vec, i[1]))) for i in all_business_vecs)

    sim_bus_byword_df = spark.createDataFrame(sim_bus_byword_rdd) \
        .withColumnRenamed('_1', 'business_id') \
        .withColumnRenamed('_2', 'score') \
        .orderBy("score", ascending=False)

    # return top 10 similar businesses
    a = sim_bus_byword_df.limit(sim_bus_limit)
    return getBusinessDetails(a)

contentRecommed('8NwU4TRsD3S6gIfBqFzDMQ')