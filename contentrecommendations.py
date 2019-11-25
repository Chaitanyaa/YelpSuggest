import boto3
import findspark
import json

#findspark.init("spark-2.4.4-bin-hadoop2.7")  # SPARK_HOME
import os

from flask import session
import numpy as np
import pandas as pd
from pyspark.sql.functions import col
from pyspark.ml import Pipeline, PipelineModel

#os.environ["JAVA_HOME"] = "/Library/Java/JavaVirtualMachines/jdk1.8.0_202.jdk/Contents/Home"
#os.environ["SPARK_HOME"] = "/content/spark-2.4.4-bin-hadoop2.7"

findspark.init()
from pyspark.sql import SparkSession

s3client = boto3.client(
    's3',
    aws_access_key_id='AKIAIM3OKQXLAZEGPXIQ',
    aws_secret_access_key='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'
)

obj = s3client.get_object(Bucket= 'yelpsuggest', Key= 'Data/Yelp_Business.csv')
business_df = pd.read_csv(obj['Body'])
business_df['address']=business_df['address'].fillna("")
business_df['postal_code']=business_df['postal_code'].fillna(0)

spark = SparkSession.builder.master("local[*]").getOrCreate()

trf_file = "./Data/Content/trf*"
reviews_by_business_trf_df = spark.read.parquet(trf_file)

import pyarrow.parquet as pq
import s3fs
s3 = s3fs.S3FileSystem(anon=False, key='AKIAIM3OKQXLAZEGPXIQ', secret='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')

def contentRecommed(u_id):
    s3client = boto3.client(
        's3',
        aws_access_key_id='AKIAIM3OKQXLAZEGPXIQ',
        aws_secret_access_key='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'
    )
    obj = s3client.get_object(Bucket='yelpsuggest', Key='Data/Final_Result/content1.csv')
    content_df = pd.read_csv(obj['Body'])
    content_recom_df = content_df[content_df['user_id']==u_id]
    return content_recom_df


def getBusinessDetails(in_bus):
    s3client = boto3.client(
        's3',
        aws_access_key_id='AKIAIM3OKQXLAZEGPXIQ',
        aws_secret_access_key='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'
    )
    obj = s3client.get_object(Bucket='yelpsuggest', Key='Data/Yelp_Business.csv')
    business_df = pd.read_csv(obj['Body'])
    business_df['address'] = business_df['address'].fillna("")
    business_df['postal_code'] = business_df['postal_code'].fillna(0)
    business = spark.createDataFrame(business_df)
    a = in_bus.alias("a")
    b = business.alias("b")

    return a.join(b, col("a.business_id") == col("b.business_id"), 'inner') \
        .select([col('a.' + xx) for xx in a.columns] + [col('b.business_name'), col('b.categories'),
                                                        col('b.stars'), col('b.review_count'),
                                                        col('b.latitude'), col('b.longitude')])

def CosineSim(vec1, vec2):
    return np.dot(vec1, vec2) / np.sqrt(np.dot(vec1, vec1)) / np.sqrt(np.dot(vec2, vec2))

def getKeyWordsRecoms(key_words, sim_bus_limit):
    pipeline_mdl = PipelineModel.load('./Data/pipe_txt')
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

print(contentRecommed('ZsUSGU1-L1ImomLZjXxxTg'))