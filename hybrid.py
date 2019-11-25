
import findspark
import os

import pandas as pd
from flask import session

from pyspark.sql.types import *
os.environ["JAVA_HOME"] = "/Library/Java/JavaVirtualMachines/jdk1.8.0_202.jdk/Contents/Home"

findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


#from content import contentRecommed
#from collabrative import getCollabRecom
spark = SparkSession.builder.master("local[*]").getOrCreate()

import pyarrow.parquet as pq
import s3fs
s3 = s3fs.S3FileSystem(anon=False, key='AKIAIM3OKQXLAZEGPXIQ', secret='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')

def gethybridRecom(u_id):
    collabrative_df = pq.ParquetDataset('s3://yelpsuggest/Data/Final_Result/collabrative.parquet', filesystem=s3).read_pandas().to_pandas()
    content_df = pq.ParquetDataset('s3://yelpsuggest/Data/Final_Result/content.parquet',filesystem=s3).read_pandas().to_pandas()
    friends_df = pq.ParquetDataset('s3://yelpsuggest/Data/Final_Result/friends.parquet', filesystem=s3).read_pandas().to_pandas()
    result_df = pd.concat([collabrative_df,content_df,friends_df])
    #result_df.sort_values(by='rating', ascending=False)
    return result_df[result_df['user_id']==u_id]

#print(gethybridRecom('_VMGbmIeK71rQGwOBWt_Kg').count())



