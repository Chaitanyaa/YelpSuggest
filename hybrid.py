import boto3
import findspark
import os

import pandas as pd
from flask import session

from pyspark.sql.types import *
os.environ["JAVA_HOME"] = "/Library/Java/JavaVirtualMachines/jdk1.8.0_202.jdk/Contents/Home"

findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

s3client = boto3.client(
    's3',
    aws_access_key_id='***',
    aws_secret_access_key='***'
)

obj = s3client.get_object(Bucket= 'yelpsuggest', Key= 'Data/Yelp_Business.csv')
business_df = pd.read_csv(obj['Body'])
def getBusinessDetails(in_bus):
    a = in_bus.alias("a")
    b = business_df.alias("b")

    return a.join(b, col("a.business_id") == col("b.business_id"), 'inner') \
        .select([col('a.' + xx) for xx in a.columns] + [col('b.business_name'), col('b.categories'),
                                                        col('b.stars'), col('b.review_count'),
                                                        col('b.latitude'), col('b.longitude')])

def getFriendRecoms(u_id, sim_bus_limit=10):
    query = """
    select business_id, user_id,count(*) as 4_5_stars_count
    from reviews
    where user_id in
        (select f.friend_id from friends f
        inner join users u on f.friend_id = u.user_id
        where f.user_id = "{}") 
    and stars >= 4 
    and business_id not in (select business_id from reviews where user_id = "{}")
    group by business_id,user_id
    order by count(*) desc limit 100
    """.format(u_id, u_id)

    friend_recoms_df = spark.sql(query).limit(sim_bus_limit)

    return getBusinessDetails(friend_recoms_df)
from content import contentRecommed
from collabrative import getCollabRecom
spark = SparkSession.builder.master("local[*]").getOrCreate()

import pyarrow.parquet as pq
import s3fs
s3 = s3fs.S3FileSystem(anon=False, key='AKIAIM3OKQXLAZEGPXIQ', secret='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')

def gethybridRecom(u_id):
    collabrative_df = getCollabRecom(session.get('user_id'))
    content_df = contentRecommed(session.get('user_id'))
    friends_df = getFriendRecoms(session.get('user_id'))
    result_df = pd.concat([friends_df,collabrative_df,content_df],sort=False)
    return result_df[result_df['user_id']==u_id]

#print(gethybridRecom('U4INQZOPSUaj8hMjLlZ3KA')['business_name'])



