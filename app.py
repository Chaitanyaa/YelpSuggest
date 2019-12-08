from __future__ import division, print_function
# coding=utf-8


# Flask utils
from pydoc import html

import boto3
import folium as folium
import pandas as pd
from flask import Flask, redirect, url_for,session,request, render_template
from werkzeug.utils import secure_filename
from gevent.pywsgi import WSGIServer
import pyarrow.parquet as pq
import s3fs
import numpy as np

from content import getKeyWordsRecoms

s3 = s3fs.S3FileSystem(anon=False, key='*******', secret='********')
s3 = s3fs.S3FileSystem(anon=False, key='*******', secret='********')
s3client = boto3.client(
    's3',
    aws_access_key_id='********',
    aws_secret_access_key='*********'
)

# Define a flask app
app = Flask(__name__)
app.secret_key='yelpsuggest'

from collabrative import getCollabRecom
from content import contentRecommed

def avg_topic_rating (ratings_df , counts_df):
    """
    Gets the average rating per topic 
    """
    ratings_array = np.array(ratings_df)
    counts_array = np.array(counts_df)
    mult = np.multiply(ratings_array,counts_array)
    sum_ratings = np.sum(np.nan_to_num(mult),axis=0)
    avg_ratings = sum_ratings / np.sum(counts_array,axis = 0)
    avg_ratings_df = pd.DataFrame(avg_ratings.reshape(1, len(avg_ratings)) ,
                                  columns= ratings_df.columns, index=['Avg Topic Rating'])
    return avg_ratings_df

@app.route('/', methods=['GET'])
def index():
    for key in session.copy():
        session.pop(key, None)
    return render_template('index.html')

@app.route('/suggest', methods=['GET', 'POST'])
def suggest():
    varieties = 0
    ambience = 0
    management = 0
    service = 0
    value = 0
    if request.method == 'POST':
        # Get the file from post request
        userid = request.form['userid']
        if userid == "":
            return redirect('/newuser', code=307)  # Handle New user case------
        # id = '_0x7W6fizaPP76xNBxBLAQ'
        id = userid
        print(userid)
        finalres = pd.read_csv("./finalres.csv", index_col="business_id")
        finalrescnt = pd.read_csv("./finalrescnt.csv", index_col="business_id")
        if finalres[finalres.index == id].shape[0] == 0:
            return redirect('/usersuggest', code=307)
        threshold = avg_topic_rating(finalres.iloc[:, 0:5], finalrescnt)
        if (finalres[finalres.index == id]['varieties'][0] >= threshold['varieties'][0]):
            varieties = 1
            # print("This is "+str(varieties))
        if (finalres[finalres.index == id]['ambience'][0] >= threshold['ambience'][0]):
            ambience = 1
        if (finalres[finalres.index == id]['management'][0] >= threshold['management'][0]):
            management = 1
        if (finalres[finalres.index == id]['service'][0] >= threshold['service'][0]):
            service = 1
        if (finalres[finalres.index == id]['value'][0] >= threshold['value'][0]):
            value = 1

        return render_template('suggest.html', varieties=varieties, ambience=ambience, management=management,
                               service=service, value=value, rating=finalres[finalres.index == id]['rating'][0],
                               name=finalres[finalres.index == id]['business_name'][0])

def getCardsValue(u_id):
    s3client = boto3.client(
        's3',
        aws_access_key_id='*******',
        aws_secret_access_key='*******'
    )

    obj = s3client.get_object(Bucket='yelpsuggest', Key='Data/Final_Result/content1.csv')
    content_df = pd.read_csv(obj['Body'])
    liked_business = pq.ParquetDataset('s3://yelpsuggest/Data/Final_Result/prev_business.parquet',
                                       filesystem=s3).read_pandas().to_pandas()
    prev_business = liked_business[liked_business['user_id']==u_id].head(5)
    content_recom_df = content_df[content_df['user_id']==u_id]
    return content_recom_df['business_name'],prev_business['business_name']

@app.route('/usersuggest',methods=['GET','POST'])
def usersuggest():
    if request.form.get('search') == None:
        userid = request.form['userid']
        session['user_id'] = userid
        c_df, p_df = getCardsValue(userid)
        return render_template('usersuggest.html', c_df=c_df, p_df=p_df)
    else:
        session['keyword'] = request.form.get('search')
        return render_template('usersuggest.html')


@app.route('/getmap', methods=['GET'])
def getmap():
    if session.get('user_id') != None:
        if session.get('keyword') != None:
            result_df = getKeyWordsRecoms(session.get('keyword'), 5).toPandas()
            for key in session.copy():
                print(key)
                if key == 'keyword':
                    session.pop(key, None)
        else:
            result_df = contentRecommed(session.get('user_id'))
        mp = folium.Map(location=[36.1699, -115.1398], zoom_start=12)
        for i, r in result_df.iterrows():
            folium.Marker(location=[float(r.latitude), float(r.longitude)],
                          popup=html.escape(r["business_name"]) + '<br>' + 'Stars: ' + str(
                              r.stars) + '<br>' + 'Reviews: ' + str(r.review_count),
                          icon=folium.Icon(color='green')).add_to(mp)
        return mp._repr_html_()

@app.route('/newuser', methods=['GET','POST'])
def newuser():
    if request.method == 'POST':
        if request.form.get('search') != None:
            session['keyword'] = request.form.get('search')
    return render_template('newuser.html')

@app.route('/getmapNewuser', methods=['GET','POST'])
def getmapNewuser():
    result_df = pd.DataFrame()
    if session.get('keyword') != None:
        result_df = getKeyWordsRecoms(session.get('keyword'), 5).toPandas()
        for key in session.copy():
            session.pop(key , None)
    mp = folium.Map(location=[36.1699, -115.1398], zoom_start=12)
    for i, r in result_df.iterrows():
        folium.Marker(location=[float(r.latitude), float(r.longitude)],
                      popup=html.escape(r["business_name"]) + '<br>' + 'Stars: ' + str(
                          r.stars) + '<br>' + 'Reviews: ' + str(r.review_count),
                      icon=folium.Icon(color='green')).add_to(mp)
    return mp._repr_html_()

@app.route('/colabrecom', methods=['GET','POST'])
def colabrecom():
    return render_template('colabrecom.html')

@app.route('/getmapColab', methods=['GET','POST'])
def getmapColab():
    if session.get('user_id') != None:
        result_df = getCollabRecom(session.get('user_id'))
        mp = folium.Map(location=[36.1699, -115.1398], zoom_start=12)
        for i, r in result_df.iterrows():
            folium.Marker(location=[float(r.latitude), float(r.longitude)],
                          popup=html.escape(r["business_name"]) + '<br>' + 'Stars: ' + str(
                              r.stars) + '<br>' + 'Reviews: ' + str(r.review_count),
                          icon=folium.Icon(color='green')).add_to(mp)
        return mp._repr_html_()


@app.route('/hybridrecom', methods=['GET','POST'])
def hybridrecom():
    return render_template('hybrid.html')

@app.route('/getmapHybrid', methods=['GET','POST'])
def getmapHybrid():
    if session.get('user_id') != None:
        result_df = getCollabRecom(session.get('user_id'))
        mp = folium.Map(location=[36.1699, -115.1398], zoom_start=12)
        for i, r in result_df.iterrows():
            folium.Marker(location=[float(r.latitude), float(r.longitude)],
                          popup=html.escape(r["business_name"]) + '<br>' + 'Stars: ' + str(
                              r.stars) + '<br>' + 'Reviews: ' + str(r.review_count),
                          icon=folium.Icon(color='green')).add_to(mp)
        return mp._repr_html_()


if __name__ == '__main__':
    http_server = WSGIServer(('127.0.0.1', 5000), app)
    http_server.serve_forever()


    

