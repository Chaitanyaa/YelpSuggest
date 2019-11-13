from __future__ import division, print_function
# coding=utf-8


# Flask utils
from flask import Flask, redirect, url_for, request, render_template
from werkzeug.utils import secure_filename
from gevent.pywsgi import WSGIServer

# @title Imports
# DataFrame
import pandas as pd
import numpy as np

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

# Define a flask app
app = Flask(__name__)

@app.route('/', methods=['GET'])
def index():
    return render_template('index.html')

@app.route('/suggest', methods=['GET', 'POST'])
def suggest():
    varieties=0
    ambience=0
    management=0
    service=0
    value=0
    if request.method == 'POST':
        #Get the file from post request
        userid = request.form['userid']
        #id = '_0x7W6fizaPP76xNBxBLAQ'
        id = userid
        print(userid)
        
        finalres = pd.read_csv("./finalres.csv",index_col="business_id")
        finalrescnt = pd.read_csv("./finalrescnt.csv",index_col="business_id")
        #print(finalres.head())
        threshold = avg_topic_rating(finalres,finalrescnt)
        if(finalres[finalres.index == id]['varieties'][0] >= threshold['varieties'][0]):
            varieties = 1
            #print("This is "+str(varieties))
        if(finalres[finalres.index == id]['ambience'][0] >= threshold['ambience'][0]):
            ambience = 1
        if(finalres[finalres.index == id]['management'][0] >= threshold['management'][0]):
            management = 1
        if(finalres[finalres.index == id]['service'][0] >= threshold['service'][0]):
            service = 1
        if(finalres[finalres.index == id]['value'][0] >= threshold['value'][0]):
            value = 1   
    
        return render_template('suggest.html',varieties=varieties,ambience=ambience,management=management,service=service,value=value)

if __name__ == '__main__':
    #http_server = WSGIServer(('127.0.0.1', 5000), app)
    #http_server.serve_forever()
	app.run(host="127.0.0.1",port=8080,debug=True)
    
    
    