from flask import Flask, jsonify, request
from flasgger import Swagger, LazyString, LazyJSONEncoder
from flasgger import swag_from
import pandas as pd
import sqlite3
import re
import numpy as np
import os.path
import json as json
from io import StringIO


app = Flask(__name__)

###############################################################################################################

# Buat connection ke db SQLite
# code di bawah komen ini
BASE_DIR = os.path.dirname(os.path.abspath('__file__'))
# print(BASE_DIR)
db_path = os.path.join(BASE_DIR, "Gold_Binar.db")
conn = sqlite3.connect(db_path, check_same_thread=False)

###############################################################################################################
app.json_encoder = LazyJSONEncoder
swagger_template = dict(
    info = {
        'title': LazyString(lambda:'API Documentation for Data Processing and Modeling'),
        'version': LazyString(lambda:'1.0.0'),
        'description': LazyString(lambda:'Dokumentasi API untuk Data Processing dan Modeling')
        }, host = LazyString(lambda: request.host)
    )
swagger_config = {
        "headers":[],
        "specs":[
            {
            "endpoint":'docs',
            "route":'/docs.json'
            }
        ],
        "static_url_path":"/flasgger_static",
        "swagger_ui":True,
        "specs_route":"/docs/"
    }
swagger = Swagger(app, template=swagger_template, config=swagger_config)

###############################################################################################################
df_raw = pd.read_sql_query('SELECT tweet FROM data50', conn)
# df = df_raw[:15]
df = df_raw.copy()
df['id'] = range(0, len(df))
df['id'] = df['id'].astype('int')
df.index = df['id']
df.head()

df_alay = pd.read_sql_query('SELECT alay, fix_alay FROM new_kamusalay', conn)
df_alay_dict = dict(df_alay.values, orient='index')
df_alay_dict = {r"\b{}\b".format(k): v for k, v in df_alay_dict.items()}

df_abusive = pd.read_sql_query('SELECT abusive, "***" FROM abusive', conn)
df_abusive_dict = dict(df_abusive.values, orient='index')
df_abusive_dict = {r"\b{}\b".format(k): v for k, v in df_abusive_dict.items()}

# reference = https://stackoverflow.com/questions/48824890/replace-words-in-pandas-dataframe-using-dictionary

df_alay_dict

###############################################################################################################

# Buat function simple cleasing misal 'frame' yang logic nya adalah sbb 
# 1. insert df
# 2. copy df menjadi df_get
# 3. buat kolom baru new_tweet yang isinya adalah kolom tweet yang di lower
# 4. kemudian rubah df_get menjadi dict dengan orient='index' dengan nama variable misal 'json' *bisa search cara nya
# 5. return atau output nya adalah variable 'json'
# contoh :
# def frame(df):
#     ...
#     return json
# code di bawah komen ini
# ...
def frame(df):
    df_low = df['tweet'].str.lower()
    df_low = df_low.replace(df_alay_dict, regex=True)
    df_low = df_low.replace(df_abusive_dict, regex=True)
    df_low = pd.DataFrame(df_low)
    df_low =  df_low.rename(columns={'tweet': 'fix'})
    df = pd.concat([df, df_low], axis=1)
    json = df.to_dict('index')
    return(json)

# print(frame(df))

###############################################################################################################

# GET
# @swag_from("docs/index.yml", methods=['GET'])
# @app.route('/', methods=['GET'])
# def test():
# 	return jsonify({'message' : 'It works!'})

@swag_from("docs/index.yml", methods=['GET'])
@app.route('/lang', methods=['GET'])
def returnAll():

    json = frame(df)

    df_upd = pd.DataFrame.from_dict(json, orient='index')
    print('df_upd aman')
    print(df_upd)
    df_upd.to_sql('mytable', conn, if_exists='replace', index=False)
    print('df.to_sql aman')
    sql1 = '''TRUNCATE OR IGNORE data50 fix FROM mytable '''
    sql2 = '''INSERT OR IGNORE INTO data50(tweet, fix) SELECT tweet, fix FROM mytable'''
    cur = conn.cursor()
    print('conn.cursor aman')
    cur.execute(sql1)
    print('execute sql1 aman')
    cur.execute(sql2)
    print('execute sql2 aman')
    conn.commit()
    print('commit aman')
    conn.close()
    print('close aman')
    print(df_raw)

    return jsonify(json)

###############################################################################################################
# GET specific
@swag_from("docs/lang_get.yml", methods=['GET'])
@app.route('/lang/<id>', methods=['GET'])
def returnOne(id):
    id = int(id)+1
    df_get = df.filter(items=[id], axis=0)
    json = frame(df_get)
    df_upd = pd.DataFrame.from_dict(json, orient='index')
    print('df_upd aman')
    print(df_upd)
    df_upd.to_sql('mytable', conn, if_exists='replace', index=False)
    print('df.to_sql aman')
    sql1 = '''TRUNCATE OR IGNORE data50 fix FROM mytable '''
    sql2 = '''INSERT OR IGNORE INTO data50(tweet, fix) SELECT tweet, fix FROM mytable'''
    cur = conn.cursor()
    print('conn.cursor aman')
    cur.execute(sql1)
    print('execute sql1 aman')
    cur.execute(sql2)
    print('execute sql2 aman')
    conn.commit()
    print('commit aman')
    conn.close()
    print('close aman')
    print(df_raw)
    return jsonify(json)

###############################################################################################################
# POST
@swag_from("docs/lang_post.yml", methods=['POST'])
@app.route('/lang', methods=['POST'])
def addOne():
    tweet = {'tweet': request.json['tweet']}
    print(tweet)
    df.loc[len(df) + 1]=[tweet['tweet'],max(df['id'])+1]
    df.index = df['id']
    json = frame(df)
    id = max(df.index)
    json = json[id]
    print(json)
    

    df_upd = pd.DataFrame(pd.Series(json)).T 
    print('df_upd aman')
    print(df_upd)
    df_upd.to_sql('mytable', conn, if_exists='replace', index=False)
    print('df.to_sql aman')
    # sql1 = ''' SELECT tweet, fix FROM mytable '''
    sql2 = '''INSERT OR IGNORE INTO data50(tweet, fix) SELECT tweet, fix FROM mytable'''
    cur = conn.cursor()
    print('conn.cursor aman')
    # cur.execute(sql1)
    print('execute sql1 aman')
    cur.execute(sql2)
    print('execute sql2 aman')
    conn.commit()
    print('commit aman')
    conn.close()
    print('close aman')
    print(df_raw)
    return jsonify(json)

###############################################################################################################
# PUT
@swag_from("docs/lang_put.yml", methods=['PUT'])
@app.route('/lang/<id>', methods=['PUT'])
def editOne(id):
    tweet = {'tweet': request.json['tweet']}
    id = int(id)
    # df_get = df.filter(items=[id], axis=0)

    if id in df['id'].tolist():
        df.loc[id] = [tweet['tweet'],id]

        json = frame(df)

        json = json[id]
        df_upd = pd.DataFrame.from_dict(json, orient='index')
        print('df_upd aman')
        print(df_upd)
        df_upd.to_sql('mytable', conn, if_exists='replace', index=False)
        print('df.to_sql aman')
        sql1 = '''TRUNCATE OR IGNORE data50 fix FROM mytable '''
        sql2 = '''INSERT OR IGNORE INTO data50(tweet, fix) SELECT tweet, fix FROM mytable'''
        cur = conn.cursor()
        print('conn.cursor aman')
        cur.execute(sql1)
        print('execute sql1 aman')
        cur.execute(sql2)
        print('execute sql2 aman')
        conn.commit()
        print('commit aman')
        conn.close()
        print('close aman')
        print(df_raw)

        return jsonify(json)
    else :
        return 'input ulang'

###############################################################################################################
# DELETE
@swag_from("docs/lang_delete.yml", methods=['DELETE'])
@app.route('/lang/<id>', methods=['DELETE'])
def removeOne(id):

    global df
    id = int(id)
    df = df.drop(id)

    json = frame(df)

    df_upd = pd.DataFrame.from_dict(json, orient='index')
    print('df_upd aman')
    print(df_upd)
    df_upd.to_sql('mytable', conn, if_exists='replace', index=False)
    print('df.to_sql aman')
    sql1 = '''TRUNCATE OR IGNORE data50 fix FROM mytable '''
    sql2 = '''INSERT OR IGNORE INTO data50(tweet, fix) SELECT tweet, fix FROM mytable'''
    cur = conn.cursor()
    print('conn.cursor aman')
    cur.execute(sql1)
    print('execute sql1 aman')
    cur.execute(sql2)
    print('execute sql2 aman')
    conn.commit()
    print('commit aman')
    conn.close()
    print('close aman')
    print(df_raw)

    return jsonify(json)

###############################################################################################################

# files = {'formData': ('test.csv', open('test.csv','rb'), 'text/x-spam')}

@swag_from("docs/lang_upload.yml", methods=['POST'])
@app.route('/langs/', methods=['POST'])
# Cari code flask untuk upload file
# file nya di baca oleh pandas
# pandas ekstrak data
# upload ke DB
def post():
    file = request.files['file']
    uploaded = file.read().decode('utf-8').rstrip()
    tweet = []
    for ind, line in enumerate(uploaded.split('\n')):
        if ind%2==0:
            tweet.append(line)
    # save to a dataframe
    df_ul = pd.DataFrame({'tweet':tweet})
    # print(df_ul)

    tezt = StringIO(uploaded)
    colnames=['tweet']
    df_ul1 = pd.read_csv(tezt, sep=";", names=colnames, header=None)
    # print(df_ul1)
    index = df_ul1.index
    # print(index)
    print(df)
    print(len(df))
    for i in index:
        new_id = len(df)
        print('input ini:')
        # df['tweet'][len(df)+1]=[df_ul1['tweet'].iloc[i], max(df['id'])+1]
        # df['tweet'][len(df)]=[df_ul1['tweet'].iloc[i], max(df['id'])+1]
        df.loc[len(df)]=[df_ul1['tweet'][i], max(df['id'])+1]
        # df.index = df['id']
        print(df['tweet'][len(df)-1])
    json = frame(df)
    # id = max(df.index)
    # json = json[id]
    # print(json)

    # df_upd = pd.DataFrame(pd.Series(json)).T 
    df_upd = pd.DataFrame.from_dict(json, orient='index')
    print('df_upd aman')
    print(df_upd)
    df_upd.to_sql('mytable', conn, if_exists='replace', index=False)
    print('df.to_sql aman')
    sql1 = '''TRUNCATE OR IGNORE data50 fix FROM mytable '''
    sql2 = '''INSERT OR IGNORE INTO data50(tweet, fix) SELECT tweet, fix FROM mytable'''
    cur = conn.cursor()
    print('conn.cursor aman')
    cur.execute(sql1)
    print('execute sql1 aman')
    cur.execute(sql2)
    print('execute sql2 aman')
    conn.commit()
    print('commit aman')
    conn.close()
    print('close aman')
    print(df_raw)

    return jsonify(json)

    
    # return ('berhasil')

###############################################################################################################
if __name__ == "__main__":
    app.run()