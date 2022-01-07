from TwitterAPI import TwitterAPI
from pprint import pprint
import pandas as pd
import time
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import numpy as np
import mysql.connector
from mysql.connector import Error
# import os
import re
import datetime
import unicodedata
# import nltk
from textblob import TextBlob
from deep_translator import GoogleTranslator

def extraccion(twitter,query,lon=None,lat=None,rad=None,geo_loc=False,since_id=None,info=False):
    #twitter : acceso con la api
    
    id_anterior = None
    #Creamos los dataframes
    df = pd.DataFrame()
    df2 = pd.DataFrame()

    #Colocamos un numero aleatorio para empezar el proceso
    response_size = 10
    #La variable i nos indica el numero del proceso de extraccion
    i=1

    while(response_size>1):
        if info:
            print('Extraccion No. {}'.format(i))
        
        #La variable since_id y id_anterior corresponden a los limites del rango de ids de tweets que nos devolvera la api (since_id < response < id_anterior)
        if not since_id:
            if geo_loc:
                response = twitter.request('search/tweets', {'q':'{} -is:retweet'.format(query), 'geocode' : '{},{},{}km'.format(lat,lon,rad),'count' : 100, 'result_type' : 'recent', 'lang' : 'es', 'max_id' : id_anterior})
            else:
                response = twitter.request('search/tweets', {'q':'{} -is:retweet'.format(query),'count' : 100, 'result_type' : 'recent', 'lang' : 'es', 'max_id' : id_anterior})
        else:
            if geo_loc:
                response = twitter.request('search/tweets', {'q':'{} -is:retweet'.format(query), 'geocode' : '{},{},{}km'.format(lat,lon,rad),'count' : 100, 'result_type' : 'recent', 'lang' : 'es', 'max_id' : id_anterior, 'since_id' : since_id})
            else:
                response = twitter.request('search/tweets', {'q':'{} -is:retweet'.format(query),'count' : 100, 'result_type' : 'recent', 'lang' : 'es', 'max_id' : id_anterior, 'since_id' : since_id})
        response = response.json()

        #Verificamos si la respuesta nos regreso un error
        if 'errors' in response.keys():
            if response['errors'][0]['message'] == 'Rate limit exceeded':
                print('Limite de tweets superado, esperar 15 minutos.')
                time.sleep(180)
                continue
            else:
                print('Ocurrio un error...')
                pprint(response)
                break
        else:
            temp = pd.json_normalize(response['statuses'])
            response_size = temp.shape[0]
            # print(response_size)
            if response_size > 0:
                id_anterior = temp['id_str'].iloc[-1]
                df = df.append(temp)
        i+=1

    if df.shape[0] > 0:
        if df.duplicated(subset=['id_str']).any():
            df2 = df.copy()
            df2 = df2.drop_duplicates(subset = ['id_str'])
        
    return df,df2

def get_search_values(creds_path,file_name,sheet_name):
    scope = ["https://spreadsheets.google.com/feeds",'https://www.googleapis.com/auth/spreadsheets',"https://www.googleapis.com/auth/drive.file","https://www.googleapis.com/auth/drive"]

    creds = ServiceAccountCredentials.from_json_keyfile_name(creds_path,scope)

    client = gspread.authorize(creds)

    sheet = client.open(file_name).worksheet(sheet_name)
    control = sheet.get_all_records()

    extracciones = pd.DataFrame()

    for hoja in control:
        sheet = client.open(file_name).worksheet(hoja['Sheets Ocupadas'])
        datos = sheet.get_all_records()

        extracciones = extracciones.append(datos)

    extracciones.replace('', np.nan, inplace=True)

    id_vars = ['Estado','Municipio','Radio (km)','Latitud','Longitud']
    value_vars = ['Busqueda1','Busqueda2','Busqueda3','Busqueda4','Busqueda5','Busqueda6']
    var_name = 'Numero de Busqueda'
    value_name = 'Palabra'

    extracciones = extracciones.melt(id_vars=id_vars,value_vars=value_vars,var_name=var_name,value_name=value_name)
    extracciones.dropna(subset=['Palabra'],inplace=True)

    return extracciones

def simple_query():
    error = None
    try:
        connection = mysql.connector.connect(host=host,user=user,password=password,database=database)
        print('Successful connection')
        cursor = connection.cursor()
        insert_query = """SELECT * FROM twitter"""
        cursor.execute(insert_query)
        result = cursor.fetchmany(size = 4)
        connection.commit()
    except Error as e:
        print('Error occured: ', e)
        error = e
    finally:
        if(connection.is_connected()):
            cursor.close()
            connection.close()
            print('Connection is closed')
        return result, error

def last_query(lat=None,lon=None,rad=None,query=None):
    error = None
    try:
        connection = mysql.connector.connect(host=host,user=user,password=password,database=database)
        # print('Successful connection')
        cursor = connection.cursor()
        if lat or lon or rad:
            value = (lat,lon,rad,query)
            insert_query = """SELECT id_proceso,ultimo_tweet FROM twitter_process WHERE lat = %s AND lon = %s AND rad = %s AND palabra = %s ORDER BY id_proceso DESC"""
            cursor.execute(insert_query,value)
        else:
            value = (query)
            insert_query = """SELECT id_proceso,ultimo_tweet FROM twitter_process WHERE lat = "" AND lon = "" AND rad = "" AND palabra = %s ORDER BY id_proceso DESC"""
            cursor.execute(insert_query)
        result = cursor.fetchmany(size = 4)
        connection.commit()
    except Error as e:
        print('Error occured: ', e)
        error = e
    finally:
        if(connection.is_connected()):
            cursor.close()
            connection.close()
            # print('Connection is closed')
        return result, error

def last_id_process():
    error = None
    try:
        connection = mysql.connector.connect(host=host,user=user,password=password,database=database)
        # print('Successful connection')
        cursor = connection.cursor()
        insert_query = """SELECT id_proceso FROM twitter_process ORDER BY id_proceso DESC"""
        cursor.execute(insert_query)
        result = cursor.fetchall()
        connection.commit()
    except Error as e:
        print('Error occured: ', e)
        error = e
    finally:
        if(connection.is_connected()):
            cursor.close()
            connection.close()
            # print('Connection is closed')
        return result, error

def insert_values(value=()):
    error = None
    try:
        connection = mysql.connector.connect(host=host,user=user,password=password,database=database)
        # print('Successful connection')
        cursor = connection.cursor()
        insert_query = """INSERT INTO twitter (id_proceso,tweet_number,lat,lon,rad,created_at,id_str,in_reply_to_user_id_str,text,geo,coordinates,
        place,contributors,retweet_count,favorite_count,retweeted,possibly_sensitive,lang,entities_hashtags,user_id_str,user_name,user_screen_name,
        user_location,user_description,user_url,user_followers_count,user_friends_count,user_listed_count,user_created_at,user_utc_offset,user_geo_enabled,
        user_verified,user_statuses_count,place_id,place_url,place_place_type,place_name,place_full_name,place_country_code,place_country,place_contained_within,
        place_bounding_box_type,place_bounding_box_coordinates) VALUE(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"""
        cursor.execute(insert_query,value)
        connection.commit()
    except Error as e:
        # print('Error occured: ', e)
        error = e
    finally:
        if(connection.is_connected()):
            cursor.close()
            connection.close()
            # print('Connection is closed')
        return error

def upload_data(data=pd.DataFrame(),lat=None,lon=None,rad=None):
    data['created_at'] = pd.to_datetime(data['created_at'],format='%a %b %d %X %z %Y')
    data['created_at'] = data['created_at'].dt.tz_convert('America/Mexico_City')
    data['user.created_at'] = pd.to_datetime(data['user.created_at'],format='%a %b %d %X %z %Y')
    data['user.created_at'] = data['user.created_at'].dt.tz_convert('America/Mexico_City')
    data['favorite_count'].fillna('-1',inplace=True)
    data['retweet_count'].fillna('-1',inplace=True)
    data['user.followers_count'].fillna('-1',inplace=True)
    data['user.friends_count'].fillna('-1',inplace=True)
    data['user.listed_count'].fillna('-1',inplace=True)
    data['user.statuses_count'].fillna('-1',inplace=True)
    data.fillna('',inplace=True)

    id_proceso=None

    result,_ = last_id_process()
    if result:
        id_proceso = int(result[0][0]) + 1
    else:
        id_proceso = 0

    counter = 0

    for index, row in data.iterrows():
        try:
            value = (id_proceso,counter,lat,lon,rad,row.created_at.strftime('%Y-%m-%d %X'),row.id_str,row.in_reply_to_status_id_str,row.text,row.geo,row.coordinates,row.place,row.contributors,row.retweet_count,row.favorite_count,row.retweeted,row.possibly_sensitive,row.lang,
            str(row['entities.hashtags']),row['user.id_str'],row['user.name'],row['user.screen_name'],row['user.location'],row['user.description'],row['user.url'],row['user.followers_count'],row['user.friends_count'],
            row['user.listed_count'],row['user.created_at'].strftime('%Y-%m-%d %X'),row['user.utc_offset'],row['user.geo_enabled'],row['user.verified'],row['user.statuses_count'],row['place.id'],row['place.url'],
            row['place.place_type'],row['place.name'],row['place.full_name'],row['place.country_code'],row['place.country'],str(row['place.contained_within']),row['place.bounding_box.type'],str(row['place.bounding_box.coordinates']))
            error = insert_values(value=value)
            
            if not error:
                counter += 1
        except:
            print('Ocurrio un error')

    # Localizamos el tweet mas reciente
    id_str = data.loc[data.created_at.idxmax()].id_str

    return id_proceso,counter,id_str

def insert_process(value):
    error = None
    try:
        connection = mysql.connector.connect(host=host,user=user,password=password,database=database)
        # print('Successful connection')
        cursor = connection.cursor()
        insert_query = """INSERT INTO twitter_process (id_proceso,lat,lon,rad,descripcion,palabra,numero_tweets,ultimo_tweet,fecha) VALUE(%s,%s,%s,%s,%s,%s,%s,%s,%s);"""
        cursor.execute(insert_query,value)
        result = cursor.fetchall()
        connection.commit()
    except Error as e:
        print('Error occured: ', e)
        error = e
    finally:
        if(connection.is_connected()):
            cursor.close()
            connection.close()
            # print('Connection is closed')
        return error

def fetch_table_data(table_name,filter=None):
    error = None
    try:
        connection = mysql.connector.connect(host=host,user=user,password=password,database=database)
        # print('Successful connection')
        cursor = connection.cursor()
        insert_query = """SELECT * FROM {}""".format(table_name)
        if filter:
            insert_query = insert_query + ' WHERE ' + filter + ';'
        else:
            insert_query = insert_query + ';'
        # print(insert_query)
        cursor.execute(insert_query)
        headers = [row[0] for row in cursor.description]
        result = cursor.fetchall()
        df = pd.DataFrame(result,columns=headers)
        connection.commit()
    except Error as e:
        print('Error occured: ', e)
        error = e
    finally:
        if(connection.is_connected()):
            cursor.close()
            connection.close()
            # print('Connection is closed')
        return df,error

def cleanTXT(text):
    text = re.sub(r'_','',text)
    text = re.sub(r'@[A-Za-z0-9]+','',text)
    text = re.sub(r'#','',text)
    text = re.sub(r'RT[\s]+','',text)
    text = re.sub(r'https?:\/\/\S+','',text)

    return text

def getAnalysis(score):
    if score < 0:
        return 'NEGATIVE'
    elif score == 0:
        return 'NEUTRAL'
    else:
        return 'POSITIVE'

def update_txtanalysis_data(value):
    error = None
    try:
        connection = mysql.connector.connect(host=host,user=user,password=password,database=database)
        # print('Successful connection')
        cursor = connection.cursor()
        insert_query = """UPDATE twitter SET text_polarity = %s, text_subjectivity = %s, text_analysis = %s WHERE id_str = %s;"""
        # print(insert_query)
        cursor.execute(insert_query,value)
        connection.commit()
    except Error as e:
        print('Error occured: ', e)
        error = e
    finally:
        if(connection.is_connected()):
            cursor.close()
            connection.close()
            # print('Connection is closed')
        return error

def sentiment_analysis():
    data1,_ = fetch_table_data(table_name='twitter',filter='text_analysis IS NULL')
    data2,_ = fetch_table_data(table_name='twitter_process')

    filtro = ['id_proceso','id_str','text']
    data = data1[filtro].merge(data2,on='id_proceso',how='left').drop(columns=['fecha','id_proceso','lat','lon','rad','numero_tweets','ultimo_tweet'])

    data['tweets'] = data['text'].apply(cleanTXT)

    traducir = lambda x: GoogleTranslator(source='es', target='en').translate(x)
    data['blob_en'] = data['tweets'].apply(traducir)

    pol = lambda x: TextBlob(x).sentiment.polarity
    data['text_polarity'] = data['blob_en'].apply(pol)

    sub = lambda x: TextBlob(x).sentiment.subjectivity
    data['text_subjectivity'] = data['blob_en'].apply(sub)

    data['text_analysis'] = data['text_polarity'].apply(getAnalysis)

    for index,row in data.iterrows():
        value = (row.text_polarity,row.text_subjectivity,row.text_analysis,row.id_str)
        _ = update_txtanalysis_data(value)
