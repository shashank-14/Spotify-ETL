import json
import os
import boto3
import pandas as pd
from datetime import datetime
from io import StringIO

def album(data):
    album_list = []
    for a in data['items']:
        album_name = a['track']['album']['name']
        album_id = a['track']['album']['id']
        album_release_date = a['track']['album']['release_date']
        album_total_tracks = a['track']['album']['total_tracks']
        album_external_url = a['track']['album']['external_urls']['spotify']
        album_dict = {"album_id":album_id, "album_name":album_name, "album_release_date":album_release_date,
                  "album_tracks":album_total_tracks,"album_url":album_external_url }
        album_list.append(album_dict)
    return album_list
    
def artist(data):
    artist_list=[]
    for a in data['items']:
        for key, value in a['track'].items():
            if key == "artists":
                for b in a['track']['artists']:
                    artist_id = b['id']
                    artist_name = b['name']
                    artist_url = b['href']
                    artist_dict = {"artist_id":artist_id, "artist_name":artist_name, "url":artist_url}
                    artist_list.append(artist_dict)
    return artist_list
    

def songs(data):
    song_list = []
    for a in data['items']:
        song_id = a['track']['id']
        song_name = a['track']['name']
        song_duration_ms = a['track']['duration_ms']
        song_popularity = a['track']['popularity']
        song_external_url = a['track']['external_urls']['spotify']
        song_added_at = a['added_at']
        song_album_id = a['track']['album']['id']
        song_artist_id = a['track']['album']['artists'][0]['id']
        song_dict = {"song_id":song_id, "song_name":song_name, "song_duration_ms":song_duration_ms,
        "song_popularity":song_popularity,"song_external_url":song_external_url, "song_added_at":song_added_at ,
        "song_album_id":song_album_id, "song_artist_id":song_artist_id}
        song_list.append(song_dict)
    return song_list
    

def lambda_handler(event, context):
    s3 = boto3.client('s3')
    Bucket = "etl-spotify-shashank"
    Key = "raw-data/to-process/"
    
    spotify_data = []
    spotify_keys = []
    
    for a in s3.list_objects(Bucket=Bucket, Prefix=Key)['Contents']:
        if a['Key'].split('.')[-1]=="json":
            response = s3.get_object(Bucket = Bucket, Key = a['Key'])
            content = response['Body']
            jasonObject = json.loads(content.read())
            spotify_data.append(jasonObject)
            spotify_keys.append(a['Key'])
    
    for d in spotify_data:
        album_list=album(d)
        artist_list=artist(d)
        song_list=songs(d)
        
        album_df = pd.DataFrame.from_dict(album_list)
        artist_df = pd.DataFrame.from_dict(artist_list)
        song_df = pd.DataFrame.from_dict(song_list)
        
        album_key = "transformed-data/album-data/album transformed-"+str(datetime.now())+'.csv'
        album_buffer = StringIO()
        album_df.to_csv(album_buffer, index = False)
        album_content = album_buffer.getvalue()
        s3.put_object(Bucket=Bucket, Key=album_key, Body=album_content)
        
        artist_key = "transformed-data/artist-data/artist transformed-"+str(datetime.now())+'.csv'
        artist_buffer = StringIO()
        artist_df.to_csv(artist_buffer, index = False)
        artist_content = artist_buffer.getvalue()
        s3.put_object(Bucket=Bucket, Key=artist_key, Body=artist_content)
        
        song_key = "transformed-data/songs-data/song transformed-"+str(datetime.now())+'.csv'
        song_buffer = StringIO()
        song_df.to_csv(song_buffer, index = False)
        song_content = song_buffer.getvalue()
        s3.put_object(Bucket=Bucket, Key=song_key, Body=song_content)
        
    s3_res = boto3.resource('s3')
    for b in spotify_keys:
        copy_source = {
            'Bucket':Bucket,
            'Key':b
        }
        s3_res.meta.client.copy(copy_source,Bucket,'raw-data/processed/'+b.split("/")[-1])
        s3_res.Object(Bucket,b).delete()