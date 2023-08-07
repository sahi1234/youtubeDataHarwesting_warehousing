import pandas as pd
import streamlit as st
import pymongo
import mysql.connector
import numpy as np
#from sqlalchemy import create_engine

mongo = pymongo.MongoClient("mongodb://127.0.0.1:27017/")

mycon = mysql.connector.connect(host ="localhost",
                                user="root",
                                password="12345",
                                database="youtube_data")
mycursor = mycon.cursor()

# v_channel_id ="UCnz-ZXXER4jOvuED5trXfEA"
# my_db = mongo["techTFQ"]
# channel_coll = my_db["Channels"]
# playlist_coll = my_db["PlatLists"]
# videos_coll = my_db["Videos"]
# comment_coll = my_db["Comments"]
#
# # Accessing data from mongodb for transfer of data
#
# channelData= channel_coll.find({"Channel_Id":v_channel_id},{'_id':0})
# channel_df = pd.DataFrame(channelData)
# print(len(channel_df))
#
# playlistData= playlist_coll.find({"Channel_Id": v_channel_id}, {'_id':0})
# playlist_df = pd.DataFrame(playlistData)
# print(len(playlist_df))
#
# videosData= videos_coll.find({"Channel_Id":v_channel_id},{'_id':0})
# videos_df = pd.DataFrame(videosData)
# print(len(videos_df))
#
# commentsData= comment_coll.find({},{'_id':0})
# comments_df = pd.DataFrame(commentsData)
# merged_df = pd.merge(videos_df,comments_df,on='video_Id',how = 'left')
# selected_df = merged_df[['comment_Id','video_Id','comment_text','comment_author','comment_published_date','Channel_Id']]
# comments_data_df = selected_df[selected_df["Channel_Id"]==v_channel_id].replace({np.nan,None})
# print(comments_data_df)

# mysql operations...
#function to insert data into mysql table: channels
def insert_into_channels(channel_df):

    cols = ",".join([str(i) for i in channel_df.columns.tolist()])
    for i, row in channel_df.iterrows():
        data = row.tolist()
        #print(data)
        query = "INSERT INTO channels (" + cols + ") VALUES (" + "%s," * (len(row) - 1) + "%s)"
        mycursor.execute(query, data)
    mycon.commit()


#insert_into_channels(channel_df)

#function to insert data into mysql table: playlists
def insert_into_playlists(playlist_df):

  #  cols = ",".join([str(i) for i in playlist_df.columns.tolist()])
    for i, row in playlist_df.iterrows():
        data = row.tolist()
        #print(data)
        query = "INSERT INTO playlists (Playlist_id,Channel_Id,Playlist_Name) VALUES(%s,%s,%s)"
        mycursor.execute(query, data)
    mycon.commit()

#insert_into_playlists(playlist_df)

#function to insert data into mysql table: videos
def insert_into_videos(videos_df):

 #   cols = ",".join([str(i) for i in videos_df.columns.tolist()])
    for i, row in videos_df.iterrows():
        data = row.tolist()
        #print(data)
        print(len(row))
        query = "INSERT INTO videos (video_id,Channel_Id,video_title,video_description,published_at,view_count,like_count,favourite_count,comment_count,duration) VALUES(%s,%s, %s, %s, %s, %s, %s, %s, %s, %s)"
        mycursor.execute(query, data)
    mycon.commit()

#insert_into_videos(videos_df)

#function to insert data into mysql table: comments
def insert_into_comments(comments_data_df):

 #   cols = ",".join([str(i) for i in comments_df.columns.tolist()])
    for i, row in comments_data_df.iterrows():
        data = row.tolist()
        #print(data)
        query = "INSERT INTO comments (comment_id,video_id,comment_text,comment_author,published_date,channel_id) VALUES(%s,%s,%s,%s,%s,%s)"
        mycursor.execute(query, data)
    mycon.commit()

#insert_into_comments(comments_data_df)

def execute_radio_option(num):
    print("Working on Data...........")
    print(num)
    if num == 1:
        query = """select channel_name, video_title from youtube_data.channels c
inner join youtube_data.videos v on c.Channel_Id=v.Channel_Id
GROUP BY channel_name, video_title"""
    elif num == 2:
        query = """select channel_name, video_count from 
(select channel_name ,video_count ,rank() over (partition by channel_name order by video_count desc) rnk from
(select channel_name, count(video_id) as video_count
from youtube_data.channels c
inner join youtube_data.videos v on c.Channel_Id=v.Channel_Id
GROUP BY channel_name order by video_count) a 
)b where rnk =1"""
    elif num == 3:
        query = """select video_title, channel_name from 
youtube_data.channels c
inner join youtube_data.videos v on c.Channel_Id=v.Channel_Id 
order by view_count desc limit 10"""
    elif num == 4:
        query = """select video_title, comment_count from youtube_data.videos"""
    elif num == 5:
        query = """select video_title, channel_name from 
youtube_data.channels c
inner join youtube_data.videos v on c.Channel_Id=v.Channel_Id where like_count=
(select max(like_count) from youtube_data.videos)"""
    elif num == 6:
        query = """select video_title, like_count from youtube_data.videos;"""
    elif num == 7:
        query = """select channel_name,sum(view_count) as no_of_views from youtube_data.channels c
inner join youtube_data.videos v on c.Channel_Id=v.Channel_Id 
group by c.channel_id"""
    elif num == 8:
        query = """select  channel_name from youtube_data.channels c
inner join youtube_data.videos v on c.Channel_Id=v.Channel_Id 
 where date(substr(published_at,1,10)) >= date('2022-01-01') group by channel_name"""
    elif num == 9:
        query = """select channel_name, avg(time_to_sec(duration))/60 as avg_duration_in_secs from youtube_data.channels c
inner join youtube_data.videos v on c.Channel_Id=v.Channel_Id 
 group by v.channel_id"""
    else:
        query = """select video_title, channel_name,comment_count from 
youtube_data.channels c
inner join youtube_data.videos v on c.Channel_Id=v.Channel_Id where comment_count=
(select max(comment_count) from youtube_data.videos)"""

    #result = mycursor.execute(query)
    df = pd.read_sql(query,mycon)
    print(df)
    return df
