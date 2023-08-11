from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import pandas as pd
import streamlit as st
import pymongo
import mysql.connector
import re
import numpy as np


# Set up the API client
api_key = "AIzaSyAU6QWHKRub3CjNfwMYFOBkXxhN1xlznj8"
youtube = build('youtube', 'v3', developerKey=api_key)

mongo = pymongo.MongoClient("mongodb://127.0.0.1:27017/")

mycon = mysql.connector.connect(host ="localhost",
                                user="root",
                                password="12345",
                                database="youtube_data")
mycursor = mycon.cursor()

# Youtube channels data Extraction
# function for getting channel statistics
def get_channel_stats(youtube, channel_id):
    request = youtube.channels().list(part = "snippet,contentDetails,statistics,status",id = channel_id)
    response = request.execute()
    all_data =[]
    for i in range(len(response['items'])):
        data = dict(Channel_name = response['items'][i]['snippet']['title'],
                    Channel_Id = response['items'][i]['id'],
                    Subscription_Count = response['items'][i]['statistics']['subscriberCount'],
                    Channel_Views =  response['items'][i]['statistics']['viewCount'],
                    Channel_Description = response['items'][i]['snippet']['description'],
                    Channel_status = response['items'][i]['status']['privacyStatus'],
                    Playlist_Id = response['items'][i]['contentDetails']['relatedPlaylists']['uploads']
            )
        all_data.append(data)
    return all_data

# function for getting playlist statistics
def get_playlist_stats(youtube, channel_id):
    request = youtube.playlists().list(
                         part = 'contentDetails, snippet',
                         channelId = channel_id,
                        maxResults = 50)
    response = request.execute()

    all_playlist_data = []
    for i in range(len(response['items'])):

        playList_stats = dict (
                         PlayListId = response['items'][i]['id'],
                         Channel_Id = response['items'][i]['snippet']['channelId'],
                         Playlist_Name = response['items'][i]['snippet']['title']
                         )
        all_playlist_data.append(playList_stats)
    next_page_token = response.get('nextPageToken')
    more_pages = True
    while more_pages:
        if next_page_token is None:
            more_pages = False
        else:
            request = youtube.playlists().list(
                part = 'contentDetails, snippet',
                channelId = channel_id,
                maxResults=50,
                pageToken = next_page_token)
            response = request.execute()

            for i in range(len(response['items'])):

                playList_stats = dict(
                    PlayListId = response['items'][i]['id'],
                    Channel_Id = response['items'][i]['snippet']['channelId'],
                    Playlist_Name = response['items'][i]['snippet']['title']
                )
                all_playlist_data.append(playList_stats)
            next_page_token = response.get('nextPageToken')

    return all_playlist_data



# playList_data = pd.DataFrame(playlist_statistics)
# print(playList_data)

# function for getting video ids
def get_video_ids(youtube,playlist_id):
    request = youtube.playlistItems().list(
                         part = 'contentDetails',
                         playlistId = playlist_id,
                        maxResults = 50)
    response = request.execute()
    video_ids = []
    for i in range(len(response['items'])):
        video_ids.append(response['items'][i]['contentDetails']['videoId'])
    next_page_token = response.get('nextPageToken')
    more_pages = True
    while more_pages:
        if next_page_token is None:
            more_pages = False
        else:
            request = youtube.playlistItems().list(
                part='contentDetails',
                playlistId=playlist_id,
                maxResults=50,
                pageToken = next_page_token)
            response = request.execute()

            for i in range(len(response['items'])):
                video_ids.append(response['items'][i]['contentDetails']['videoId'])
            next_page_token = response.get('nextPageToken')
    return video_ids

# function to get video details
def get_video_details(youtube,video_ids):

    all_video_stats =[]
    for i in range(0, len(video_ids), 50):
        request = youtube.videos().list(
                    part = 'snippet, contentDetails, statistics',
                    id = ','.join(video_ids[i:i+50]))
        response = request.execute()
        for j in range(len(response['items'])):
            video_stats = dict(video_Id = response['items'][j]['id'],
                               Channel_Id = response['items'][j]['snippet']['channelId'],
                               video_title = response['items'][j]['snippet']['title'],
                               video_description = response['items'][j]['snippet']['description'],
                               #video_tags = response['items'][j]['snippet']['tags'],
                               Published_At = response['items'][j]['snippet']['publishedAt'],
                               View_count = response['items'][j]['statistics']['viewCount'],
                               Like_Count = response['items'][j]['statistics']['likeCount'],
                               #Dislike_count = response['items'][j]['statistics']['dislikeCount'],
                               Favorite_count = response['items'][j]['statistics']['favoriteCount'],
                               Comment_count = response['items'][j]['statistics']['commentCount'],
                               Duration = convert_duration(response['items'][j]['contentDetails']['duration'])
                               )
            all_video_stats.append(video_stats)
    return all_video_stats

#function to covert video duration into seconds
def convert_duration(duration):
    regex = r'PT(\d+H)?(\d+M)?(\d+S)?'
    match=re.match(regex,duration)
    if not match:
        return '00:00:00'
    hours,minutes,seconds = match.groups()
    hours = int(hours[:-1]) if hours else 0
    minutes = int(minutes[:-1]) if minutes else 0
    seconds = int(seconds[:-1]) if seconds else 0
    total_seconds = hours*3600 + minutes*60 + seconds
    return '{:02d}:{:02d}:{:02d}'.format(int(total_seconds/3600),int(total_seconds % 3600)%60,int(total_seconds % 3600)//60)

#function to get comments details
def get_comment_details(youtube, video_ids):

    all_comment_stats = []
    for video in range(0, len(video_ids)):
        id = video_ids[video]

        request = youtube.commentThreads().list(
                     part = 'snippet,replies',
                     videoId = id,
                    maxResults = 100)
        response = request.execute()
        for k in range(len(response['items'])):
            comments_stats = dict(
                            comment_Id = response['items'][k]['id'],
                            video_Id = response['items'][k]['snippet']['videoId'],
                            comment_text = response['items'][k]['snippet']['topLevelComment']['snippet']['textDisplay'],
                            comment_author = response['items'][k]['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            comment_published_date = response['items'][k]['snippet']['topLevelComment']['snippet']['publishedAt']
                            )
            all_comment_stats.append(comments_stats)
        next_page_token = response.get('nextPageToken')
        more_pages = True
        while more_pages:
            if next_page_token is None:
                more_pages = False
            else:
                request = youtube.commentThreads().list(
                    part='snippet, replies',
                    videoId = id,
                    maxResults=100,
                    pageToken=next_page_token)
                response = request.execute()

                for k in range(len(response['items'])):
                    comments_stats = dict(
                        comment_Id=response['items'][k]['id'],
                        video_Id=response['items'][k]['snippet']['videoId'],
                        comment_text=response['items'][k]['snippet']['topLevelComment']['snippet']['textDisplay'],
                        comment_author=response['items'][k]['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        comment_published_date=response['items'][k]['snippet']['topLevelComment']['snippet']['publishedAt']
                        )
                    all_comment_stats.append(comments_stats)
                next_page_token = response.get('nextPageToken')


    return all_comment_stats
# comments_data = pd.DataFrame(comments_statistics)
# print(comments_data)

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
)b where rnk =1 order by video_count desc"""
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
