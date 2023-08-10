from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import pandas as pd
import json
import seaborn as sns
import streamlit as st
import pymongo
import mysql.connector
import re
import numpy as np
from function_list import *


# # Set up the API client
# api_key = "AIzaSyAU6QWHKRub3CjNfwMYFOBkXxhN1xlznj8"
# youtube = build('youtube', 'v3', developerKey=api_key)

#Mongodb connection string
mongo=pymongo.MongoClient("mongodb://127.0.0.1:27017/")

#mysql connection string
mycon = mysql.connector.connect(host ="localhost",
                                user="root",
                                password="12345",
                                database="youtube_data")
mycursor = mycon.cursor()


# channel_id = "UCnz-ZXXER4jOvuED5trXfEA"
# Channel_name = "techTFQ"
def extract_data(channel_ids):

    print(channels)
    st.write("Channel data is extracting......This take some time.....")
    for channel_id in channels:
        print(channel_id)
        channel_statistics = get_channel_stats(youtube, channel_id)
        # print(channel_statistics)
        channel_data = pd.DataFrame(channel_statistics)
        print(channel_data)

        playlist_id = channel_data[channel_data['Channel_Id'] == channel_id].iloc[0, -1]
        #channel_name = channel_data[channel_data['Channel_Id'] == channel_id].iloc[0, 0]
        print(playlist_id)
        playlist_statistics = get_playlist_stats(youtube, channel_id)
        # print(playlist_statistics)

        video_ids = get_video_ids(youtube, playlist_id)
        # print(video_ids)
        video_statistics = get_video_details(youtube, video_ids)
        # print(video_data)
        comments_statistics = get_comment_details(youtube, video_ids)
        # print(comments_statistics)

        # Inserting data to MongoDB
        my_db = mongo["youtube_data"]
        channel_collection = my_db["Channels"]
        playlist_collection = my_db["PlayLists"]
        video_collection = my_db["Videos"]
        comment_collection = my_db["Comments"]
        dbs = mongo.list_databases()
        for db in dbs:
            print(db)

        channel_collection.insert_many(channel_statistics)
        playlist_collection.insert_many(playlist_statistics)
        video_collection.insert_many(video_statistics)
        comment_collection.insert_many(comments_statistics)

    print("data inserted in Mongodb successfully............")


def transfer_data(channels):

    print(channels)
    for v_channel_id in channels:
        my_db = mongo["youtube_data"]
        channel_coll = my_db["Channels"]
        playlist_coll = my_db["PlayLists"]
        videos_coll = my_db["Videos"]
        comment_coll = my_db["Comments"]

        # Accessing data from mongodb for transfer of data

        channelData = channel_coll.find({"Channel_Id": v_channel_id}, {'_id': 0})
        channel_df = pd.DataFrame(channelData).replace({np.nan,None})
        print(len(channel_df))

        playlistData = playlist_coll.find({"Channel_Id": v_channel_id}, {'_id': 0})
        playlist_df = pd.DataFrame(playlistData).replace({np.nan,None})
        print(len(playlist_df))

        videosData = videos_coll.find({"Channel_Id": v_channel_id}, {'_id': 0})
        videos_df = pd.DataFrame(videosData).replace({np.nan,None})
        print(len(videos_df))

        commentsData = comment_coll.find({}, {'_id': 0})
        comments_df = pd.DataFrame(commentsData)
        merged_df = pd.merge(videos_df, comments_df, on='video_Id', how='left')
        selected_df = merged_df[
            ['comment_Id', 'video_Id', 'comment_text', 'comment_author', 'comment_published_date', 'Channel_Id']]
        comments_data_df = selected_df[selected_df["Channel_Id"] == v_channel_id].replace({np.nan,None})
        print(len(comments_data_df))

        insert_into_channels(channel_df)
        insert_into_playlists(playlist_df)
        insert_into_videos(videos_df)
        insert_into_comments(comments_data_df)
    print("data transferred to  Mysql successfully....")


st.title(':blue[YouTube Data Harvesting and Warehousing]')
channel_ids_input = st.text_area(":violet[Enter channel Ids(comma separated) you want to fetch data]")
channels = channel_ids_input.split(",")
s_btn = st.button(":orange[SUBMIT]",on_click=extract_data,args=(channels,))

transfer_input = st.multiselect(":violet[Select channel ids to transfer data from mongo db to sql db]",options = channels)
s_btn1 = st.button(":orange[TRANSFER]", on_click = transfer_data,args=(channels,))

query_select = st.radio(":violet[Please select an option of Sql data analysis]",[
    "None",
    "Show the names of all the videos and their corresponding channels",
    "Show Which channels have the most number of videos, and how many videos do they have",
    "Show What are the top 10 most viewed videos and their respective channels",
    "Show How many comments were made on each video, and what are their corresponding video names",
    "Show Which videos have the highest number of likes, and what are their corresponding channel names",
    "Show what is the total number of likes and dislikes for each video, and what are their corresponding video names",
    "Show what is the total number of views for each channel, and what are their corresponding channel names",
    "Show what are the names of all the channels that have published videos in the year 2022",
    "Show what is the average duration of all videos in each channel, and what are their  corresponding channel names",
    "Show which videos have the highest number of comments, and what are their corresponding channel names"
])
if query_select == "None":
    st.write(" Please select any option")
elif query_select == "Show the names of all the videos and their corresponding channels":
    st.table(execute_radio_option(1))
elif query_select == "Show Which channels have the most number of videos, and how many videos do they have":
    st.table(execute_radio_option(2))
elif query_select == "Show What are the top 10 most viewed videos and their respective channels":
    st.table(execute_radio_option(3))
elif query_select == "Show How many comments were made on each video, and what are their corresponding video names":
    st.table(execute_radio_option(4))
elif query_select == "Show Which videos have the highest number of likes, and what are their corresponding channel names":
    st.table(execute_radio_option(5))
elif query_select == "Show what is the total number of likes and dislikes for each video, and what are their corresponding video names":
    st.table(execute_radio_option(6))
elif query_select == "Show what is the total number of views for each channel, and what are their corresponding channel names":
    st.table(execute_radio_option(7))
elif query_select == "Show what are the names of all the channels that have published videos in the year 2022":
    st.table(execute_radio_option(8))
elif query_select == "Show what is the average duration of all videos in each channel, and what are their  corresponding channel names":
    st.table(execute_radio_option(9))
elif query_select == "Show which videos have the highest number of comments, and what are their corresponding channel names":
    st.table(execute_radio_option(10))
else:
    st.write("nothing")
