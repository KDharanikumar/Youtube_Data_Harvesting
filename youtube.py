# LIBRARY
from googleapiclient.discovery import build
import pymongo
from pymongo import MongoClient
import pandas as pd
import mysql.connector
import streamlit as st

# API KEY CONNECTION
def Api_connect():
  Api_id="AIzaSyCDOgXyTd_j1k3AEWrCaVFvRTeoJA8mvcg"
  api_service_name="youtube"
  api_version="v3"
  youtube=build(api_service_name, api_version, developerKey=Api_id)
  return youtube
youtube=Api_connect()

# GET CHANNEL INFORMATIONS
def get_channel_info(channel_id):
  request=youtube.channels().list(
    part="snippet, ContentDetails, statistics",
    id=channel_id
  )
  response=request.execute()

  for i in response['items']:
    data=dict(Channel_Name=i["snippet"]["title"],
              Channel_Id=i["id"],
              Subscribers=i["statistics"]["subscriberCount"],
              Views=i["statistics"]["viewCount"],
              Total_Videos=i["statistics"]["videoCount"],
              Channel_Description=i["snippet"]["description"],
              Playlist_Id=i["contentDetails"]["relatedPlaylists"]["uploads"])
    return data

# GET VIDEO ID's
def get_videos_ids(channel_id):
  video_ids=[]
  response=youtube.channels().list(id=channel_id,
                                  part="contentDetails").execute()
  Playlist_Id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
  next_page_token=None
  while True:
    response1=youtube.playlistItems().list(
                                          part="snippet",
                                          playlistId=Playlist_Id,maxResults=50,pageToken=next_page_token).execute()
    for i in range(len(response1["items"])):
      video_ids.append(response1["items"][i]["snippet"]["resourceId"]["videoId"])
    next_page_token=response1.get("nextPageToken")

    if next_page_token is None:
      break
  return video_ids

# GET VIDEO INFORMATIONS
def get_video_info(video_ids):
    video_data = []
    for video_id in video_ids:
        request = youtube.videos().list(
            part="snippet, contentDetails, statistics",
            id=video_id
        )
        response = request.execute()
        for item in response["items"]:
            data = dict(
                Channel_Name=item["snippet"]['channelTitle'],
                Channel_Id=item["snippet"]['channelId'],
                Video_Id=item["id"],
                Title=item["snippet"]["title"],
                Tags=item['snippet'].get("tags"),
                Published_At=item["snippet"]["publishedAt"],
                Description=item['snippet'].get("description"),
                Views=item["statistics"].get("viewCount"),
                Likes=item["statistics"].get("likeCount"),
                Thumbnail=item["snippet"]["thumbnails"]["default"]['url'],
                Duration=item["contentDetails"]["duration"],
                Comments=item["statistics"].get("commentCount"),
                Favorite_Count=item["statistics"]["favoriteCount"],
                Definition=item["contentDetails"]["definition"],
                Caption_Status=item["contentDetails"]["caption"]
            )
            video_data.append(data)
    return video_data

# GET COMMENT INFORMATIONS
def get_comment_info(video_ids):
  Comment_data=[]
  try:
    for video_id in video_ids:
        request=youtube.commentThreads().list(
          part="snippet",
          videoId=video_id,
          maxResults=50
          )
        response=request.execute()
        for item in response['items']:
          data=dict(Comment_Id=item['snippet']['topLevelComment']['id'],
                    Video_Ids=item['snippet']['topLevelComment']['snippet']['videoId'],
                    Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                    Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    Comment_Published_At=item['snippet']['topLevelComment']['snippet']['publishedAt'],
                  )
          Comment_data.append(data)
  except:
    pass
  return Comment_data

# GET PLAYLIST DETAILS
def get_playlist_details(channel_id):
  next_page_token=None
  All_data=[]
  while True:
    request=youtube.playlists().list(
            part='snippet,contentDetails',
            channelId=channel_id,
            maxResults=50,
            pageToken=next_page_token
    )
    response=request.execute()

    for item in response['items']:
          data=dict(Playlist_Id=item['id'],
                  Title=item['snippet']['title'],
                  Channel_Id=item['snippet']['channelId'],
                  Channel_Name=item['snippet']['channelTitle'],
                  PublishedAt=item['snippet']['publishedAt'],
                  Video_Count=item['contentDetails']['itemCount'],
                  )
          All_data.append(data)
    next_page_token=response.get('nextPageToken')
    if next_page_token is None:
      break
  return All_data

# CONNECTING MONGODB
client = pymongo.MongoClient("mongodb+srv://dharan:sumithra@dharanikumark.b0rt3.mongodb.net/?retryWrites=true&w=majority&appName=DharanikumarK")
db=client["Youtube_Data"]

def channel_details(channel_id):
  ch_details=get_channel_info(channel_id)
  pl_details=get_playlist_details(channel_id)
  vi_ids=get_videos_ids(channel_id)
  vi_details=get_video_info(vi_ids)
  com_details=get_comment_info(vi_ids)

  collection1=db["channel_details"]
  collection1.insert_one({"channel_information":ch_details,"playlist_information":pl_details,"video_information":vi_details,"comment_information":com_details})

  return "Uploaded Successfully"

# CREATE CHANNEL TABLE
import mysql.connector
import pandas as pd
from pymongo import MongoClient
def channels_table(channel_name_s):
    # Connect to MySQL database
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="root",
        database="youtube_data",
        port="3306"
    )
    mycursor = mydb.cursor(buffered=True)

    # Create the channels table if it doesn't exist
    create_query = '''CREATE TABLE IF NOT EXISTS channels (
                          Channel_Name VARCHAR(100),
                          Channel_Id VARCHAR(80) PRIMARY KEY,
                          Subscribers BIGINT,
                          Views BIGINT,
                          Total_Videos INT,
                          Channel_Description TEXT,
                          Playlist_Id VARCHAR(80))'''

    mycursor.execute(create_query)
    mydb.commit()

    single_channel_details=[]
    db = client["Youtube_Data"]
    collection1 = db["channel_details"]
    for ch_data in collection1.find({"channel_information.Channel_Name": channel_name_s }, {"_id": 0}):
      single_channel_details.append(ch_data["channel_information"])
    df_single_channel_detail=pd.DataFrame(single_channel_details)

    # Iterate over the rows in the DataFrame and insert into MySQL
    for index, row in df_single_channel_detail.iterrows():
        insert_query = '''INSERT INTO channels (
                                                Channel_Name,
                                                Channel_Id,
                                                Subscribers,
                                                Views,
                                                Total_Videos,
                                                Channel_Description,
                                                Playlist_Id
                                                )
                                                VALUES (%s, %s, %s, %s, %s, %s, %s)'''
        values = (
            row["Channel_Name"],
            row["Channel_Id"],
            row["Subscribers"],
            row["Views"],
            row["Total_Videos"],
            row["Channel_Description"],
            row["Playlist_Id"]
        )
        try:
          mycursor.execute(insert_query, values)
          mydb.commit()
        except:
            news=f"Your Provided Channel Name {channel_name_s} is Already Exists"
            return news

# CREATE PLAYLIST TABLE
import mysql.connector
import pandas as pd
from pymongo import MongoClient
def playlist_table(channel_name_s):
    # Connect to MySQL database
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="root",
        database="youtube_data",
        port="3306"
    )
    mycursor = mydb.cursor(buffered=True)
# Create the playlists table if it doesn't exist
    create_query = '''CREATE TABLE IF NOT EXISTS playlists (
                                                            Playlist_Id VARCHAR(100) PRIMARY KEY,
                                                            Title VARCHAR(100),
                                                            Channel_Id VARCHAR(100),
                                                            Channel_Name VARCHAR(100),
                                                            Video_Count INT
                                                        )'''
    mycursor.execute(create_query)
    mydb.commit()

    single_playlist_details=[]
    db = client["Youtube_Data"]
    collection1 = db["channel_details"]
    for ch_data in collection1.find({"channel_information.Channel_Name": channel_name_s }, {"_id": 0}):
      single_playlist_details.append(ch_data["playlist_information"])
    df_single_playlist_detail=pd.DataFrame(single_playlist_details[0])

# Insert data into MySQL
    for index, row in df_single_playlist_detail.iterrows():
        try:
            # Prepare the query and the data to insert
            insert_query = '''INSERT INTO playlists(
                                                Playlist_Id,
                                                Title,
                                                Channel_Id,
                                                Channel_Name,
                                                Video_Count
                                            ) VALUES (%s, %s, %s, %s, %s)'''
            values = (
                row['Playlist_Id'],
                row['Title'],
                row['Channel_Id'],
                row['Channel_Name'],
                row['Video_Count']
            )
            # Execute the insert query
            mycursor.execute(insert_query, values)
            mydb.commit()
        except:
            print("Playlist values are already inserted")

# CREATE VIDEOS TABLE
import mysql.connector
import pandas as pd
from pymongo import MongoClient
def videos_tables(channel_name_s):
    # Connect to MySQL database
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="root",
        database="youtube_data",
        port="3306"
    )
    mycursor = mydb.cursor(buffered=True)

# Create the videos table if it doesn't exist
    create_query = '''CREATE TABLE IF NOT EXISTS videos (
        Channel_Name VARCHAR(100),
        Channel_Id VARCHAR(100),
        Video_Id VARCHAR(50) PRIMARY KEY,
        Title VARCHAR(150),
        Thumbnail VARCHAR(200),
        Views BIGINT,
        Likes BIGINT,
        Comments INT,
        Favorite_Count INT,
        Definition VARCHAR(50),
        Caption_Status VARCHAR(50)
    )'''
    mycursor.execute(create_query)
    mydb.commit()

    single_videos_details=[]
    db = client["Youtube_Data"]
    collection1 = db["channel_details"]
    for ch_data in collection1.find({"channel_information.Channel_Name": channel_name_s}, {"_id": 0}):
        single_videos_details.append(ch_data["video_information"])
    df_single_playlist_detail=pd.DataFrame(single_videos_details[0])

    # Insert data into MySQL
    for index, row in df_single_playlist_detail.iterrows():
            insert_query = '''INSERT INTO videos(
                Channel_Name,
                Channel_Id,
                Video_Id,
                Title,
                Thumbnail,
                Views,
                Likes,
                Comments,
                Favorite_Count,
                Definition,
                Caption_Status
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''

            # Prepare the values tuple
            values = (
                row['Channel_Name'],
                row['Channel_Id'],
                row['Video_Id'],
                row['Title'],
                row['Thumbnail'],
                row['Views'],
                row['Likes'],
                row['Comments'],
                row['Favorite_Count'],
                row['Definition'],
                row['Caption_Status']
            )

            # Execute the insert query
            mycursor.execute(insert_query, values)
            mydb.commit()

# CREATE COMMENT TABLE
import mysql.connector
import pandas as pd
from pymongo import MongoClient
def comment_table(channel_name_s):
    # Connect to MySQL database
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="root",
        database="youtube_data",
        port="3306"
    )
    mycursor = mydb.cursor(buffered=True)

    # Create the comments table if it doesn't exist
    create_query = '''CREATE TABLE IF NOT EXISTS comments (
                                                              Comment_Id varchar(100) PRIMARY KEY,
                                                              Video_Ids varchar(50),
                                                              Comment_Text text,
                                                              Comment_Author varchar(150)
                                                              -- Comment_Published_At timestamp
                                                          )'''
    mycursor.execute(create_query)
    mydb.commit()

    single_comments_details=[]
    db = client["Youtube_Data"]
    collection1 = db["channel_details"]
    for ch_data in collection1.find({"channel_information.Channel_Name": channel_name_s }, {"_id": 0}):
        single_comments_details.append(ch_data["comment_information"])
    df_single_comments_details=pd.DataFrame(single_comments_details[0])

    # Insert data into MySQL
    for index, row in df_single_comments_details.iterrows():
                # Prepare the query and the data to insert
                insert_query = '''INSERT INTO comments(
                                                        Comment_Id,
                                                        Video_Ids,
                                                        Comment_Text,
                                                        Comment_Author
                                                        -- Comment_Published_At
                                                    ) VALUES (%s, %s, %s, %s)'''
                values = (
                    row['Comment_Id'],
                    row['Video_Ids'],
                    row['Comment_Text'],
                    row['Comment_Author']
                    # row['Comment_Published_At']  # You can add this later once the field is available
                )
# Execute the insert query
                mycursor.execute(insert_query, values)
                mydb.commit()

# CREATE ALL FUNTION TABLE IN ONE PLACE
def tables(single_channel):
    news=channels_table(single_channel)

    if news:
      return news

    else:
      playlist_table(single_channel)
      videos_tables(single_channel)
      comment_table(single_channel)
      return "Table Created Successfully"

def show_channels_table():
  ch_list = []
  db = client["Youtube_Data"]
  collection1 = db["channel_details"]
  for ch_data in collection1.find({}, {"_id": 0, "channel_information": 1}):
      # for i in range(len(ch_data["channel_information"])):
        ch_list.append(ch_data["channel_information"])
  df = st.dataframe(ch_list)
  return df

def show_playlists_table():
  pl_list = []
  db = client["Youtube_Data"]
  collection1 = db["channel_details"]
  for pl_data in collection1.find({}, {"_id": 0, "playlist_information": 1}):
      for i in range(len(pl_data["playlist_information"])):
        pl_list.append(pl_data["playlist_information"][i])
  df1 = st.dataframe(pl_list)
  return df1

def show_videos_table():
  vi_list = []
  db = client["Youtube_Data"]
  collection1 = db["channel_details"]
  for vi_data in collection1.find({}, {"_id": 0, "video_information": 1}):
      for i in range(len(vi_data["video_information"])):
        vi_list.append(vi_data["video_information"][i])
  df2 = st.dataframe(vi_list)
  return df2

def show_comments_table():
  com_list = []
  db = client["Youtube_Data"]
  collection1 = db["channel_details"]
  for com_data in collection1.find({}, {"_id": 0, "comment_information": 1}):
      for i in range(len(com_data["comment_information"])):
        com_list.append(com_data["comment_information"][i])
  df3 = st.dataframe(com_list)
  return df3

# STREAMLIT
with st.sidebar:
  st.title(":red[YOUTUBE DATA HAVERSTING AND WAREHOUSING]")
  st.header("Skill Take Away")
  st.caption("Python Scripting")
  st.caption("Data Collection")
  st.caption("MongoDB")
  st.caption("API Integration")
  st.caption("Data Stored Sql and MongoDB")

channel_id=st.text_input("Enter the Channel ID")

if st.button("collect and store data"):
    ch_ids=[]
    db=client["Youtube_Data"]
    collection1=db["channel_details"]
    for ch_data in collection1.find({},{"_id":0,"channel_information":1}):
        ch_ids.append(ch_data["channel_information"]["Channel_Id"])

    if channel_id in ch_ids:
      st.success("Already have channel ID")

    else:
      insert=channel_details(channel_id)
      st.success(insert)

all_channels=[]
db = client["Youtube_Data"]
collection1 = db["channel_details"]
for ch_data in collection1.find({}, {"_id": 0, "channel_information": 1}):
    all_channels.append(ch_data["channel_information"]["Channel_Name"])

unique_Channel=st.selectbox("Select the Channel",all_channels )

if st.button("Migrate to Sql"):
  Table=tables(unique_Channel)
  st.success(Table)

show_table=st.radio("SELECT THE TABLE FOR VIEW",("CHANNELS", "PLAYLIST", "VIDEOS", "COMMENTS"))

if show_table=="CHANNELS":
  show_channels_table()

elif show_table=="PLAYLIST":
  show_playlists_table()

elif show_table=="VIDEOS":
  show_videos_table()

elif show_table=="COMMENTS":
    show_comments_table()

# SQL CONNECTION
# Connect to MySQL database
mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="root",
        database="youtube_data",
        port="3306"
    )
mycursor = mydb.cursor(buffered=True)

question=st.selectbox("Select Your Question",("1. All the Videos and the Channel Name",
                                           "2. Channels with Most Number of Videos",
                                           "3. 10 Most Viewed Videos",
                                           "4. Comment in Each Videos",
                                           "5. Videos with Highest Likes",
                                           "6. Likes of All Videos",
                                           "7. Views of Each Cahnnels",
                                           "8. Videos with Highest Number of Comments"
                                           ))

if question=="1. All the Videos and the Channel Name":
  query1='''select title as video, channel_name as channelname from videos'''
  mycursor.execute(query1)
  mydb.commit()
  t1=mycursor.fetchall()
  df=pd.DataFrame(t1,columns=["video title","channel_name"])
  st.write(df)

elif question=="2. Channels with Most Number of Videos":
  query2='''select channel_name as channelname, total_videos as no_videos from channels order by total_videos desc'''
  mycursor.execute(query2)
  mydb.commit()
  t2=mycursor.fetchall()
  df2=pd.DataFrame(t2,columns=["channel name","No of videos"])
  st.write(df2)

elif question=="3. 10 Most Viewed Videos":
  query3='''select views as views, channel_name as channelname, title as videotitle from videos
            where views is not null order by views desc limit 10'''
  mycursor.execute(query3)
  mydb.commit()
  t3=mycursor.fetchall()
  df3=pd.DataFrame(t3,columns=["views","channel name","video title"])
  st.write(df3)

elif question=="4. Comment in Each Videos":
  query4='''select comments as no_comments, title as videotitle from videos where comments is not null'''
  mycursor.execute(query4)
  mydb.commit()
  t4=mycursor.fetchall()
  df4=pd.DataFrame(t4,columns=["no of comments","videotitle"])
  st.write(df4)

elif question=="5. Videos with Highest Likes":
  query5='''select title as videotitle, channel_name as channelname,likes as likecount
            from videos where likes is not null order by likes desc'''
  mycursor.execute(query5)
  mydb.commit()
  t5=mycursor.fetchall()
  df5=pd.DataFrame(t5,columns=["videotitle","channelname","likecount"])
  st.write(df5)

elif question=="6. Likes of All Videos":
  query6='''select likes as likecount,title as videotitle from videos'''
  mycursor.execute(query6)
  mydb.commit()
  t6=mycursor.fetchall()
  df6=pd.DataFrame(t6,columns=["likecount","videotitle"])
  st.write(df6)

if question=="7. Views of Each Cahnnels":
  query7='''select channel_name as channelname,views as totalviews from channels'''
  mycursor.execute(query7)
  mydb.commit()
  t7=mycursor.fetchall()
  df7=pd.DataFrame(t7,columns=["channel name","total views"])
  st.write(df7)

elif question=="8. Videos with Highest Number of Comments":
  query8='''select title as videotitle, channel_name as channelname, comments as comments from videos where comments is not null order by comments desc'''
  mycursor.execute(query8)
  mydb.commit()
  t8=mycursor.fetchall()
  df8=pd.DataFrame(t8,columns=["video title","channel name", "comments"])
  st.write(df8)