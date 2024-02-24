from googleapiclient.discovery import build
import pymongo
import pymysql
import pandas as pd
import streamlit as st

#API key connection
api_service_name = "youtube"
api_version = "v3"
DEVELOPER_KEY = "AIzaSyCf8i_Xbc9-Pdj2Me12MJHL9Ww6aJqSzjE"

youtube =build(api_service_name, api_version, developerKey = DEVELOPER_KEY)

#Get channel data 
def get_channel_details(channel_id):
    request = youtube.channels().list(
        part = "snippet,contentDetails, statistics",
        id = channel_id
    )
    response = request. execute()
    for i in response['items']:
        data = dict(channel_id             = i['id'], 
                        channel_name           = i['snippet']['title'], 
                        channel_publish        = str((i['snippet']['publishedAt']).split("T")[0]),  
                        channel_description    = i['snippet']['description'], 
                        channel_view           = i['statistics']['viewCount'], 
                        channel_subscriber     = i['statistics']['subscriberCount'], 
                        channel_video          = i['statistics']['videoCount'],
                        Playlist_Id            = i['contentDetails']['relatedPlaylists']['uploads'])
    return data

#Get Videos id's 
def get_video_ids(channel_id):
    video_ids = []
    # get Uploads playlist id
    res = youtube.channels().list(id=channel_id, 
                                  part='contentDetails').execute()
    playlist_id = res['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None
    
    while True:
        res = youtube.playlistItems().list(playlistId=playlist_id, 
                                           part='snippet', 
                                           maxResults=50,
                                           pageToken=next_page_token).execute()
        
        for i in range(len(res['items'])):
            video_ids.append(res['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = res.get('nextPageToken')
        
        if next_page_token is None:
            break
    return video_ids

#Get videos data
def get_video_info(videos_ids):
    videos_data=[]
    for videos_id in videos_ids:
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=videos_id
        )
        response=request.execute()
        def time_str_to_seconds(t):
                def time_duration(t):
                    a = pd.Timedelta(t)
                    b = str(a).split()[-1]
                    return b
                time_str = time_duration(t)
                    
                hours, minutes, seconds = map(int, time_str.split(':'))
                total_seconds = (hours * 3600) + (minutes * 60) + seconds
                    
                return total_seconds

        for item in response["items"]:
            data=dict(Channel_Name=item['snippet']['channelTitle'],
                    Channel_Id=item['snippet']['channelId'],
                    Video_Id=item['id'],
                    Title=item['snippet']['title'],
                    Tags=",".join(item['snippet'].get('tags',['no tags'])),
                    Thumbnails=item['snippet']['thumbnails']['default']['url'],
                    Description=item['snippet'].get('description'),
                    Published_Date=str((item['snippet']['publishedAt']).split("T")[0]),
                    Duration=str(time_str_to_seconds(item['contentDetails']['duration'])),
                    Views=item['statistics'].get('viewCount'),
                    Likes=item['statistics'].get('likeCount'),
                    Comments=item['statistics'].get('commentCount'),
                    Favorite_Count=item['statistics']['favoriteCount'],
                    Definition=item['contentDetails']['definition'],
                    Caption=item['contentDetails']['caption']
                    )
            videos_data.append(data)
    return videos_data


#Get comments info
def get_comment_info(video_ids):
    Comments_data=[]
    try:
        for video_id in video_ids:
            request=youtube.commentThreads().list(
                part='snippet',
                videoId=video_id,
                maxResults=50
            )
            response=request.execute()

            for item in response['items']:
                data=dict(Comment_Id=item['snippet']['topLevelComment']['id'],
                        Video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                        Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                        Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        Comment_PublishedAt=str((item['snippet']['topLevelComment']['snippet']['publishedAt']).split("T")[0])
                )
                Comments_data.append(data)
    except:
        pass
    return Comments_data

#Get Playlist 
def get_playlist_info(channel_id):

        next_page_token=None
        Playlist_data=[]
        while True:
                request = youtube.playlists().list(
                        part="snippet,contentDetails",
                        channelId=channel_id,
                        maxResults=50,
                        pageToken=next_page_token
                        )
                response = request.execute()
                
                for item in response['items']:
                        data=dict(Playlist_Id=item['id'],
                                Title=item['snippet']['title'],
                                channel_Id=item['snippet']['channelId'],
                                Channel_Name=item['snippet']['channelTitle'],
                                PublishedAt=str((item['snippet']['publishedAt']).split("T")[0]),
                                Video_count=item['contentDetails']['itemCount']
                                )
                        Playlist_data.append(data)
                next_page_token=response.get('nextPageToken')
                if next_page_token is None:
                        break
        return Playlist_data

#CONNECT to mongodb

client=pymongo.MongoClient("mongodb://localhost:27017")
db=client["youtube_data"]

# GETTING ALL CHANNEL DETAILS
def channel_details(channel_id):
    ch_details=get_channel_details(channel_id)
    pl_details=get_playlist_info(channel_id)
    vi_ids=get_video_ids(channel_id)
    vi_details=get_video_info(vi_ids)
    com_details=get_comment_info(vi_ids)

    coll1=db["channel_details"]
    coll1.insert_one({"channel_information":ch_details,
                      "playlist_information":pl_details,
                      "video_information":vi_details,
                      "comment_information":com_details})
    return "upload completed successfully"

#Table creation for datas

# Channel Table
def channels_table():

    mydb=pymysql.connect(host="127.0.0.1",
                        user="root",
                        password="Mgobi@12",
                        database="youtube_data")
    cursor = mydb.cursor()

    drop_query='''drop table if exists channels'''
    cursor.execute(drop_query)
    mydb.commit()

    create_query='''create table if not exists Channels(channel_id varchar(250) primary key,
                                                        channel_name varchar(250),
                                                        channel_publish varchar(250),
                                                        channel_description text,
                                                        channel_view bigint,
                                                        channel_subscriber bigint,
                                                        channel_video int,
                                                        Playlist_Id varchar(250))'''
    cursor.execute(create_query)
    mydb.commit()


    ch_list=[]
    db=client["youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df=pd.DataFrame(ch_list)

    for index,row in df.iterrows():
        insert_query='''insert into channels(channel_id,
                                            channel_name,
                                            channel_publish,
                                            channel_description,
                                            channel_view,
                                            channel_subscriber,
                                            channel_video,
                                            Playlist_Id)
                                            
                                            values(%s,%s,%s,%s,%s,%s,%s,%s)'''
        values=(row['channel_id'],
                row['channel_name'],
                row['channel_publish'],
                row['channel_description'],
                row['channel_view'],
                row['channel_subscriber'],
                row['channel_video'],
                row['Playlist_Id'])
        
        cursor.execute(insert_query,values)
        mydb.commit()


# Playlist Table
def playlists_table():
    mydb=pymysql.connect(host="127.0.0.1",
                        user="root",
                        password="Mgobi@12",
                        database="youtube_data")
    cursor = mydb.cursor()

    drop_query='''drop table if exists playlists'''
    cursor.execute(drop_query)
    mydb.commit()


    create_query1='''create table if not exists playlists(Playlist_Id varchar(250) primary key,
                                                        Title varchar(250),
                                                        channel_Id varchar(250),
                                                        Channel_Name varchar(250),
                                                        PublishedAt varchar(250),
                                                        Video_count int
                                                        )'''


    cursor.execute(create_query1)
    mydb.commit()

    pl_list=[]
    db=client["youtube_data"]
    coll1=db["channel_details"]
    for pl_data in coll1.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(pl_data["playlist_information"])):
            pl_list.append(pl_data["playlist_information"][i])
    df1=pd.DataFrame(pl_list)

    for index,row in df1.iterrows():
        insert_query1='''insert into playlists(Playlist_Id,
                                            Title ,
                                            channel_Id,
                                            Channel_Name,
                                            PublishedAt,
                                            Video_count
                                            )
                                            
                                            values(%s,%s,%s,%s,%s,%s)'''
        values1=(row['Playlist_Id'],
                row['Title'],
                row['channel_Id'],
                row['Channel_Name'],
                row['PublishedAt'],
                row['Video_count']
                )
        

        cursor.execute(insert_query1,values1)
        mydb.commit()

# Videos Table
def videos_table():
    mydb=pymysql.connect(host="127.0.0.1",
                    user="root",
                    password="Mgobi@12",
                    database="youtube_data")
    cursor = mydb.cursor()

    drop_query = "drop table if exists videos"
    cursor.execute(drop_query)
    mydb.commit()


    create_query ='''create table if not exists videos(
                                                        Channel_Name varchar(250),
                                                        Channel_Id varchar(250),
                                                        Video_Id varchar(250) primary key,
                                                        Title varchar(250),
                                                        Tags text,
                                                        Thumbnails varchar(250),
                                                        Description text,
                                                        Published_Date date,
                                                        Duration int,
                                                        Views int,
                                                        Likes int,
                                                        Comments int,
                                                        Favorite_Count int,
                                                        Definition varchar(50),
                                                        Caption varchar(50)
                                                        )'''
    cursor.execute(create_query)             
    mydb.commit()

    vi_list = []
    db = client["youtube_data"]
    coll1 = db["channel_details"]
    for vi_data in coll1.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    df2 = pd.DataFrame(vi_list) 

    for index,row in df2.iterrows():
        insert_query='''INSERT INTO videos(Channel_Name,
                                            Channel_Id,
                                            Video_Id,
                                            Title,
                                            Tags,
                                            Thumbnails,
                                            Description,
                                            Published_Date,
                                            Duration,
                                            Views,
                                            Likes,
                                            Comments,
                                            Favorite_Count,
                                            Definition,
                                            Caption
                                            )
                                            
                                            values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        values=(row['Channel_Name'],
                row['Channel_Id'],
                row['Video_Id'],
                row['Title'],
                row['Tags'],
                row['Thumbnails'],
                row['Description'],
                row['Published_Date'],
                row['Duration'],
                row['Views'],
                row['Likes'],
                row['Comments'],
                row['Favorite_Count'],
                row['Definition'],
                row['Caption'],
                )
        

        cursor.execute(insert_query,values)
        mydb.commit()


# Comments table

def comments_table():
    mydb=pymysql.connect(host="127.0.0.1",
                    user="root",
                    password="Mgobi@12",
                    database="youtube_data")
    cursor = mydb.cursor()

    drop_query = "drop table if exists comments"
    cursor.execute(drop_query)
    mydb.commit()

    create_query ='''create table if not exists comments(
                                                        Comment_Id varchar(250) primary key,
                                                        Video_Id varchar(250),
                                                        Comment_Text text,
                                                        Comment_Author varchar(250),
                                                        Comment_PublishedAt varchar(100) 
                                                        )'''
    cursor.execute(create_query)             
    mydb.commit()

    com_list = []
    db = client["youtube_data"]
    coll1 = db["channel_details"]
    for com_data in coll1.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
    con=com_list
    df3 = pd.DataFrame(com_list)

    for index,row in df3.iterrows():
        insert_query='''INSERT INTO comments(
                                            Comment_Id,
                                            Video_Id,
                                            Comment_Text,
                                            Comment_Author,
                                            Comment_PublishedAt 
                                            )
                                            
                                            values (%s,%s,%s,%s,%s)'''
        values=(row['Comment_Id'],
                row['Video_Id'],
                row['Comment_Text'],
                row['Comment_Author'],
                row['Comment_PublishedAt']
                )
        

        cursor.execute(insert_query,values)
        mydb.commit()

#ALL TABLES FUNCTION
def tables():
    channels_table()
    playlists_table()
    videos_table()
    comments_table()

    return " Tables created sucessfully"

# SHOW TABLES
def show_channels_table():
    ch_list=[]
    db=client["youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df=st.dataframe(ch_list)

    return df

def show_playlists_table():
    pl_list=[]
    db=client["youtube_data"]
    coll1=db["channel_details"]
    for pl_data in coll1.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(pl_data["playlist_information"])):
            pl_list.append(pl_data["playlist_information"][i])
    df1=st.dataframe(pl_list)

    return df1

def show_videos_table():
    vi_list = []
    db = client["youtube_data"]
    coll1 = db["channel_details"]
    for vi_data in coll1.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    df2 = st.dataframe(vi_list) 

    return df2

def show_comments_table():
    com_list = []
    db = client["youtube_data"]
    coll1 = db["channel_details"]
    for com_data in coll1.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
    con=com_list
    df3 = st.dataframe(com_list)

    return df3

# Streamlit COMMENTS

with st.sidebar:
    st.title(":red[YOUTUBE DATA HAVERSTING AND WAREHOUSING]")
    st.header("Unveiling YouTube Data: A Simple Approach")
    st.caption("Explore YouTube data effortlessly using Python scripts")
    st.caption("Integrate APIs seamlessly to access valuable data")
    st.caption("Manage and organize data efficiently with MongoDB and MySQL")
    st.caption("Simplify data harvesting and warehousing for insightful analysis")
    st.caption("Embark on a journey of discovery and unlock the potential of your YouTube data")

channel_id=st.text_input("ENTER THE CHANNEL ID")

if st.button("Collect and store data"):
    ch_ids=[]
    db=client["youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_ids.append(ch_data["channel_information"]["channel_id"])
    
    if channel_id in ch_ids:
        st.success("Channel Details of the given channel id already exists")

    else:
        insert=channel_details(channel_id)
        st.success(insert)


if st.button("Migrate to SQL"):
    Table=tables()
    st.success(Table)

show_table=st.radio("SELECT THE TABLES FOR VIEW",("CHANNELS","PLAYLISTS","VIDEOS","COMMENTS"))

if show_table=="CHANNELS":
    show_channels_table()

elif show_table=="PLAYLISTS":
    show_playlists_table()

elif show_table=="VIDEOS":
    show_videos_table()

elif show_table=="COMMENTS":
    show_comments_table()

# SQL Connection 
mydb=pymysql.connect(host="127.0.0.1",
                user="root",
                password="Mgobi@12",
                database="youtube_data")
cursor = mydb.cursor()

question=st.selectbox("Please Select Your Question",
                      ('1. What are the names of all the videos and their corresponding channels?',
                        '2. Which channels have the most number of videos, and how many videos dothey have?',
                        '3. What are the top 10 most viewed videos and their respective channels?',
                        '4. How many comments were made on each video, and what are their corresponding video names?',
                        '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
                        '6. What is the total number of likes for each video and what are their corresponding video names?',
                        '7. What is the total number of views for each channel, and what are their corresponding channel names?',
                        '8. What are the names of all the channels that have published videos in the year 2022?',
                        '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
                        '10. Which videos have the highest number of comments, and what are their corresponding channel names?'))

# QUERYS FOR QUESTIONS

if question=='1. What are the names of all the videos and their corresponding channels?':

    query1='''select title as videos,channel_name as channelname from videos'''
    cursor.execute(query1)
    mydb.commit()
    t1=cursor.fetchall()
    df=pd.DataFrame(t1,columns=["videos title","channel name"])
    st.write(df)

elif question=='2. Which channels have the most number of videos, and how many videos dothey have?':

    query2='''select channel_name as channelname,channel_video as no_videos from channels order by channel_video desc'''
    cursor.execute(query2)
    mydb.commit()
    t2=cursor.fetchall()
    df2=pd.DataFrame(t2,columns=["channel name","No of videos"])
    st.write(df2)

elif question=='3. What are the top 10 most viewed videos and their respective channels?':

    query3='''select views as views,channel_name as channelname,title as videotitle from videos where views is not null order by views desc limit 10'''
    cursor.execute(query3)
    mydb.commit()
    t3=cursor.fetchall()
    df3=pd.DataFrame(t3,columns=["Views","Channel Name","Video Title"])
    st.write(df3)

elif question=='4. How many comments were made on each video, and what are their corresponding video names?':

    query4='''select comments as no_comments,title as videotitle from videos where comments is not null'''
    cursor.execute(query4)
    mydb.commit()
    t4=cursor.fetchall()
    df4=pd.DataFrame(t4,columns=["No Comments","Video title"])
    st.write(df4)

elif question=='5. Which videos have the highest number of likes, and what are their corresponding channel names?':

    query5='''select likes as likes,title as videostitle,channel_name as channelname from videos where likes is not null order by likes desc'''
    cursor.execute(query5)
    mydb.commit()
    t5=cursor.fetchall()
    df5=pd.DataFrame(t5,columns=["Likes","Video title","Channel Name"])
    st.write(df5)

elif question=='6. What is the total number of likes for each video and what are their corresponding video names?':

    query6='''select likes as likes,title as videostitle from videos where likes is not null'''
    cursor.execute(query6)
    mydb.commit()
    t6=cursor.fetchall()
    df6=pd.DataFrame(t6,columns=["Likes","Video Title"])
    st.write(df6)

elif question=='7. What is the total number of views for each channel, and what are their corresponding channel names?':

    query7='''select channel_view as views,channel_name as channelname from channels where channel_view is not null order by views desc'''
    cursor.execute(query7)
    mydb.commit()
    t7=cursor.fetchall()
    df7=pd.DataFrame(t7,columns=["Views","Channel Name"])
    st.write(df7)

elif question=='8. What are the names of all the channels that have published videos in the year 2022?':

    query8='''select title as videotitle,Published_Date as videopublished,channel_name as channelname from videos where Published_Date like '2022%' order by Published_Date'''
    cursor.execute(query8)
    mydb.commit()
    t8=cursor.fetchall()
    df8=pd.DataFrame(t8,columns=["Video Title","Video Published","Channel Name"])
    st.write(df8)

elif question=='9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':

    query9='''select channel_name as channelname,AVG(Duration) as averageduration from videos group by channel_name'''
    cursor.execute(query9)
    mydb.commit()
    t9=cursor.fetchall()
    df9=pd.DataFrame(t9,columns=["Channel Name","Average Dutation(SEC)"])
    st.write(df9)

elif question=='10. Which videos have the highest number of comments, and what are their corresponding channel names?':

    query10='''select title as videotitle,channel_name as channelname,comments as comments from videos where comments is not null order by comments desc'''
    cursor.execute(query10)
    mydb.commit()
    t10=cursor.fetchall()
    df10=pd.DataFrame(t10,columns=["Video Title","Channel Name","Comments"])
    st.write(df10)