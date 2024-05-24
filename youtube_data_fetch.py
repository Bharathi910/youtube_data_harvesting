import streamlit as st
import pandas as pd
from googleapiclient.discovery import build
import pymysql
import re

# Function to connect to YouTube API
def youtube_api():
    # Add your API key here
    api_key = 'API key'
    youtube = build('youtube', 'v3', developerKey=api_key)
    return youtube

youtube = youtube_api()

# MySQL connection details
# Add your username,password,host
username = 'username'
password = 'password'
host = 'host'
database_name = 'database_name'

# Connect to MySQL server and create the database if it doesn't exist
connection = pymysql.connect(host=host, user=username, password=password)
cursor = connection.cursor()
cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database_name}")
connection.select_db(database_name)

# Define table creation queries 
channel_info_table_query = """
CREATE TABLE IF NOT EXISTS channel_info (
    Channel_Id VARCHAR(255) PRIMARY KEY,
    Channel_Name VARCHAR(255),
    Subscribers INT,
    Views INT,
    Total_Videos INT,
    Channel_Description LONGTEXT,
    Playlist_Id VARCHAR(255)
)
"""

video_info_table_query = """
CREATE TABLE IF NOT EXISTS video_info (
    Video_Id VARCHAR(255) PRIMARY KEY,
    Channel_Id VARCHAR(255),
    Title VARCHAR(255),
    Tags TEXT,
    Thumbnail VARCHAR(255),
    Description LONGTEXT,
    Published_Date VARCHAR(255),
    Duration INT,
    Views INT,
    Likes INT,
    Comments INt,
    Favorite_Count INT,
    Definition VARCHAR(255),
    Caption_Status VARCHAR(255),
    FOREIGN KEY (Channel_Id) REFERENCES channel_info(Channel_Id)
)
"""

comment_info_table_query = """
CREATE TABLE IF NOT EXISTS comment_info (
    Comment_Id VARCHAR(255) PRIMARY KEY,
    Video_Id VARCHAR(255),
    Comment_Text LONGTEXT,
    Comment_Author VARCHAR(255),
    Comment_Published VARCHAR(255),
    FOREIGN KEY (Video_Id) REFERENCES video_info(Video_Id)
)
"""

# Execute table creation queries
cursor.execute(channel_info_table_query)
cursor.execute(video_info_table_query)
cursor.execute(comment_info_table_query)
connection.commit()

# Function to get channel information
def get_channel_info(channel_id):
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id
    )
    response = request.execute()

    for i in response['items']:
        data = dict(Channel_Id=i["id"],
                    Channel_Name=i["snippet"]["title"],
                    Subscribers=i['statistics']['subscriberCount'],
                    Views=i["statistics"]["viewCount"],
                    Total_Videos=i["statistics"]["videoCount"],
                    Channel_Description=i["snippet"]["description"],
                    Playlist_Id=i["contentDetails"]["relatedPlaylists"]["uploads"])
    return data

# Function to get video IDs
def get_videos_ids(channel_id):
    video_ids = []
    response = youtube.channels().list(id=channel_id,
                                       part='contentDetails').execute()
    Playlist_Id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token = None

    while True:
        response1 = youtube.playlistItems().list(
            part='snippet',
            playlistId=Playlist_Id,
            maxResults=50,  # Change this number as needed
            pageToken=next_page_token).execute()
        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = response1.get('nextPageToken')

        if next_page_token is None or len(video_ids) >= 2:
            break
    return video_ids[:2]

# Function to parse ISO 8601 duration to total seconds
def parse_duration(duration):
    pattern = re.compile(
        r'P(?:(?P<years>\d+)Y)?(?:(?P<months>\d+)M)?(?:(?P<weeks>\d+)W)?(?:(?P<days>\d+)D)?'
        r'(?:T(?:(?P<hours>\d+)H)?(?:(?P<minutes>\d+)M)?(?:(?P<seconds>\d+)S)?)?'
    )
    match = pattern.match(duration)
    if not match:
        return 0
    time_dict = match.groupdict()
    time_params = {name: int(param) if param else 0 for name, param in time_dict.items()}
    total_seconds = (
        time_params['years'] * 31536000 +  # 365 * 24 * 60 * 60
        time_params['months'] * 2592000 +  # 30 * 24 * 60 * 60
        time_params['weeks'] * 604800 +    # 7 * 24 * 60 * 60
        time_params['days'] * 86400 +      # 24 * 60 * 60
        time_params['hours'] * 3600 +      # 60 * 60
        time_params['minutes'] * 60 +
        time_params['seconds']
    )
    return total_seconds

# Function to get video information
def get_video_info(video_ids):
    video_data = []
    for video_id in video_ids:
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=video_id
        )
        response = request.execute()

        for item in response["items"]:
            data = dict(Video_Id=item['id'],
                        Channel_Id=item['snippet']['channelId'],
                        Title=item['snippet']['title'],
                        Tags=str(item['snippet'].get('tags')),
                        Thumbnail=item['snippet']['thumbnails']['default']['url'],
                        Description=item['snippet'].get('description'),
                        Published_Date=item['snippet']['publishedAt'],
                        Duration=parse_duration(item['contentDetails']['duration']),
                        Views=item['statistics'].get('viewCount'),
                        Likes=item['statistics'].get('likeCount'),
                        Comments=item['statistics'].get('commentCount'),
                        Favorite_Count=item['statistics']['favoriteCount'],
                        Definition=item['contentDetails']['definition'],
                        Caption_Status=item['contentDetails']['caption']
                        )
            video_data.append(data)
    return video_data

# Function to get comment information
def get_comment_info(video_ids):
    Comment_data = []
    try:
        for video_id in video_ids:
            request = youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=2  # Change this number as needed
            )
            response = request.execute()

            for item in response['items']:
                data = dict(Comment_Id=item['snippet']['topLevelComment']['id'],
                            Video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                            Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                            Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            Comment_Published=item['snippet']['topLevelComment']['snippet']['publishedAt'])

                Comment_data.append(data)

    except Exception as e:
        st.error(f"An error occurred: {e}")
    return Comment_data

# Function to save channel and comment data to MySQL
def save_to_mysql(table_name, data):
    conn = pymysql.connect(host=host, user=username, password=password, database=database_name)
    cursor = conn.cursor()
    for row in data:
        placeholders = ', '.join(['%s'] * len(row))
        columns = ', '.join(row.keys())
        sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE "
        update_clause = ', '.join([f"{col}=VALUES({col})" for col in row.keys()])
        sql += update_clause
        cursor.execute(sql, list(row.values()))
    conn.commit()
    cursor.close()
    conn.close()

# Function to save video data to MySQL
def save_video_data_to_mysql(video_data):
    insert_query = """
    INSERT INTO video_info (Video_Id, Channel_Id, Title, Tags, Thumbnail, Description, Published_Date, Duration, Views, Likes, Comments, Favorite_Count, Definition, Caption_Status)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        Channel_Id=VALUES(Channel_Id),
        Title=VALUES(Title),
        Tags=VALUES(Tags),
        Thumbnail=VALUES(Thumbnail),
        Description=VALUES(Description),
        Published_Date=VALUES(Published_Date),
        Duration=VALUES(Duration),
        Views=VALUES(Views),
        Likes=VALUES(Likes),
        Comments=VALUES(Comments),
        Favorite_Count=VALUES(Favorite_Count),
        Definition=VALUES(Definition),
        Caption_Status=VALUES(Caption_Status)
    """
    cursor.executemany(insert_query, [(
        video['Video_Id'],
        video['Channel_Id'],
        video['Title'],
        video['Tags'],
        video['Thumbnail'],
        video['Description'],
        video['Published_Date'],
        video['Duration'],
        video['Views'],
        video['Likes'],
        video['Comments'],
        video['Favorite_Count'],
        video['Definition'],
        video['Caption_Status']
    ) for video in video_data])
    connection.commit()

# Streamlit UI
st.set_page_config(page_title="Youtube Data Handling", layout="wide", initial_sidebar_state="auto", menu_items=None)
st.title(':red[YouTube Data Fetcher]')

tab1, tab2,tab3,tab4= st.tabs(['view','Store to MYSQl','Show DB data','Questions'])

with tab1:
    channel_id = st.text_input('Enter YouTube Channel ID')
    if st.button('view'):
        if channel_id:
            channel_info = get_channel_info(channel_id)
            st.markdown(f'<span style="color:blue; font-size:40px; font-weight:bold;">Channel Information</span>',unsafe_allow_html=True)
            st.markdown(f'<span style="color:Red; font-size:20px; font-weight:bold; font-family:Georgia, serif;">Channel ID:</span> {channel_info["Channel_Id"]}', unsafe_allow_html=True)
            st.markdown(f'<span style="color:Red; font-size:20px; font-weight:bold; font-family:Georgia, serif;">Channel Name:</span> {channel_info['Channel_Name']}',unsafe_allow_html=True)
            st.markdown(f'<span style="color:Red; font-size:20px; font-weight:bold; font-family:Georgia, serif;">Subscribers:</span> {channel_info['Subscribers']}',unsafe_allow_html=True)
            st.markdown(f'<span style="color:Red; font-size:20px; font-weight:bold; font-family:Georgia, serif;">Views:</span> {channel_info['Views']}',unsafe_allow_html=True)
            st.markdown(f'<span style="color:Red; font-size:20px; font-weight:bold; font-family:Georgia, serif;">Total Videos:</span> {channel_info['Total_Videos']}',unsafe_allow_html=True)
            st.markdown(f'<span style="color:Red; font-size:20px; font-weight:bold; font-family:Georgia, serif;">Channel Description:</span> {channel_info['Channel_Description']}',unsafe_allow_html=True)
        else:
            st.error('Please enter a YouTube Channel ID.')

with tab2:
    
    
    if st.button('Store Data to SQL'):
        with st.spinner(':Green[Uploading....]'):
            # Fetch and store channel information
            channel_info = get_channel_info(channel_id)
            save_to_mysql('channel_info', [channel_info])

            # Fetch and store video information
            video_ids = get_videos_ids(channel_id)
            video_data = get_video_info(video_ids)
            #video_df = pd.DataFrame(video_data)
            save_video_data_to_mysql(video_data)
            
            # Fetch and store comment information
            comment_info = get_comment_info(video_ids)
            save_to_mysql('comment_info', comment_info)
            
            st.success('Data saved to MySQL database successfully!',icon="✅")

with tab3:
    # Display saved data from MySQL
    if st.button('Show Saved Data'):
        with st.spinner('Fetching....'):
            conn = pymysql.connect(host=host, user=username, password=password, database=database_name)
            channel_df = pd.read_sql('SELECT * FROM channel_info', conn)
            video_df = pd.read_sql('SELECT * FROM video_info', conn)
            comment_df = pd.read_sql('SELECT * FROM comment_info', conn)
            conn.close()

            st.write('Saved Channel Information')
            st.dataframe(channel_df)

            st.write('Saved Video Information')
            st.dataframe(video_df)

            st.write('Saved Comment Information')
            st.dataframe(comment_df)

with tab4:
    
        conn = pymysql.connect(host=host, user=username, password=password, database=database_name)
        cursor = conn.cursor()

        
        details = st.selectbox('Select any questions given below :', ['Click the question that you would like to query', 
                                                                      '1. Display all the videos and the channel name', 
                                                                      '2. Display the channels with most number of videos',
                                                                      '3. Display the 10 most viewed videos',
                                                                      '4. Display the comments in each video', 
                                                                      '5. Display the videos with highest likes',
                                                                      '6. Display the likes of all videos',
                                                                      '7. Display the views of each channel', 
                                                                      '8. Display the videos published in the year of 2024',
                                                                      '9. Display the average duration of all videos in each channel', 
                                                                      '10. Display the videos with highest number of comments'])
        
        connection = pymysql.connect(host=host, user=username, password=password)
        cursor = connection.cursor()
        connection.select_db(database_name)

        
        # Check the selected question and construct the corresponding query
        if details == '1. Display all the videos and the channel name':
            query = '''SELECT vi.Title AS videos, ci.Channel_Name AS channelname
                    FROM video_info vi
                    JOIN channel_info ci ON vi.Channel_Id = ci.Channel_Id'''
            columns = ["video title", "channel name"]
            cursor.execute(query)
            data = cursor.fetchall()
            df = pd.DataFrame(data, columns=columns)
            st.write(df)

        elif details == '2. Display the channels with most number of videos':
            query = '''SELECT Channel_Name AS channelname, Total_Videos AS no_videos
                    FROM channel_info
                    ORDER BY Total_Videos DESC LIMIT 1'''
            columns = ["channel name", "No of videos"]
            cursor.execute(query)
            data = cursor.fetchall()
            df = pd.DataFrame(data, columns=columns)
            st.write(df)

        elif details == '3. Display the 10 most viewed videos':
            query = '''SELECT vi.Views AS views, ci.Channel_Name AS channelname, vi.Title AS videotitle
                    FROM video_info vi
                    JOIN channel_info ci ON vi.Channel_Id = ci.Channel_Id
                    WHERE vi.Views IS NOT NULL
                    ORDER BY vi.Views DESC
                    LIMIT 10'''
            columns = ["views", "channel name", "videotitle"]
            cursor.execute(query)
            data = cursor.fetchall()
            df = pd.DataFrame(data, columns=columns)
            st.write(df)

        elif details == '4. Display the comments in each video':
            query = '''SELECT Comments AS no_comments, Title AS videotitle
                    FROM video_info
                    WHERE Comments IS NOT NULL'''
            columns = ["no of comments", "videotitle"]
            cursor.execute(query)
            data = cursor.fetchall()
            df = pd.DataFrame(data, columns=columns)
            st.write(df)

        elif details == '5. Display the videos with highest likes':
            query = '''SELECT vi.Title AS videotitle, ci.Channel_Name AS channelname, Likes AS likecount
                    FROM video_info vi
                    JOIN channel_info ci ON vi.Channel_Id = ci.Channel_Id
                    WHERE Likes IS NOT NULL
                    ORDER BY Likes DESC LIMIT 1'''
            columns = ["videotitle", "channelname", "likecount"]
            cursor.execute(query)
            data = cursor.fetchall()
            df = pd.DataFrame(data, columns=columns)
            st.write(df)

        elif details == '6. Display the likes of all videos':
            query = '''SELECT Likes AS likecount, Title AS videotitle
                    FROM video_info'''
            columns = ["likecount", "videotitle"]
            cursor.execute(query)
            data = cursor.fetchall()
            df = pd.DataFrame(data, columns=columns)
            st.write(df)

        elif details == '7. Display the views of each channel':
            query = '''SELECT Channel_Name AS channelname, Views AS totalviews
                    FROM channel_info'''
            columns = ["channel name", "totalviews"]
            cursor.execute(query)
            data = cursor.fetchall()
            df = pd.DataFrame(data, columns=columns)
            st.write(df)

        elif details == '8. Display the videos published in the year of 2024':
            query = '''SELECT Title AS video_title, Published_Date AS videorelease, ci.Channel_Name AS channelname
                    FROM video_info vi
                    JOIN channel_info ci ON vi.Channel_Id = ci.Channel_Id
                    WHERE YEAR(STR_TO_DATE(Published_Date, '%Y-%m-%d')) = 2024'''
            columns = ["videotitle", "published_date", "channelname"]
            cursor.execute(query)
            data = cursor.fetchall()
            df = pd.DataFrame(data, columns=columns)
            st.write(df)

        elif details == '9. Display the average duration of all videos in each channel':
            query = '''SELECT ci.Channel_Name AS channelname, AVG(Duration) AS averageduration
                    FROM video_info vi
                    JOIN channel_info ci ON vi.Channel_Id = ci.Channel_Id
                    GROUP BY ci.Channel_Name'''
            columns = ["channelname", "averageduration"]
            cursor.execute(query)
            data = cursor.fetchall()
            df = pd.DataFrame(data, columns=columns)
            st.write(df)

        elif details == '10. Display the videos with highest number of comments':
            query = '''SELECT vi.Title AS videotitle, ci.Channel_Name AS channelname, Comments AS comments
                    FROM video_info vi
                    JOIN channel_info ci ON vi.Channel_Id = ci.Channel_Id
                    WHERE Comments IS NOT NULL
                    ORDER BY Comments DESC LIMIT 1'''
            columns = ["video title", "channel name", "comments"]
            cursor.execute(query)
            data = cursor.fetchall()
            df = pd.DataFrame(data, columns=columns)
            st.write(df)

        