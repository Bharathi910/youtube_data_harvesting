# youtube_data_harvesting_and_warehousing

**Overview**
This project is a Streamlit application designed to harvest data from YouTube, transform it, and store it in a MySQL database. The application allows users to fetch channel, video, and comment information from YouTube, view the data, store it in a database, and perform queries to analyze the stored data.

**Features**
Data Extraction: Fetch channel, video, and comment data from YouTube using the YouTube Data API.
Data Loading: Store the transformed data into a MySQL database.
Data Viewing: View fetched data within the Streamlit application.
Data Querying: Execute predefined SQL queries to analyze the stored data.

**Prerequisites**
Python 3.7+
Streamlit
YouTube Data API Key
MySQL server
Required Python Libraries: streamlit, pandas, google-api-python-client, pymysql

**Usage**

**Extract and View Data:**
Enter a YouTube Channel ID in the Streamlit app.
Click "view" to fetch and display channel information.

**Store Data to MySQL:**
After viewing channel information, click "Store Data to SQL" to fetch related video and comment data and store it in the MySQL database.

**Show Saved Data:**
Click "Show Saved Data" to display data stored in the MySQL database.

**Query Data:**
Select a predefined query from the dropdown in the "Questions" tab to analyze the stored data.
