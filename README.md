# Youtube-Data-Harvesting-and-Warehousing
**Project Overview:**
This project aims to create a user-friendly Streamlit application for accessing and analyzing data from multiple YouTube channels.

**Key Features:**
1. **YouTube Data Retrieval:** Users can enter a YouTube channel ID and retrieve essential data such as channel name, subscribers, total video count, playlist ID, video ID, likes, dislikes, and comments using the Google API.

2. **Data Storage:** The application allows storing retrieved data in MongoDB, providing a flexible data lake. Users can collect data from up to 10 YouTube channels and store them with a single click.

3. **Data Migration:** Users can select a channel name and migrate its data from MongoDB to a SQL database for structured storage and analysis.

4. **Querying SQL Data:** Users can search and retrieve data from the SQL database, including joining tables for comprehensive channel details.

5. **Integration:** The application seamlessly integrates with the YouTube API, MongoDB, and SQL databases.

**Technologies Used:**
- Python (libraries like googleapiclient, pymongo, pymysql, pandas)
- Streamlit
- MongoDB
- SQL Database (e.g., MySQL, PostgreSQL)
- Google API client library for Python

**Conclusion:**
This project simplifies the process of accessing and analyzing YouTube channel data through an intuitive Streamlit interface. Users can effortlessly retrieve, store, and query data, enabling insightful analysis with minimal effort.
