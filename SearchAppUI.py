import streamlit as st
import pandas as pd
import psycopg2
import psycopg2.extras
import plotly.graph_objects as go
import plotly.express as px
import streamlit as st
import pandas as pd
import psycopg2
from application import *
from pymongo.mongo_client import MongoClient
from urllib.parse import quote_plus
from pymongo.mongo_client import MongoClient
from urllib.parse import quote_plus


username = "preethirameshtce"
password = quote_plus("Preethi@99")  # URL-encode the password

uri = f"mongodb+srv://{username}:{password}@cluster0.mnwlroe.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

# Create a new client and connect to the server
client = MongoClient(uri, tlsAllowInvalidCertificates=True)
mongo_client = client
#mongo_db = mongo_client["mongo_dbms"]
collection = client.mongo_dbms.tweets

# Send a ping to confirm a successful connection
try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)

# SearchApps----------------------------------------------------------------------------------------------------------------------------
# Sidebar for navigation
optionss = ["Search", "Top 10 Users", "Top 10 Tweets"]
choice = st.sidebar.selectbox("Choose an Option", optionss)


conn = psycopg2.connect(
            host="localhost",
            database="postgres",
            user="postgres",
            password="passkey123",
            port="5433"
        )

if choice == "Search":


    def query_data():
        conn = psycopg2.connect(
            host="localhost",
            database="postgres",
            user="postgres",
            password="passkey123",
            port="5433"
        )


        # df = pd.read_sql("select hashtags from twitter_users WHERE hashtags IS NOT NULL and hashtags != '{}'", conn)
        df = pd.read_sql("select user_id,user_name, hashtags from twitter_users WHERE hashtags IS NOT NULL and hashtags != '{}'", conn)

        def clean_and_split_tags(tag_string):
            # Remove curly braces and split by comma
            return tag_string.strip('{}').split(',')

        # Apply the function to each row and explode into multiple rows
        df['hashtags'] = df['hashtags'].apply(clean_and_split_tags)

        print(df)
        df_exploded = df.explode('hashtags')

        # Get unique tags and create a new data frame
        unique_tags = df_exploded['hashtags'].unique()
        unique_df = pd.DataFrame(unique_tags, columns=['Unique_Tags'])
        
        hashtag_list= unique_df['Unique_Tags'].values.tolist()
        
        # Execute the query

        # Fetch and print all results
        # user_list=df["user_name"].values.tolist()
        user_list = ['NA']

        time_stamps = fetch_data_with_caching("SELECT MIN(created_at) as MIN,MAX(created_at) as MAX from twitter_users")
        # print('test')
        # print(time_stamps)
        date_range=pd.read_sql("SELECT MIN(created_at) as MIN,MAX(created_at) as MAX from twitter_users",conn)
    
        
        min_date=date_range["min"][0].to_pydatetime()
        max_date=date_range["max"][0].to_pydatetime()
        return hashtag_list,user_list,min_date,max_date, df
            

    # Call the function
    hashtag_list,user_list,start_date,end_date, df=query_data()


    # users,start_date,end_date=query_data()

    st.title("Search by Hashtag")

    # selected_option=st.selectbox("Choose option",users)
    selected_option = st.selectbox("Choose your #HASHTAG",hashtag_list)
    selected_date = st.slider("Select a date", min_value=start_date, max_value=end_date, value=(start_date,end_date), format="YYYY-MM-DD")

    user_id_list = []
    user_name_list = []
    for i in range(0, len(df)):
        if selected_option in df['hashtags'][i]:
            user_id_list.append(df['user_id'][i])
            user_name_list.append(df['user_name'][i])


    selected_option2 = st.selectbox("Select the below Users using above tweet",user_name_list)


    user_id = df[df["user_name"]==selected_option2]["user_id"].values.tolist()[0]

    
    mongo_query2 = {
        "collection": "ReTweets",
        "filter": {
            "USER_ID": user_id,  # replace with the actual user ID you're interested in
            "ENTITIES_HASHTAGS": selected_option  # replace with the actual hashtag you're interested in
        },
        "projection": {"TWEET_ID": 1, "TEXT": 1, "ORG_RETWEET_COUNT": 1, "_id": 0},
        "sort": [("ORG_RETWEET_COUNT", pymongo.DESCENDING)],
        "limit": 10
    }

    # st.write(mongo_query2)

    mongo_top_tweets2 = fetch_data_with_caching(mongo_query2)

    normalized_mongo_tweets2 = normalize_data(mongo_top_tweets2, {'TWEET_ID': 'tweet_id', 'TEXT': 'text', 'ORG_RETWEET_COUNT': 'retweets'})
    st.write('RETWEETS WITH THE ABOVE HASHTAG')
    st.write(normalized_mongo_tweets2)


    mongo_query3 = {
        "collection": "tweets",
        "filter": {
            "USER_ID": user_id,  # replace with the actual user ID you're interested in
            "ENTITIES_HASHTAGS": selected_option  # replace with the actual hashtag you're interested in
        },
        "projection": {"TWEET_ID": 1, "TEXT": 1,  "_id": 0},
        "limit": 10
    }

    # st.write(mongo_query3)

    mongo_top_tweets3 = fetch_data_with_caching(mongo_query3)

    normalized_mongo_tweets3 = normalize_data(mongo_top_tweets3, {'TWEET_ID': 'tweet_id', 'TEXT': 'text', 'ORG_RETWEET_COUNT': 'retweets'})
    st.write('TWEETS WITH THE ABOVE HASHTAG')
    st.write(normalized_mongo_tweets3)


    # conn = psycopg2.connect(
    #     host="localhost",
    #     database="postgres",
    #     user="postgres",
    #     password="passkey123",
    #     port="5433"
    # )



    # user_results=search_by_username(selected_option,start_date,end_date)
    # st.write(selected_date)


    # def get_hashtags(user):
  
    #     # Create a cursor object
    #     hash_df = pd.read_sql("select hashtags from twitter_users WHERE user_name  = '{user}' and hashtags IS NOT NULL;".format(user=user),conn)
    #     # hash_df['hashtags'] = hash_df['hashtags'].apply(ast.literal_eval)

    #     result = hash_df['hashtags']

    #     # Extracting the part inside the curly braces
    #     hashtags_string = hash_df['hashtags'][0].split('{')[1].split('}')[0]

    #     # Splitting the string into a list of hashtags
    #     hashtags_list = hashtags_string.split(',')

    #     # Creating a DataFrame
    #     h_df = pd.DataFrame(hashtags_list, columns=['HASHTAG'])
    #     hashtags_list.extend(['coronavirus','animals'])
    #     return h_df,hashtags_list

    # user_hashtag,h_list = get_hashtags(selected_option)

    # if user_hashtag['HASHTAG'][0] :
    #     st.write("LIST OF HASHTAGS")
    #     st.dataframe(user_hashtag)

    # selected_option2=st.selectbox("Choose option",h_list)

    # # st.write("LIST OF TWEETS")
    # # st.write(user_results)
    # df = pd.read_sql("select user_id from twitter_users WHERE user_name  = '{user}' and hashtags IS NOT NULL;".format(user='Dobie4ever'),conn)
    # a_userid=int(df['user_id'][0])
    # print(a_userid)
    # print(type(a_userid))
    # print(a_userid)
    # selected_usr_id=str(a_userid)


    # mongo_query = {

    #     "filter": {
    #         "USER_ID": a_userid,
    #         "ENTITIES_HASHTAGS": selected_option2
    #         },
    #     "projection": {
    #             "TWEET_ID": 1,
    #             "TEXT": 1,
    #             "_id": 0
    #         }
    #     }
    


    # data = list(collection.find(mongo_query['filter'], mongo_query['projection']))

    # # mongotweets = fetch_data_with_caching(mongo_query)
    # print(data)
    # st.write(data)


if choice == "Search by text":

    text_input=st.text_input("Search For:")

    st.write(text_input)
    query = {
        'text': {
            '$regex': text_input,
            '$options': 'i'  # Case-insensitive matching
        }
    }
    #st.write(query)
    results = collection.find(query)
    df3=pd.DataFrame(list(results))
    st.dataframe(results)



    
if choice == "Top 10 Users": 

    st.title("Top 10 Users")
    df = pd.read_sql("select user_id, user_name, user_verified, followers_count, friends_count, statuses_count from twitter_users WHERE followers_count IS NOT NULL ORDER BY  followers_count DESC LIMIT 10", conn)
    st.dataframe(df)

    df1 = df.sort_values('followers_count', ascending=False).head(5)
    df2 = df.sort_values('friends_count', ascending=False).head(5)
    # df3 = df.sort_values('statuses_count', ascending=False).head(5)
    df3 = pd.read_sql("select country_code, count(user_name) as user_count from twitter_users WHERE country_code IS NOT NULL GROUP BY country_code", conn)
    df4 = pd.read_sql("select user_verified, followers_count from twitter_users WHERE friends_count IS NOT NULL", conn)
    


    fig1 = px.bar(df1, x='user_name', y='followers_count', title='Top 5 Users by Followers Count')
    fig2 = px.bar(df2, x='user_name', y='friends_count', title='Top 5 Users by Friends Count')
    # fig3 = px.bar(df3, x='user_name', y='statuses_count', title='Top 5 Users by statuses Count')
    fig3 = px.pie(df3, names="country_code", values="user_count", title='Countries of Users in DataSet')
    fig4 = px.pie(df4, names="user_verified", values="followers_count", title='User_verified in the DataSet')


    col1, col2 = st.columns(2)

    with col1:
            st.plotly_chart(fig1, use_container_width=True)
            st.plotly_chart(fig4, use_container_width=True)
            

    with col2:
        st.plotly_chart(fig2, use_container_width=True)
        st.plotly_chart(fig3, use_container_width=True)


if choice == "Top 10 Tweets": 

    st.title("Top 10 Tweets")


    mongo_query = {
        "collection": "ReTweets",
        "filter": {},
        "projection": {"TWEET_ID": 1, "TEXT": 1, "ORG_RETWEET_COUNT": 1, "_id": 0},
        "sort": [("ORG_RETWEET_COUNT", pymongo.DESCENDING)],
        "limit": 10
    }

    mongo_top_tweets = fetch_data_with_caching(mongo_query)

    normalized_mongo_tweets = normalize_data(mongo_top_tweets, {'TWEET_ID': 'tweet_id', 'TEXT': 'text', 'ORG_RETWEET_COUNT': 'retweets'})

    st.write(normalized_mongo_tweets)

    

