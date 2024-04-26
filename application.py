###### import psycopg
###### Functions to call are defined here 
import pymongo
from pymongo.mongo_client import MongoClient
from urllib.parse import quote_plus
import json
import time
import hashlib
import os
import psycopg2.extras


# PostgreSQL Connection-------------------------------------------------------------
def get_postgres_connection():
    return psycopg2.connect(
        user='postgres',
        password='passkey123',
        host='localhost',
        port='5433',
        database='postgres'
    )

# MongoDB Connection---------------------------------------------------------------------------------------

username = "preethirameshtce"
password = quote_plus("Preethi@99")
uri = f"mongodb+srv://{username}:{password}@cluster0.mnwlroe.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
mongo_client = MongoClient(uri, tlsAllowInvalidCertificates=True)
mongo_db = mongo_client["mongo_dbms"]  # Assuming the MongoDB database is called mongo_dbms

def get_mongo_client():
    username = "preethirameshtce"
    password = quote_plus("Preethi@99")
    uri = f"mongodb+srv://{username}:{password}@cluster0.mnwlroe.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
    return MongoClient(uri, tlsAllowInvalidCertificates=True)

# Cache Initialization--------------------------------------------------------------------------------------
cache = {}
last_checkpoint_time = time.time()
MAX_CACHE_SIZE = 1000
CHECKPOINT_INTERVAL = 3600  # Checkpoint every 1 hour
CACHE_CHECKPOINT_DIR = "cache_checkpoint"
os.makedirs(CACHE_CHECKPOINT_DIR, exist_ok=True)

def generate_cache_key(query):
    return hashlib.md5(str(query).encode()).hexdigest()


def fetch_data_with_caching(query):
    cache_key = generate_cache_key(query)
    if cache_key in cache:
        print("Data fetched from cache")
        cache[cache_key]['access_time'] = time.time()
        return cache[cache_key]['data']
    
    data = None
    try:
        if isinstance(query, tuple):  # SQL query
            sql_query, params = query
            print("Data fetched from PostgreSQL")
            with postgres_connection.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
                cursor.execute(sql_query, params)
                data = cursor.fetchall()  # This will now fetch as a list of dicts
        elif isinstance(query, dict) and 'aggregate' in query:  # MongoDB aggregation
            print("Data fetched from MongoDB using aggregation")
            collection = mongo_db[query['collection']]
            data = list(collection.aggregate(query['aggregate']))
        elif isinstance(query, dict):  # MongoDB find operation
            print("Data fetched from MongoDB using find")
            collection = mongo_db[query['collection']]
            data = list(collection.find(query['filter'], query['projection']))
    except Exception as e:
        print(f"An error occurred: {e}")
    
    if len(cache) >= MAX_CACHE_SIZE:
        evict_least_accessed()
    cache[cache_key] = {'data': data, 'access_time': time.time()}
    possibly_checkpoint_cache()
    return data



def evict_least_accessed():
    min_access_time = float('inf')
    min_cache_key = None
    for key, value in cache.items():
        if float(value['access_time']) < min_access_time:
            min_access_time = float(value['access_time'])
            min_cache_key = key
    if min_cache_key:
        del cache[min_cache_key]
        print("Evicted cache entry")

def possibly_checkpoint_cache():
    if time.time() - last_checkpoint_time >= CHECKPOINT_INTERVAL:
        checkpoint_cache()

def checkpoint_cache():
    global last_checkpoint_time
    checkpoint_file = os.path.join(CACHE_CHECKPOINT_DIR, "cache_checkpoint.json")
    with open(checkpoint_file, 'w') as f:
        json.dump(cache, f, default=str)
    print("Cache checkpointed to disk")
    last_checkpoint_time = time.time()

def load_cache_from_checkpoint():
    checkpoint_file = os.path.join(CACHE_CHECKPOINT_DIR, "cache_checkpoint.json")
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file, 'r') as f:
            loaded_cache = json.load(f)
        return loaded_cache
    return {}


#----------------------------------------------------------------------------------------------------
def search_by_username(search_query, start_date, end_date):
    print("Searching by user...")
    start_timestamp = f"{start_date}T00:00:00Z"
    end_timestamp = f"{end_date}T23:59:59Z"
    user_results = []

    try:
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
                sql_query = "SELECT user_id, user_name FROM twitter_users WHERE user_name ILIKE %s"
                cursor.execute(sql_query, (f'%{search_query}%',))
                user_results = cursor.fetchall()
                print(f"Found {len(user_results)} users in PostgreSQL.")
    except Exception as e:
        print(f"PostgreSQL query failed: {e}")
        return []

    user_ids = [user['user_id'] for user in user_results]
    tweet_results = []

    try:
        with get_mongo_client() as client:
            db = client["mongo_dbms"]
            for collection_name in ['tweets', 'ReTweets']:
                collection = db[collection_name]
                mongo_query = {
                    'filter': {
                        'USER_ID': {'$in': user_ids},
                        'DATE_STAMP': {'$gte': start_timestamp, '$lte': end_timestamp}
                    },
                    'projection': {'_id': 0, 'USER_ID': 1, 'TEXT': 1, 'DATE_STAMP': 1, 'TWEET_ID': 1, 'TWEET_LANGUAGE': 1, 'ORG_RETWEET_COUNT': 1}
                }
                results = list(collection.find(mongo_query['filter'], mongo_query['projection']))
                tweet_results.extend(results)
                print(f"Found {len(results)} tweets in MongoDB collection {collection_name}.")
    except Exception as e:
        print(f"MongoDB query failed: {e}")
        return []

    combined_results = []
    for user in user_results:
        user_tweets = [tweet for tweet in tweet_results if tweet['USER_ID'] == user['user_id']]
        for tweet in user_tweets:
            combined_results.append({
                'user_id': user['user_id'],
                'user_name': user['user_name'],
                'tweet_id': tweet.get('TWEET_ID', 'N/A'),
                'text': tweet.get('TEXT', 'N/A'),
                'created_at': tweet.get('DATE_STAMP', 'N/A'),
                'retweets': tweet.get('ORG_RETWEET_COUNT', 0)
            })
    return combined_results

def search_by_text_or_hashtag(search_query, start_date, end_date, search_field='text'):
    print("Searching by text or hashtag...")
    start_timestamp = f"{start_date}T00:00:00Z"
    end_timestamp = f"{end_date}T23:59:59Z"
    results = []
    try:
        with get_mongo_client() as client:
            db = client["mongo_dbms"]
            for collection_name in ['tweets', 'ReTweets']:
                collection = db[collection_name]
                field = 'TEXT' if search_field == 'text' else 'HASHTAGS'
                mongo_query = {
                    'filter': {
                        field: {'$regex': search_query, '$options': 'i'},
                        'DATE_STAMP': {'$gte': start_timestamp, '$lte': end_timestamp}
                    },
                    'projection': {
                        '_id': 0,
                        'USER_ID': 1,
                        'TEXT': 1,
                        'DATE_STAMP': 1,
                        'TWEET_ID': 1,
                        'TWEET_LANGUAGE': 1,
                        'ORG_RETWEET_COUNT': 1
                    }
                }
                found = list(collection.find(mongo_query['filter'], mongo_query['projection']))
                results.extend(found)
                print(f"Found {len(found)} results in total from MongoDB collection {collection_name}.")
    except Exception as e:
        print(f"Error during MongoDB search: {e}")
        return []

    return results


#---------------------------------------
def display_tweets_users(tweets):
    if not tweets:
        print("No tweets found.")
        return

    # Convert all 'retweets' to floats and handle 'N/A' or missing cases
    for tweet in tweets:
        retweets_value = tweet.get('ORG_RETWEET_COUNT', 0)  # default to 0 if not available
        if isinstance(retweets_value, str) and retweets_value == 'N/A':
            tweet['retweets'] = 0  # replace 'N/A' with 0 for consistent data type
        else:
            tweet['retweets'] = float(retweets_value)

    # Sort tweets based on the retweet count in descending order
    sorted_tweets = sorted(tweets, key=lambda x: x['retweets'], reverse=True)

    # Initially display only the top 10 tweets
    top_tweets = sorted_tweets[:10]
    display_tweet_list(top_tweets)

    # First ask if the user wants to see all tweets
    response = input("Do you want to see all tweets? (Yes/No): ")
    if response.strip().lower() == 'yes':
        display_tweet_list(sorted_tweets)  # Display all tweets if yes
    elif response.strip().lower() != 'no':
        print("Invalid input. Please answer 'Yes' or 'No'.")

    # Then ask for more detailed interaction after showing the list
    while True:
        response = input("Enter the number of a tweet to view more details, 'exit' to stop, or 'list' to show the tweets again: ")
        if response.lower() == 'exit':
            break
        elif response.lower() == 'list':
            display_tweet_list(sorted_tweets)  # show the current list, whether it's all tweets or just the top 10
        elif response.isdigit() and 1 <= int(response) <= len(sorted_tweets):
            tweet_index = int(response) - 1
            tweet = sorted_tweets[tweet_index]
            print("\nTweet Details:")
            print(f"Tweet ID: {tweet.get('TWEET_ID', 'N/A')}")
            print(f"Text: {tweet.get('TEXT', 'N/A')}")
            print(f"Tweet Language: {tweet.get('TWEET_LANGUAGE', 'N/A')}")
            print(f"Tweeted at: {tweet.get('DATE_STAMP', 'N/A')}")
            print(f"Author ID: {tweet.get('USER_ID', 'N/A')}")
            print(f"Retweets: {tweet['retweets']}")
            view_retweets_response = input("Press 'r' to view retweeters or any other key to continue: ")
            if view_retweets_response.lower() == 'r':
                if 'TWEET_ID' in tweet:
                    display_retweets(tweet['TWEET_ID'])
                else:
                    print("Tweet ID not available, cannot display retweets.")
        else:
            print("Invalid input, please try again.")

def display_tweet_list(tweets):
    for index, tweet in enumerate(tweets, start=1):
        user_name = get_user_name_by_id(tweet.get('USER_ID', 'N/A'))
        print(f"{index}. Author: {user_name}, Tweeted at: {tweet.get('DATE_STAMP', 'N/A')}, Retweets: {tweet['retweets']}")




def display_retweets(tweet_id):
    if tweet_id is None:
        print("Tweet ID not available, cannot display retweets.")
        return

    print(f"Displaying retweets for Tweet ID: {tweet_id}")

    try:
        with get_mongo_client() as client:
            db = client["mongo_dbms"]
            retweets_collection = db["ReTweets"]
            # Ensure the tweet_id is cast to string if necessary
            tweet_id_str = str(tweet_id)
            retweets_query = {
                'ORG_TWEET_ID': tweet_id_str
            }
            retweets_projection = {
                '_id': 0,
                'USER_ID': 1,
                'CREATED_AT': 1
            }
            retweets = list(retweets_collection.find(retweets_query, retweets_projection))

            if not retweets:
                print("No retweets found for this tweet.")
            else:
                for retweet in retweets:
                    user_name = get_user_name_by_id(retweet['USER_ID'])
                    retweet_time = retweet['CREATED_AT']
                    print(f"User: {user_name}, Retweeted at: {retweet_time}")
    except Exception as e:
        print(f"Error fetching retweets: {e}")



def get_user_name_by_id(user_id):
    """
    Fetch the user name from PostgreSQL database based on user_id.
    """
    conn = None
    try:
        conn = psycopg2.connect(
            user='postgres',
            password='passkey123',  # Adjust password and other connection details as necessary
            host='localhost',
            port='5433',
            database='postgres'
        )

        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cursor.execute("SELECT user_name FROM twitter_users WHERE user_id = %s", (user_id,))
        result = cursor.fetchone()
        if result:
            return result['user_name']
        else:
            return "Unknown"  # Default if no user is found
    except psycopg2.DatabaseError as e:
        print(f"Database error: {e}")
        return "Unknown"
    finally:
        if conn:
            conn.close()

    return "Unknown"  # Return unknown if no connection was made or if there were other issues


        
def get_top_users():
    """
    Retrieve top 10 users based on followers count and tweet activity.
    """
    # SQL query
    sql_query = "SELECT user_name, followers_count FROM twitter_users ORDER BY followers_count DESC LIMIT 10"
    sql_params = []
    sql_top_users = fetch_data_with_caching((sql_query, sql_params))

    # MongoDB aggregation query
    mongo_query = {
        "collection": "tweets",
        "aggregate": [
            {"$group": {"_id": "$USER_ID", "tweet_count": {"$sum": 1}}},
            {"$sort": {"tweet_count": -1}},
            {"$limit": 10},
            {"$lookup": {
                "from": "twitter_users",
                "localField": "_id",
                "foreignField": "user_id",
                "as": "user_details"
            }},
            {"$unwind": "$user_details"},
            {"$project": {
                "user_name": "$user_details.user_name",
                "tweet_count": "$tweet_count",
                "_id": 0
            }}
        ]
    }
    mongo_top_users = fetch_data_with_caching(mongo_query)

    return sql_top_users + mongo_top_users

def get_top_tweets():
    """
    Retrieve and display the top 10 tweets based on retweet count from MongoDB.
    """
    # Fetch and cache MongoDB data
    mongo_query = {
        "collection": "tweets",
        "filter": {},
        "projection": {"TWEET_ID": 1, "TEXT": 1, "ORG_RETWEET_COUNT": 1, "_id": 0},
        "sort": [("ORG_RETWEET_COUNT", pymongo.DESCENDING)],
        "limit": 10
    }
    mongo_top_tweets = fetch_data_with_caching(mongo_query)

    # Normalize MongoDB data
    normalized_mongo_tweets = normalize_data(mongo_top_tweets, {'TWEET_ID': 'tweet_id', 'TEXT': 'text', 'ORG_RETWEET_COUNT': 'retweets'})

    return normalized_mongo_tweets



def normalize_data(data, keys):
    """
    Normalize data entries according to the specified key mappings.
    'keys' can be a list or a dictionary mapping source keys to new keys.
    Ensures all keys are present by setting defaults for missing keys.
    Filters the data to only include the top 10 tweets based on retweet count.
    """
    normalized_data = []
    key_map = keys if isinstance(keys, dict) else {k: k for k in keys}

    for item in data:
        normalized_item = {}
        for src_key, new_key in key_map.items():
            # Ensure every expected key is present, set to None or a default if missing
            normalized_item[new_key] = item.get(src_key) if isinstance(item, dict) else None
        if 'retweets' not in normalized_item or normalized_item['retweets'] is None:
            normalized_item['retweets'] = 0  # Set default value for 'retweets' if missing or None
        normalized_data.append(normalized_item)

    # Sort the normalized data by retweet count in descending order
    normalized_data.sort(key=lambda x: x['retweets'], reverse=True)

    # Return only the top 10 tweets
    return normalized_data[:10]


def display_tweets_for_users(tweets):
    if not tweets:
        print("No tweets found.")
        return
    
    for index, tweet in enumerate(tweets, start=1):
        print(f"{index}. Author: {tweet.get('user_name', 'N/A')}, Tweeted at: {tweet.get('created_at', 'N/A')}, Retweets: {tweet.get('retweets', 'N/A')}")

    while True:
        response = input("Enter the number of a tweet to view more details, 'exit' to stop, or 'list' to show the tweets again: ")
        if response.lower() == 'exit':
            break
        elif response.lower() == 'list':
            for index, tweet in enumerate(tweets, start=1):
                author_name = tweet.get('user_name', 'N/A')
                created_at = tweet.get('created_at', 'N/A')
                retweets = tweet.get('retweets', 'N/A')
                print(f"{index}. Author: {author_name}, Tweeted at: {created_at}, Retweets: {retweets}")
        elif response.isdigit() and 1 <= int(response) <= len(tweets):
            tweet_index = int(response) - 1
            tweet = tweets[tweet_index]
            print("\nTweet Details:")
            print(f"Tweet ID: {tweet.get('tweet_id', 'N/A')}")
            print(f"Text: {tweet.get('text', 'N/A')}")
            print(f"Tweet Language: {tweet.get('tweet_language', 'N/A')}")
            print(f"Tweeted at: {tweet.get('created_at', 'N/A')}")
            print(f"Author ID: {tweet.get('user_id', 'N/A')}")
            print(f"Retweets: {tweet.get('retweets', 'N/A')}")

            view_retweets_response = input("Press 'r' to view retweeters or any other key to continue: ")
            if view_retweets_response.lower() == 'r':
                # Check if 'tweet_id' key exists before calling display_retweets
                if 'tweet_id' in tweet:
                    display_retweets(tweet['tweet_id'])
                else:
                    print("Tweet ID not available, cannot display retweets.")
        else:
            print("Invalid input, please try again.")
            


            
def search_by_hashtag(hashtag, start_date, end_date):
    print("Searching by hashtag...")
    start_timestamp = f"{start_date}T00:00:00Z"
    end_timestamp = f"{end_date}T23:59:59Z"
    results = []

    # First, find all user IDs associated with the hashtag from the PostgreSQL database
    try:
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
                sql_query = """
                SELECT user_id, user_name FROM twitter_users
                WHERE hashtags LIKE %s
                """
                cursor.execute(sql_query, (f'%{hashtag}%',))
                user_data = cursor.fetchall()
                user_ids = [user['user_id'] for user in user_data]
    except Exception as e:
        print(f"PostgreSQL query failed: {e}")
        return []

    # Now, fetch tweets related to these user IDs from MongoDB
    try:
        with get_mongo_client() as client:
            db = client["mongo_dbms"]
            mongo_query = {
                'filter': {
                    'USER_ID': {'$in': user_ids},
                    'DATE_STAMP': {'$gte': start_timestamp, '$lte': end_timestamp}
                },
                'projection': {
                    '_id': 0,
                    'USER_ID': 1,
                    'TEXT': 1,
                    'DATE_STAMP': 1,
                    'TWEET_ID': 1,
                    'TWEET_LANGUAGE': 1,
                    'ORG_RETWEET_COUNT': 1
                }
            }
            tweet_results = list(db.tweets.find(mongo_query['filter'], mongo_query['projection']))
            print(f"Found {len(tweet_results)} tweets related to hashtag #{hashtag}.")
    except Exception as e:
        print(f"MongoDB query failed: {e}")
        return []

    # Combine the results with user details
    combined_results = []
    for tweet in tweet_results:
        user_info = next((user for user in user_data if user['user_id'] == tweet['USER_ID']), None)
        combined_results.append({
            'user_id': tweet['USER_ID'],
            'user_name': user_info['user_name'] if user_info else 'Unknown',
            'tweet_id': tweet['TWEET_ID'],
            'text': tweet['TEXT'],
            'created_at': tweet['DATE_STAMP'],
            'retweets': tweet.get('ORG_RETWEET_COUNT', 0)  # Using get with default value 0
        })

    return combined_results

        

#---------------------------------------------------------------------------------------------------------

def main():
    global cache
    cache = load_cache_from_checkpoint()

    # Establish the database connections
    postgres_connection = get_postgres_connection()
    mongo_client = get_mongo_client()

    try:
        while True:
            print("\nChoose search type or view top lists:")
            print("1. Search by string/hashtag with time range")
            print("2. Search by hashtag with time range")
            print("3. Search by username with time range")
            print("4. Top 10 users")
            print("5. Top 10 tweets")
            print("0. Exit")
            choice = input("Enter your choice: ")
            if choice == '0':
                break

            if choice == '3':
                query = input("Enter search query: ")
                start_date = input("Enter start date (YYYY-MM-DD): ")
                end_date = input("Enter end date (YYYY-MM-DD): ")
                results = search_by_username(query, start_date, end_date)
                #display_tweets_users(results)
                display_tweets_for_users(results)
            elif choice in ['1']:
                query = input("Enter search query: ")
                start_date = input("Enter start date (YYYY-MM-DD): ")
                end_date = input("Enter end date (YYYY-MM-DD): ")
                search_field = 'text' if choice == '1' else 'hashtag'
                results = search_by_text_or_hashtag(query, start_date, end_date, search_field)
                #display_tweets(results)
                display_tweets_users(results)
            elif choice in ['2']:
                hashtag = input("Enter search #Hashtag: ")
                start_date = input("Enter start date (YYYY-MM-DD): ")
                end_date = input("Enter end date (YYYY-MM-DD): ")
                results = search_by_hashtag(hashtag, start_date, end_date)
                display_tweets_for_users(results)
                #display_tweets_users()
            elif choice == '4':
                top_users = get_top_users()
                print("Top Users:")
                for user in top_users:
                    if isinstance(user, dict):
                        print(f"User Name: {user.get('user_name', 'N/A')}, Followers Count: {user.get('followers_count', 'N/A')}")
                    elif isinstance(user, list) or isinstance(user, tuple):
                        # Assuming that user is a list or tuple where the first element is the username and the second is the follower count
                        print(f"User Name: {user[0]}, Followers Count: {user[1]}")
                    else:
                        print("User data format not recognized.")
            elif choice == '5':
                top_tweets = get_top_tweets()
                print("Top Tweets:")
                for tweet in top_tweets:
                    print(f"Tweet ID: {tweet.get('tweet_id', 'N/A')}, Text: {tweet.get('text', 'N/A')}, Retweets: {tweet.get('retweets', 'N/A')}")

    finally:
        if postgres_connection is not None:
            postgres_connection.close()
        if mongo_client is not None:
            mongo_client.close()
        checkpoint_cache()

if __name__ == "__main__":
    main()