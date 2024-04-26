# 16-954-694-01_Spring2024_Team6_Project

# Twitter Data Analysis and Search Engine Project

## Overview
The aim of this project is to design and implement a search engine for analyzing Twitter data, leveraging both relational and non-relational databases, caching, and a search application.

## Data Processing
The given Twitter dataset is processed to generate summaries of user statistics, tweet counts, and other relevant facets. User information is stored in PostgreSQL, a relational datastore. Tweets and retweets are separated into two different files and stored in MongoDB, a non-relational datastore.

## Database Design
- **PostgreSQL**: User information is stored in PostgreSQL, which provides a robust relational data structure.
- **MongoDB**: Tweets and retweets are stored in separate collections in MongoDB, allowing for flexible non-relational storage.

Strategies for efficient data storage and retrieval include:
- Use of indexing to speed up queries
- Techniques to manage tweet actions like retweets

## Caching Mechanism
A cache mechanism is implemented to enhance performance for frequently accessed data. The cache uses a Python dictionary, with the following strategies:
- **Eviction policies**: Supports Least Recently Used (LRU) and a Time-To-Live (TTL) field to prevent stale data.
- **Checkpointing**: The cache is checkpointed to disk periodically to ensure persistence.

## Search Application
The search application allows users to query tweets by keyword, hashtag, or user, with time range filtering. It provides metadata for each tweet, such as:
- Author
- Tweet time
- Retweet counts

Additionally, the application offers drill-down features for more detailed information.
