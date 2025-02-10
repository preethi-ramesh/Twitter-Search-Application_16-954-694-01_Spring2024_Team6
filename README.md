# 16-954-694-01_Spring2024_Team6_Project

# Twitter Data Analysis and Search Engine Project

## Overview
This project represents a comprehensive search engine for Twitter data analysis with some collaborative input from Team 6. The system leverages both relational and non-relational databases, implementing sophisticated caching mechanisms and a robust search application.

## Individual Contribution
I took the lead in designing and implementing the core components of this project, including:
- Database architecture and integration
- Search engine functionality
- Caching mechanism design
- Data processing pipeline
- Application interface development

## Data Processing
I developed a data processing pipeline for the Twitter dataset that generates comprehensive summaries of user statistics, tweet counts, and other relevant metrics. The system I designed stores user information in PostgreSQL (relational datastore), while tweets and retweets are separated and stored in MongoDB (non-relational datastore).

## Database Design
I implemented a dual-database architecture:
- **PostgreSQL**: Handles user information with a robust relational data structure
- **MongoDB**: Manages tweets and retweets in separate collections, utilizing flexible non-relational storage

I incorporated several optimization strategies, including:
- Strategic indexing for query performance
- Efficient management of tweet actions and retweets
- Optimized data retrieval patterns

## Caching Mechanism
I designed and implemented a sophisticated cache mechanism using a Python dictionary to enhance performance for frequently accessed data. The cache features:
- **Eviction policies**: Implementation of Least Recently Used (LRU) and Time-To-Live (TTL) field
- **Checkpointing**: Periodic cache persistence to disk for reliability

## Search Application
The search application I developed enables users to query tweets by:
- Keywords
- Hashtags
- User handles
- Time range filtering

Each tweet result includes comprehensive metadata:
- Author information
- Tweet timestamp
- Retweet metrics
- Drill-down capabilities for detailed analysis
