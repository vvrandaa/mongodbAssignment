import pymongo
from datetime import datetime

mongodb_uri = "mongodb://localhost:27017/"
client = pymongo.MongoClient(mongodb_uri)
db = client["mflix_db"]


# COMMENTS COLLECTION
comments_collection = db['comments']

# Task 1: Find top 10 users who made the maximum number of comments
def top_10_users_max_comments():
    top_users = comments_collection.aggregate([
        {"$group": {"_id": "$name", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 10}
    ])
    return list(top_users)

print("\nTop 10 users who made the maximum number of comments:\n")
for user in top_10_users_max_comments():
    print(user['_id'], "-", user['count'])
print('-------------------------------------------------------------------------------------------------------------------')

# Task 2: Find top 10 movies with most comments
def top_10_movies_most_comments():
    top_movies = comments_collection.aggregate([
        {"$group": {"_id": "$movie_id", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 10}
    ])
    return list(top_movies)

print("\nTop 10 movies with most comments:\n")
for movie in top_10_movies_most_comments():
    movie_doc = db.movies.find_one({"_id": movie['_id']})
    print(movie_doc['title'], "-", movie['count'])
print('-------------------------------------------------------------------------------------------------------------------')


# Task 3: Given a year, find the total number of comments created each month in that year
def total_comments_by_month_in_year(given_year):
    comments_by_month = comments_collection.aggregate([
        {"$match": {"date": {"$gte": datetime(given_year, 1, 1), "$lt": datetime(given_year + 1, 1, 1)}}},
        {"$group": {"_id": {"$month": "$date"}, "count": {"$sum": 1}}}
    ])
    return list(comments_by_month)

given_year = int(input('\nEnter a year to find the total number of comments created each month - '))
print(f"\nTotal number of comments created each month in {given_year}:\n")
for month_data in total_comments_by_month_in_year(given_year):
    print(f"Month {month_data['_id']}: {month_data['count']} comments")
print('-------------------------------------------------------------------------------------------------------------------')
