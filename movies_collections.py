import pymongo
from datetime import datetime

mongodb_uri = "mongodb://localhost:27017/"
client = pymongo.MongoClient(mongodb_uri)
db = client["mflix_db"]

# MOVIES COLLECTION
movies_collection = db['movies']

# Task 1: Find top N movies with the highest IMDB rating
def top_n_movies_highest_imdb_rating(N):
    top_movies = movies_collection.find().sort("imdb.rating", -1).limit(N)
    return list(top_movies)

N = int(input('\nEnter N, to find top N movies with the highest IMDB rating - '))
print('\nTop', N,' movies with the highest IMDB rating :')
for movie in top_n_movies_highest_imdb_rating(N):
    print(movie['title'])
print('-------------------------------------------------------------------------------------------------------------------')

# Task 2: Find top N movies with the highest IMDB rating in a given year
def top_n_movies_highest_imdb_rating_given_year(N, year):
    top_movies = movies_collection.find({"year": year}).sort("imdb.rating", -1).limit(N)
    return list(top_movies)

year = int(input('\nEnter year, to find top N movies with the highest IMDB rating in given year - '))
N = int(input('\nEnter N - '))
print('\nTop ', N,' movies with the highest IMDB rating in a given ',year,' :')
for movie in top_n_movies_highest_imdb_rating_given_year(N, year):
    print(movie['title'],' - ',movie['imdb']['rating'])
print('-------------------------------------------------------------------------------------------------------------------')


# Task 3: Find top N movies with the highest IMDB rating and number of votes > 1000
def top_n_movies_highest_imdb_rating_votes_gt_1000(N):
    top_movies = movies_collection.find({"imdb.votes": {"$gt": 1000}}).sort("imdb.rating", -1).limit(N)
    return list(top_movies)

N = int(input('\nEnter N, to find top N movies with the highest IMDB rating and number of votes > 1000 - '))
print('\nTop N movies with the highest IMDB rating and number of votes > 1000')
for movie in top_n_movies_highest_imdb_rating_votes_gt_1000(5):
    print('Movie: ',movie['title'],', Imdb Rating: ',movie['imdb']['rating'],', Votes: ',movie['imdb']['votes'])
print('-------------------------------------------------------------------------------------------------------------------')


# Task 4: Find top N movies with title matching a given pattern sorted by highest tomatoes ratings
def top_n_movies_title_matching_pattern(N, pattern):
    regex_pattern = {"$regex": pattern, "$options": "i"}  # Case-insensitive search
    top_movies = movies_collection.find({"title": regex_pattern}).sort("tomatoes.viewer.rating", -1).limit(N)
    return list(top_movies)

pattern = input('\nEnter the pattern to find to N movies matching title sorted by highest tomatoes ratings - ')
n = int(input('Enter N - '))
print("\nTop", N ,"movies with title matching pattern", pattern, "sorted by highest tomatoes ratings:")
for movie in top_n_movies_title_matching_pattern(N,pattern):
    print(movie['title'],', Tomatoes rating -',movie['tomatoes']['viewer']['rating'])
print('-------------------------------------------------------------------------------------------------------------------')

# Task 5: Find top N directors who created the maximum number of movies:
def top_n_directors_most_movies(N):
    top_directors = movies_collection.aggregate([
        {"$unwind": "$directors"},
        {"$group": {"_id": "$directors", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": N}
    ])
    return list(top_directors)

N = int(input('\nEnter N, to find top N directors who created the maximum number of movies - '))
print("\nTop",N," directors who created the maximum number of movies:")
for director in top_n_directors_most_movies(N):
    print(director['_id'],' - ',director['count'])
print('-------------------------------------------------------------------------------------------------------------------')


# Task 6: Find top N directors who created the maximum number of movies in a given year:
def top_n_directors_most_movies_in_given_year(N,year):
    top_directors = movies_collection.aggregate([
        {"$match": {"year": year}},
        {"$unwind": "$directors"},
        {"$group": {"_id": "$directors", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": N}
    ])
    return list(top_directors)

year = int(input('\nEnter the year - '))
N = int(input('\nEnter N - '))
print("\nTop",N," directors who created the maximum number of movies in the ",year," :")
for director in top_n_directors_most_movies_in_given_year(N,year):
    print(director['_id'],' - ',director['count'])   

print('-------------------------------------------------------------------------------------------------------------------')

# Task 7: Find top N directors who created the maximum number of movies for a given genre: 
def top_n_directors_most_movies_in_given_genre(N,genre):
    top_directors = movies_collection.aggregate([
        {"$unwind": "$directors"},
        {"$match": {"genres": genre}},
        {"$group": {"_id": "$directors", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": N}
    ])
    return list(top_directors)

genre = input('\nEnter genre - ')
N = int(input('\nEnter N - '))
print("\nTop ",N," directors who created the maximum number of movies for the genre ",genre," :")
for director in top_n_directors_most_movies_in_given_genre(N,genre):
    print(director['_id'],' - ',director['count']) 

print('-------------------------------------------------------------------------------------------------------------------')

# Task 8: Find top N actors who starred in the maximum number of movies:
def top_n_actors_most_movies(N):
    top_actors = movies_collection.aggregate([
        {"$unwind": "$cast"},
        {"$group": {"_id": "$cast", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": N}
    ])
    return list(top_actors)

N = int(input('\nEnter N - '))
print('\nTop N actors who starred in the maximum number of movies')
for actor in top_n_actors_most_movies(N):
    print(actor['_id'],' - ',actor['count'])


print('-------------------------------------------------------------------------------------------------------------------')

# Task 9: Find top N actors who starred in the maximum number of movies in a given year:
def top_n_actors_most_movies_in_year(N,year):
    top_actors = movies_collection.aggregate([
        {"$match": {"year": year}},
        {"$unwind": "$cast"},
        {"$group": {"_id": "$cast", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": N}
    ])
    return list(top_actors)  

year = int(input('\nEnter year - '))
N = int(input('\nEnter N - '))
print('\nTop ',N,' actors who starred in the maximum number of movies in the ',year,':') 
for actor in top_n_actors_most_movies_in_year(N,year):
    print(actor['_id'],' - ',actor['count'])

print('-------------------------------------------------------------------------------------------------------------------')

# Task 10: Find top N actors who starred in the maximum number of movies for a given genre:
def top_n_actors_most_movies_in_genre(N,genre):
    top_actors = movies_collection.aggregate([
        {"$match": {"genres": genre}},
        {"$unwind": "$cast"},
        {"$group": {"_id": "$cast", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": N}
    ])
    return list(top_actors) 

genre = input('\nEnter genre - ')
N = int(input('\nEnter N - '))
print('\nTop ',N,' actors who starred in the maximum number of movies for genre ',genre,':') 
for actor in top_n_actors_most_movies_in_genre(N,genre):
    print(actor['_id'],' - ',actor['count'])  

print('-------------------------------------------------------------------------------------------------------------------')

# Task 11: Find top N movies for each genre with the highest IMDB rating:
def top_n_movie_in_genre_highest_imdb_rating(N):
    top_movies = movies_collection.aggregate([
        {"$unwind": "$genres"},
        {"$sort": {"imdb.rating": -1}},
        {"$group": {
            "_id": "$genres",
            "movies": {"$push": "$$ROOT"}
        }},
        {"$project": {
            "_id": 0,
            "genre": "$_id",
            "top_movies": {"$slice": ["$movies", N]}
        }}
    ])
    return list(top_movies)

N = int(input('\nEnter N, to find top N movies for each genre with the highest IMDB rating - '))
print('\nTop N movies for each genre with the highest IMDB rating:')
for movie in top_n_movie_in_genre_highest_imdb_rating(N):
    print('Genres : ',movie['genre'])
    for i in range(N):
        if i<len(movie['top_movies']):
            print('Movie : ',movie['top_movies'][i]['title'])

print('-------------------------------------------------------------------------------------------------------------------')

