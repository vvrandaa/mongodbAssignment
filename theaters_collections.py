import pymongo
from datetime import datetime

mongodb_uri = "mongodb://localhost:27017/"
client = pymongo.MongoClient(mongodb_uri)
db = client["mflix_db"]

# THEATERS COLLECTION 
theaters_collection = db['theaters']

# Task 1: Top 10 cities with the maximum number of theaters:
def top_10_cities_max_theaters():
    top_cities = theaters_collection.aggregate([
        {"$group": {"_id": "$location.address.city", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 10}
    ]) 
    return list(top_cities) 

print('\nTop 10 cities with the maximum number of theaters:')
for city in top_10_cities_max_theaters():
    print(city['_id'],' - ',city['count'])
print('-------------------------------------------------------------------------------------------------------------------')

# Task 2: Top 10 theaters nearby given coordinates:
def top_10_theater_near_coordinates(coordinates):
    theaters_collection.create_index([("location.geo.coordinates", "2dsphere")])
    top_theaters = theaters_collection.aggregate([
        {"$geoNear": {
            "near": {"type": "Point", "coordinates": coordinates},
            "distanceField": "distance",
            "spherical": True
        }},
        {"$limit": 10}
    ]) 
    return list(top_theaters) 

print('\nTop 10 cities with the maximum number of theaters:')
for theater in top_10_theater_near_coordinates([-93.24565, 44.85466]):
    print(theater['theaterId'])