

import sys

import requests

import json
import pandas as pd
import numpy as np
from getpass import getpass
import re 

import os
from dotenv import load_dotenv

from pymongo import MongoClient
import collections





#### Scraping Foursquare API ################################################

passwd = os.getenv("passwd")

def fsq_request(query,lat_lon,radius,category,limit):

    """returns the result for query in radius of x around a specific coordinates"""
    if category != 0:
        url = f"https://api.foursquare.com/v3/places/search?query={query}&ll={lat_lon[0]}%2C{lat_lon[1]}&radius={radius}&categories={category}&limit={limit}"
    elif category == 0:
        url = f"https://api.foursquare.com/v3/places/search?query={query}&ll={lat_lon[0]}%2C{lat_lon[1]}&radius={radius}&limit={limit}"
            
    query = query.replace(" ","%20")


    headers = {
        "accept": "application/json",
        "Authorization": passwd
    }

    response = requests.get(url, headers=headers)

    return response.json()["results"]

coord_london = [51.500152,-0.126236]
airport_london = fsq_request("airport",coord_london,30000,19040,50)
starbucks_london = fsq_request("starbucks",coord_london,1500,13035,50)
school_london = fsq_request("primary school",coord_london,1500,12057,50)
v_restaurant_london = fsq_request("vegan restaurant",coord_london,1500,13377,50)
night_club_london = fsq_request("night club",coord_london,1500,10032,50)
dog_groom_london = fsq_request("dog hairdresser",coord_london,1500,11134,50)
basketball_london = fsq_request("basketball court",coord_london,1500,18008,50)

coord_ny = [40.7549309, -73.9840195]
airport_ny = fsq_request("airport",coord_ny,30000,19040,50)
starbucks_ny = fsq_request("starbucks",coord_ny,1500,13035,50)
school_ny= fsq_request("primary school",coord_ny,1500,12057,50)
v_restaurant_ny = fsq_request("vegan restaurant",coord_ny,1500,13377,50)
night_club_ny = fsq_request("night club",coord_ny,1500,10032,50)
dog_groom_ny = fsq_request("dog hairdresser",coord_ny,1500,11134,50)
basketball_ny = fsq_request("basketball court",coord_ny,1500,18008,50)

coord_sf = [37.777720,-122.395785] 
airport_sf = fsq_request("airport",coord_sf,30000,19040,50)
starbucks_sf = fsq_request("starbucks",coord_sf,1500,13035,50)
school_sf = fsq_request("primary school",coord_sf,1500,12057,50)
v_restaurant_sf = fsq_request("vegan restaurant",coord_sf,1500,13377,50)
night_club_sf = fsq_request("night club",coord_sf,1500,10032,50)
dog_groom_sf = fsq_request("dog hairdresser",coord_sf,1500,11134,50)
basketball_sf = fsq_request("basketball court",coord_sf,1500,18008,50)

category_code_list = [19040, 13035, 12057, 13377, 10032, 11134, 18008]


#### Loading Mongo ################################################

client = MongoClient("localhost:27017")
client.list_database_names()
db = client["ironhack"]
c = db.get_collection("companies")


try:
    london_list = [ airport_london, starbucks_london, school_london, v_restaurant_london, night_club_london, dog_groom_london, basketball_london]
    london_list_flat = [j for i in london_list for j in i ]

    for i in range(len(london_list_flat)):
        london_list_flat[i]["geocodes"]["main"] = dict(collections.OrderedDict(sorted(london_list_flat[i]["geocodes"]["main"].items(),reverse=True)))
        
    with open('data/london_api.json', 'w') as fout:
        json.dump(london_list_flat, fout)

    mycol = db.create_collection("london_places")


    with open("data/london_api.json") as file:
        file_data = json.load(file)
        
    db["london_places"].insert_many(file_data)

    london_places_col = db.get_collection("london_places")
except:
    pass


try:
    sf_list = [ airport_sf, starbucks_sf, school_sf, v_restaurant_sf, night_club_sf, dog_groom_sf, basketball_sf]
    sf_list_flat = [j for i in sf_list for j in i ]

    for i in range(len(sf_list_flat)):
        sf_list_flat[i]["geocodes"]["main"] = dict(collections.OrderedDict(sorted(sf_list_flat[i]["geocodes"]["main"].items(),reverse=True)))
        
    with open('data/sf_api.json', 'w') as fout:
        json.dump(sf_list_flat, fout)

    mycol = db.create_collection("sf_places")


    with open("data/sf_api.json") as file:
        file_data = json.load(file)
        
    db["sf_places"].insert_many(file_data)

    sf_places_col = db.get_collection("sf_places")
except:
    pass


try:
    ny_list = [ airport_ny, starbucks_ny, school_ny, v_restaurant_ny, night_club_ny, dog_groom_ny, basketball_ny]
    ny_list_flat = [j for i in ny_list for j in i ]

    for i in range(len(ny_list_flat)):
        ny_list_flat[i]["geocodes"]["main"] = dict(collections.OrderedDict(sorted(ny_list_flat[i]["geocodes"]["main"].items(),reverse=True)))
        
    with open('data/ny_api.json', 'w') as fout:
        json.dump(ny_list_flat, fout)

    mycol = db.create_collection("ny_places")


    with open("data/ny_api.json") as file:
        file_data = json.load(file)
        
    db["ny_places"].insert_many(file_data)

    ny_places_col = db.get_collection("ny_places")
except:
    pass


#### Mongo Geoqueries (geo index necesarry, done in compass) ################################################

london_places_col = db.get_collection("london_places")
sf_places_col = db.get_collection("sf_places")
ny_places_col = db.get_collection("ny_places")


def type_point_mongo (list_):
    """ Mongo uses LONGITUDE first, LATITUDE second"""
    list_ = list_[::-1]
    formatted_dict_ = {"type": "Point", "coordinates": list_}
    return formatted_dict_

def close_by (collection,location, distance):

    converted_location = type_point_mongo(location)
    
    query = {"geocodes.main": 
            {"$near": 
                {"$geometry": converted_location, "$maxDistance": distance
                }}}

    proy_ = {"_id": 0}
    
    result = list(collection.find(query, proy_))
    
    print(len(result))

    return result

london_places = close_by(london_places_col,coord_london,30000)
sf_places =close_by(sf_places_col,coord_sf,30000)
ny_places =close_by(ny_places_col,coord_ny,30000)

category_id = [london_places[i]["categories"][j]["id"] for i in range(len(london_places)) for j in range(len(london_places[i]["categories"])) if london_places[i]["categories"][j]["id"] in category_code_list] 
category_name = [london_places[i]["categories"][j]["name"] for i in range(len(london_places)) for j in range(len(london_places[i]["categories"])) if london_places[i]["categories"][j]["id"] in category_code_list]
distance = [london_places[i]["distance"] for i in range(len(london_places)) for j in range(len(london_places[i]["categories"])) if london_places[i]["categories"][j]["id"] in category_code_list]
lat = [london_places[i]["geocodes"]["main"]["latitude"] for i in range(len(london_places)) for j in range(len(london_places[i]["categories"])) if london_places[i]["categories"][j]["id"] in category_code_list]
lon = [london_places[i]["geocodes"]["main"]["longitude"] for i in range(len(london_places)) for j in range(len(london_places[i]["categories"])) if london_places[i]["categories"][j]["id"] in category_code_list]
name = [london_places[i]["name"] for i in range(len(london_places)) for j in range(len(london_places[i]["categories"])) if london_places[i]["categories"][j]["id"] in category_code_list]

london_dict = {
    "category_id" : category_id,
    "category_name" : category_name,
    "distance": distance,
    "lat" : lat,
    "lon" : lon,
    "name" : name
}

london_df = pd.DataFrame(london_dict)

london_df.to_csv(r"data\london_fsq_df.csv")

category_id = [ny_places[i]["categories"][j]["id"] for i in range(len(ny_places)) for j in range(len(ny_places[i]["categories"])) if ny_places[i]["categories"][j]["id"] in category_code_list] 
category_name = [ny_places[i]["categories"][j]["name"] for i in range(len(ny_places)) for j in range(len(ny_places[i]["categories"])) if ny_places[i]["categories"][j]["id"] in category_code_list]
distance = [ny_places[i]["distance"] for i in range(len(ny_places)) for j in range(len(ny_places[i]["categories"])) if ny_places[i]["categories"][j]["id"] in category_code_list]
lat = [ny_places[i]["geocodes"]["main"]["latitude"] for i in range(len(ny_places)) for j in range(len(ny_places[i]["categories"])) if ny_places[i]["categories"][j]["id"] in category_code_list]
lon = [ny_places[i]["geocodes"]["main"]["longitude"] for i in range(len(ny_places)) for j in range(len(ny_places[i]["categories"])) if ny_places[i]["categories"][j]["id"] in category_code_list]
name = [ny_places[i]["name"] for i in range(len(ny_places)) for j in range(len(ny_places[i]["categories"])) if ny_places[i]["categories"][j]["id"] in category_code_list]

ny_dict = {
    "category_id" : category_id,
    "category_name" : category_name,
    "distance": distance,
    "lat" : lat,
    "lon" : lon,
    "name" : name
}

ny_df = pd.DataFrame(ny_dict)

ny_df.to_csv(r"data\ny_fsq_df.csv")




category_id = [sf_places[i]["categories"][j]["id"] for i in range(len(sf_places)) for j in range(len(sf_places[i]["categories"])) if sf_places[i]["categories"][j]["id"] in category_code_list] 
category_name = [sf_places[i]["categories"][j]["name"] for i in range(len(sf_places)) for j in range(len(sf_places[i]["categories"])) if sf_places[i]["categories"][j]["id"] in category_code_list]
distance = [sf_places[i]["distance"] for i in range(len(sf_places)) for j in range(len(sf_places[i]["categories"])) if sf_places[i]["categories"][j]["id"] in category_code_list]
lat = [sf_places[i]["geocodes"]["main"]["latitude"] for i in range(len(sf_places)) for j in range(len(sf_places[i]["categories"])) if sf_places[i]["categories"][j]["id"] in category_code_list]
lon = [sf_places[i]["geocodes"]["main"]["longitude"] for i in range(len(sf_places)) for j in range(len(sf_places[i]["categories"])) if sf_places[i]["categories"][j]["id"] in category_code_list]
name = [sf_places[i]["name"] for i in range(len(sf_places)) for j in range(len(sf_places[i]["categories"])) if sf_places[i]["categories"][j]["id"] in category_code_list]

sf_dict = {
    "category_id" : category_id,
    "category_name" : category_name,
    "distance": distance,
    "lat" : lat,
    "lon" : lon,
    "name" : name
}

sf_df = pd.DataFrame(sf_dict)

sf_df.to_csv(r"data\sf_fsq_df.csv")