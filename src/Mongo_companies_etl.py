import os
from dotenv import load_dotenv

from pymongo import MongoClient
import pandas as pd

import re


load_dotenv() 


#### Data extraction  ############

client = MongoClient("localhost:27017")
client.list_database_names()
db = client["ironhack"]
c = db.get_collection("companies")


filter = {"tag_list": {"$regex": "(design|games_video|software|web|internet|digital|gaming|games|animation|UX|UI)"}} 
projection ={"name": 1, "offices":1,"tag_list":1,"total_money_raised":1}

pipeline = [
    {"$match": filter},
    {"$project": projection},
    {"$unwind": "$offices"}
]

complete_dict = list(c.aggregate(pipeline))



comp_id = [str(complete_dict[i]["_id"]) for i in range(len(complete_dict))]
comp_name = [complete_dict[i]["name"] for i in range(len(complete_dict))]
comp_city = [complete_dict[i]["offices"]["city"] for i in range(len(complete_dict))]
comp_country = [complete_dict[i]["offices"]["country_code"] for i in range(len(complete_dict))]
comp_money_raised = [complete_dict[i]["total_money_raised"] for i in range(len(complete_dict))]
comp_tags = [complete_dict[i]["tag_list"]for i in range(len(complete_dict))]
comp_office_lat = [complete_dict[i]["offices"]["latitude"] for i in range(len(complete_dict))]
comp_office_lon = [complete_dict[i]["offices"]["longitude"] for i in range(len(complete_dict))]



comp_dict= pd.DataFrame({
    "id": comp_id,
    "name" : comp_name,
    "city" : comp_city,
    "country" : comp_country,
    "funds_raised" : comp_money_raised,
    "tags": comp_tags,
    "office_lat": comp_office_lat,
    "office_lon":comp_office_lon
})

comp_df =pd.DataFrame(comp_dict)


#### funding values adjustment ############



def convert_value(value):

    amount = float(re.sub(r'[^\d.]', '', value))
    unit = value[-1]

    if unit == 'M':
        amount *= 1000000
    elif unit == 'B':
        amount *= 1000000000
    elif unit == 'k':
        amount *= 1000
    else:
        amount
        
    return amount



comp_df["funds_raised_float"] = comp_df["funds_raised"].apply(lambda x: convert_value(x))
comp_df["currency"] = comp_df["funds_raised"].apply(lambda x: x[0])


ccy = pd.DataFrame({
"sym" : ['$', '€', '£', '¥'],
"fx_rate" : [1,1.07,1.21,0.0076]
})


comp_df = comp_df.merge(ccy,how="left", left_on="currency", right_on="sym")

comp_df["funds_raised_usd"] = comp_df["funds_raised_float"]*comp_df["fx_rate"] # USD equivalent of funds raised
comp_df["desing_tag"] = comp_df["tags"].apply(lambda x: "design" in x) # design tag boolean
comp_df["gaming_tag"] = comp_df["tags"].apply(lambda x: "gaming" in x) # gaming tag boolean
comp_df["funding_tag"] = comp_df["funds_raised_usd"].apply(lambda x: x>1000000) # funding 1M tag


comp_df.drop(["funds_raised","sym","id"],axis=1,inplace=True) # dropping unnecesary columns
comp_df = comp_df[~comp_df["office_lat"].isna()] # filtering for offices with coordinates
comp_df = comp_df.drop_duplicates() #dropping duplicates
comp_df.to_csv("data\companies_df.csv")