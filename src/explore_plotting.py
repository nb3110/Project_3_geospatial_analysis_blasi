
import folium
from folium import Choropleth, Circle, Marker, Icon, Map
from folium.plugins import HeatMap, MarkerCluster
import pandas as pd
import math

def comp_score(df):

    df =pd.DataFrame({
    "tag":["desing_tag","funding_tag"],
    "min_dist" : [df[df["desing_tag"]==True]["distance"].min(),df[df["funding_tag"]==True]["distance"].min()]
    })

    df["score"] = df["min_dist"]*(0.95/7)
    return df

def cat_weight(row):
    tier1_cat = ["International Airport" ,"Primary and Secondary School","Vegan and Vegetarian Restaurant","Coffee Shop","Night Club"]
    if row['category_name'] in (tier1_cat):
        return row['distance']*(0.95/7)
    else:
        return row['distance']*(0.05/2)







def haversine(coord1, coord2):
 
    # Coordinates in decimal degrees (e.g. 2.89078, 12.79797)
    lon1, lat1 = coord1
    lon2, lat2 = coord2

    R = 6371000  # radius of Earth in meters
    phi_1 = math.radians(lat1)
    phi_2 = math.radians(lat2)

    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)

    a = math.sin(delta_phi / 2.0) ** 2 + math.cos(phi_1) * math.cos(phi_2) * math.sin(delta_lambda / 2.0) ** 2
    
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    meters = R * c  # output distance in meters
    km = meters / 1000.0  # output distance in kilometers

    return round(meters, 3)




def heatmap_top3(location,dataframe,condition_filter):
    map_ = Map(location=location, zoom_start = 12)

    filtered_df =dataframe[condition_filter]
    group = folium.FeatureGroup(name=f"Funding {filtered_df.shape[0]}")

    HeatMap(data = filtered_df[["office_lat", "office_lon"]]).add_to(map_)
    group.add_to(map_)
    return map_





def full_plot(lat,lon,df):
    map_plot = Map(location=[lat, lon], zoom_start = 15)

    for index, row in df.iterrows():
        
        #1. MARKER without icon
        mark_type = {"location": [row["lat"], row["lon"]], "tooltip": row["category_name"]}
        
        #2. Icon
        if row["category_name"] == 'Pet Grooming Service':        
            icon = Icon (
                color="blue",
                opacity = 0.6,
                prefix = "fa",
                icon="paw",
                icon_color = "white"
            )
        elif row["category_name"] == "Coffee Shop":
            icon = Icon (
                color="white",
                opacity = 0.6,
                prefix = "fa",
                icon="coffee",
                icon_color = "green"
            )
        elif row["category_name"] == "Vegan and Vegetarian Restaurant":
            icon = Icon (
                color="green",
                opacity = 0.6,
                prefix = "fa",
                icon="leaf",
                icon_color = "white"
            )
        elif row["category_name"] == "Night Club":
            icon = Icon (
                color="black",
                opacity = 0.6,
                prefix = "fa",
                icon="beer",
                icon_color = "yellow"
            )
        elif row["category_name"] == "Primary and Secondary School":
            icon = Icon (
                color="darkblue",
                opacity = 0.6,
                prefix = "fa",
                icon="child",
                icon_color = "white"
            )
        elif row["category_name"] == "Basketball Court":
            icon = Icon (
                color="black",
                opacity = 0.6,
                prefix = "fa",
                icon="basketball",
                icon_color = "orange"
            )
        elif row["category_name"] == "International Airport":
            icon = Icon (
                color="red",
                opacity = 0.6,
                prefix = "fa",
                icon="plane",
                icon_color = "white"
            )    
        elif row["category_name"] == "design/Funding1M":
            icon = Icon (
                color="darkpurple",
                opacity = 0.6,
                prefix = "fa",
                icon="briefcase",
                icon_color = "white"
            )
        else:
            icon = Icon (
                color="blue",
                opacity = 0.6,
                prefix = "fa",
                icon="question",
                icon_color = "brown",
                icon_size=(14, 14)
            )
        #3. Marker
        new_marker = Marker(**mark_type, icon = icon, radius = 2)
        
        #4. Add the Marker
        new_marker.add_to(map_plot)

    folium.Circle([lat, lon], radius=500).add_to(map_plot)
    folium.Circle([lat, lon], radius=1000).add_to(map_plot)
    folium.Circle([lat, lon], radius=10000).add_to(map_plot)

    return map_plot