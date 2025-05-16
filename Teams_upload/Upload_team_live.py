import sys
sys.path.append(r'C:/Users/lokeshwari.k/AppData/Local/Programs/Python/Python312')
import pandas as pd
import requests
from typing import Dict, List
from ZUPER import *
import random


HOST = "https://us-east-1.zuperpro.com/api/"
API = ''

geo_host = 'https://maps.googleapis.com/maps/api/geocode/json'
geo_API = 'AIzaSyBq_s_LfRdHH06w422HTJk4ZsxTCI5UUI8'
zuper_create = ZUPER(HOST, API)
zuper_create = ZUPER(HOST, API)
job_object = Jobs(zuper_create)
Emp_object = Employee(zuper_create)
team_object = Team(zuper_create)
#print(df.head())


df= pd.read_excel("D:\ROM_TECH\Teams_upload\Teams_upload_2_live.xlsx")
TEAM_COLORS = [
    "#4960a0",  # Blue
    "#2ecc71",  # Green
    "#e74c3c",  # Red
    "#f39c12",  # Orange
    "#9b59b6",  # Purple
    "#16a085",  # Teal
    "#d35400",  # Dark Orange
    "#27ae60",  # Emerald
    "#8e44ad",  # Violet
    "#2980b9"   # Dark Blue
]

# Update the team creation code
for i in range(len(df.index)):
    try:
        # Randomly select a color from the list
        if df.loc[i,'Team_uid'] == 'error found':
                
            team_color = random.choice(TEAM_COLORS)
            
            data = json.dumps({
                "team": {
                    "team_color": team_color,  # Use random color here
                    "team_name": df.loc[i, 'Team'],
                    "custom_fields": [
                        {
                            "label": "DC/SL",
                            "value": ''
                        },
                        {
                            "label": "Supervisor",
                            "value": ''
                        }
                    ]
                }
            }, default=str)
            
            df.loc[i, 'Team_uid'] = team_object.create_team(data)
            # Store the color used for reference
            df.loc[i, 'Team_Color'] = team_color
        
    except Exception as e:
        print(f"Error processing row {i}: {e}")
        df.loc[i, 'Team_uid'] = 'error found'

df.to_excel("D:\ROM_TECH\Teams_upload\Teams_upload_2_live.xlsx", index=False)
