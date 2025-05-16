import sys
sys.path.append(r'C:/Users/lokeshwari.k/AppData/Local/Programs/Python/Python312')
from ZUPER import *
from pickle import TRUE
import pandas as pd
import json
import os
from datetime import datetime

#wb = openpyxl.load_workbook('D:/customer/Zuper_CUSTOMER.xlsx')
HOST = "https://us-east-1.zuperpro.com/api/"
API = 'bca012636a18b9a51f0e8e5b71199c53'
geo_host = 'https://maps.googleapis.com/maps/api/geocode/json'
geo_API = 'AIzaSyBq_s_LfRdHH06w422HTJk4ZsxTCI5UUI8'
zuper_create = ZUPER(HOST, API)
territory_object = ServiceTerritory(zuper_create)
#for i in wb.sheetnames:


df= pd.read_excel("D:\ROM_TECH\Service_territory_team_update\Service_teritory_update_teams.xlsx")

def get_territory_teams(territory_uid):
    """Get existing teams for a territory"""
    try:
        response = requests.get(
            f"{HOST}territory/{territory_uid}",
            headers={"Authorization": f"Bearer {API}"}
        )
        
        if response.status_code == 200:
            data = response.json()
            return [team['team']['team_uid'] for team in data.get('data', {}).get('teams', [])]
    except Exception as e:
        print(f"Error getting territory teams: {str(e)}")
    return []

def get_team_uid_by_name(team_name):
    """Get team UID by name"""
    try:
        response = requests.get(
            f"{HOST}team",
            headers={'x-api-key': API,
        'Content-Type': 'application/json'}
        )
        
        if response.status_code == 200:
            teams = response.json().get('data', [])
            for team in teams:
                if team['team_name'].strip() == team_name.strip():
                    return team['team_uid']
    except Exception as e:
        print(f"Error getting team UID: {str(e)}")
    return None

def update_territory_teams(territory_uid, team_uids):
    """Update territory with new team assignments"""
    try:
        # Get existing territory details
        territory_details = get_territory_details(territory_uid)
        if not territory_details:
            print(f"Could not get territory details for {territory_uid}")
            return False

        # Prepare the complete payload with all required fields
        payload = json.dumps({
            "territory": {
                "territory_name": territory_details.get('territory_name'),
                "territory_description": territory_details.get('territory_description'),
                "territory_type": territory_details.get('territory_type', 'RADIUS'),
                "territory_radius": territory_details.get('territory_radius', {}),
                "territory_zipcodes": territory_details.get('territory_zipcodes', []),
                 "territory_coordinates": territory_details.get('territory_coordinates', []),
                #"territory_uid": territory_uid,
                "teams": [{"team_uid": uid} for uid in team_uids],
               # "owners": territory_details.get('owners', []),
               "territory_color": territory_details.get('territory_color', '#e67e22')
            }
        }, default=str)
        
        print(f"Updating territory with payload: {payload}")
        
        response = requests.put(
            f"{HOST}territory/{territory_uid}",
            headers={
                'x-api-key': API,
                'Content-Type': 'application/json'
            },
            data=payload
        )
        print(payload)
        if response.status_code == 200:
            print(f"Successfully updated territory {territory_uid}")
            return True
        else:
            print(f"Failed to update territory. Status: {response.status_code}")
            print(f"Response: {response.text}")
            return False
            
    except Exception as e:
        print(f"Error updating territory: {str(e)}")
        return False
def get_territory_details(territory_uid):
    """Get full territory details"""
    try:
        response = requests.get(
            f"{HOST}territory/{territory_uid}",
            headers={'x-api-key': API, 'Content-Type': 'application/json'}
        )
        
        if response.status_code == 200:
            return response.json().get('data', {})
        print(f"Failed to get territory details. Status: {response.status_code}")
    except Exception as e:
        print(f"Error getting territory details: {str(e)}")
    return None
# Main processing loop
for i in range(1,len(df.index)):
    print(i)
    try:

        if df.loc[i, 'Status'] == 'Failed':
            service_territory = df.loc[i, 'Service Territory']
            teams_to_assign = str(df.loc[i, 'teams_to_get_assigned']).split(',')
            
            # Get territory UID based on name
            territory_response = requests.get(
                f"{HOST}territory?count=100&page=1",
                headers={'x-api-key': API,
            'Content-Type': 'application/json'}
            )
            
            territory_uid = None
            if territory_response.status_code == 200:
                territories = territory_response.json().get('data', [])
                for territory in territories:
                    if territory['territory_name'].strip() == service_territory.strip():
                        territory_uid = territory['territory_uid']
                        print(f"Found territory UID: {territory_uid}")
                        break
            
            if territory_uid:
                # Get existing team UIDs
                existing_teams = get_territory_teams(territory_uid)
                
                # Get UIDs for new teams
                new_team_uids = []
                for team_name in teams_to_assign:
                    team_name = team_name.strip()
                    if team_name:
                        team_uid = get_team_uid_by_name(team_name)
                        if team_uid:
                            new_team_uids.append(team_uid)
                
                # Combine existing and new teams
                all_team_uids = list(set(existing_teams + new_team_uids))
                
                # Update territory
                if update_territory_teams(territory_uid, all_team_uids):
                    df.loc[i, 'Status'] = 'Success'
                else:
                    df.loc[i, 'Status'] = 'Failed'
            else:
                print(f"Territory not found: {service_territory}")
                df.loc[i, 'Status'] = 'Territory not found'
                
    except Exception as e:
        print(f"Error processing row {i}: {str(e)}")
        df.loc[i, 'Status'] = f'Error: {str(e)}'

# Save results
df.to_excel("D:\ROM_TECH\Service_territory_team_update\Service_teritory_update_teams.xlsx", index=False)
print("Process completed - results saved")
