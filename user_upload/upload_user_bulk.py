import sys
sys.path.append(r'C:/Users/lokeshwari.k/AppData/Local/Programs/Python/Python312')
import pandas as pd
import requests
from typing import Dict, List
from ZUPER import *

HOST = "https://stagingv2.zuperpro.com/api/"
API = '1c30dabfef2ab006fefe4f6f17292884'

geo_host = 'https://maps.googleapis.com/maps/api/geocode/json'
geo_API = 'AIzaSyBq_s_LfRdHH06w422HTJk4ZsxTCI5UUI8'
zuper_create = ZUPER(HOST, API)
zuper_create = ZUPER(HOST, API)
job_object = Jobs(zuper_create)
Emp_object = Employee(zuper_create)
#print(df.head())

def get_headers(api_key):
    return {
        'Content-Type': 'application/json',
        'x-api-key': api_key
    }

def get_access_roles(api_key: str) -> Dict:
    headers = get_headers(api_key)
    url = f"{HOST}access_role?count=50&page=1"
    try:
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            print(f"Error fetching access roles: {response.status_code}")
            return {}
        data = response.json()
        roles = data.get('data', [])
        return roles
    except Exception as e:
        print(f"Error fetching access roles: {e}")
        return []

def get_existing_teams(api_key: str) -> Dict:
    headers = get_headers(api_key)
    all_teams = {}
    page = 1
    page_size = 100
    try:
        while True:
            url = f"{HOST}teams/summary?page={page}&count={page_size}&sort=ASC&sort_by=team_name"
            response = requests.get(url, headers=headers)
            if response.status_code != 200:
                print(f"Error fetching teams page {page}: {response.status_code}")
                break
            data = response.json()
            if data.get('type') != 'success':
                print(f"Error in API response for page {page}: {data.get('message', '')}")
                break
            teams = data.get('data', [])
            if not teams:
                break
            for team in teams:
                all_teams[team['team_name']] = team['team_uid']
            if len(teams) < page_size:
                break
            page += 1
        return all_teams
    except Exception as e:
        print(f"Error fetching teams: {e}")
        return {}

def get_role_id(user_role: str) -> int:
    role_mapping = {
        'Team Leader': 2,
        'Administrator': 1,
        'Field Executive': 3
    }
    return role_mapping.get(user_role, 3)  # Default to Field Executive (3)

# --- Load all sheets ---
all_sheets = pd.read_excel("D:/ROM_TECH/user_upload_staging - Copy.xlsx", sheet_name=None, engine='openpyxl')
print(f"Sheets found: {list(all_sheets.keys())}")

api_roles = get_access_roles(API)
existing_teams = get_existing_teams(API)

# To save modified sheets
updated_sheets = {}

for sheet_name, df in all_sheets.items():
    df = df.fillna('')  # Handle missing values
    print(f"\nProcessing sheet: {sheet_name} | Total users: {len(df)}")
    
    for i in range(len(df)):
        try:
            print(f"Processing row: {i}")
            full_name = str(df.loc[i, 'User Name']).strip()
            name_parts = full_name.split(' ', 1)
            first_name = name_parts[0]
            last_name = name_parts[1] if len(name_parts) > 1 else ''
            
            user_role = str(df.loc[i, 'User Role']).strip()
            role_id = get_role_id(user_role)
            
            user_access_role = str(df.loc[i, 'User Access Role']).strip()
            access_role_uid = None
            for role in api_roles:
                if role['role_name'] == user_access_role:
                    access_role_uid = role['access_role_uid']
                    break
            if not access_role_uid:
                print(f"Warning: No matching access role found for {user_access_role}")
            
            team_uids = []
            if df.loc[i, 'Team Name']:
                teams = [t.strip() for t in str(df.loc[i, 'Team Name']).split(',')]
                for team in teams:
                    if team in existing_teams:
                        team_uids.append(existing_teams[team])
                    else:
                        print(f"Team not found: {team}")

            data = json.dumps({
                "user": {
                    "role_id": role_id,
                    "emp_code": df.loc[i, 'Employee Code'],
                    "first_name": first_name,
                    "last_name": last_name,
                    "email": df.loc[i, 'User Email'],
                    "designation": df.loc[i, 'ROMtech Role'],
                    "access_role": access_role_uid,
                    "password": "Zuper@123",
                    "confirm_password": "Zuper@123",
                    "work_phone_number": '',
                    "mobile_phone_number": '',
                    "team_uid": team_uids[0] if team_uids else None,
                    "custom_fields": [{"label":'Supervisor', "value": df.loc[i, 'Supervisor']}],
                    "skillsets": [],
                },
                "work_hours": [{
                        "day": day,
                        "is_enabled": False,
                        "start_time": '',
                        "end_time": ''
                    } for day in ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]
                ],
                "send_welcome_email": bool(0)
            }, default=str)

            print(f"Creating user with data: {data}")
            df.loc[i, 'Employee_UID'] = Emp_object.create_user(data)

            # Assign additional teams
            if len(team_uids) > 1:
                for team_uid in team_uids[1:]:
                    assign_url = f"{HOST}team/assign"
                    assign_data = json.dumps({
                        "team_uid": team_uid,
                        "user_uid": df.loc[i, 'Employee_UID']
                    })
                    r = requests.post(assign_url, headers={'x-api-key': API, 'Content-Type': 'application/json'}, data=assign_data)
                    print(f"Assigned additional team: {r.json()}")
        
        except Exception as e:
            print(f"Error processing row {i}: {e}")
            df.loc[i, 'Employee_UID'] = 'error found'
    
    updated_sheets[sheet_name] = df  # Save the modified DataFrame

# --- Save all updated sheets back ---
with pd.ExcelWriter("D:/ROM_TECH/user_upload_staging_upload.xlsx") as writer:
    for sheet_name, df in updated_sheets.items():
        df.to_excel(writer, sheet_name=sheet_name, index=False)

print("All sheets processed and saved!")
