import sys
sys.path.append(r'C:/Users/lokeshwari.k/AppData/Local/Programs/Python/Python312')
from ZUPER import *
from pickle import TRUE
import pandas as pd
import json
import moment
from datetime import datetime, timedelta

#wb = openpyxl.load_workbook('D:/customer/Zuper_CUSTOMER.xlsx')
HOST = "https://stagingv2.zuperpro.com/api/"
API = ''
geo_host = 'https://maps.googleapis.com/maps/api/geocode/json'
geo_API = 'AIzaSyBq_s_LfRdHH06w422HTJk4ZsxTCI5UUI8'
zuper_create = ZUPER(HOST, API)
cust_object = Customer(zuper_create)
Org_object = Organization(zuper_create)
Prop_object = Property(zuper_create)
field_object = Misc(zuper_create)
geo_create = ZUPER(geo_host, geo_API)
geo_object = Misc(geo_create)
price_obj = PartsServices(zuper_create)
#for i in wb.sheetnames:
df = pd.read_excel("D:\ROM_TECH\project_upload\Sample_project_upload_0805.xlsx")
df = df.fillna('')

def get_customer_details(api_key: str, patient_id: str) -> dict:
    headers = {
        'Content-Type': 'application/json',
        'x-api-key': api_key
    }
    
    payload = {
        "limit": 10,
        "page": 1,
        "filter_rules": [{
            "displayKeyValue": [patient_id],
            "key": "Romtech_Patient_ID",
            "operator": "EQUAL_TO",
            "field_type": "TEXT",
            "value": patient_id,
            "module": "CUSTOMER",
            "type": "custom_field"
        }],
        
    }
    
    try:
        url = f"{HOST}v2/customers/filter"
        response = requests.post(url, headers=headers, json=payload)
        if response.status_code != 200:
            print(f"Error fetching customer: {response.status_code}")
            return None
        data = response.json()
        if data.get('type') != 'success' or not data.get('data'):
            return None
        customer = data['data'][0]
        return {
            'customer_uid': customer['customer_uid'],
            'address': customer.get('customer_address', {}),
            'billing_address': customer.get('customer_billing_address', {})
        }
        
    except Exception as e:
        print(f"Error finding customer: {str(e)}")
        return None
CATEGORY_MAPPING = {
    'Cardiac': '25323aee-5191-495a-b42b-f0c85393a4fc',
    'Orthopedic': 'd64fd8e1-f2d5-4653-a908-25fa901b78dc'
}

JOB_TEAM_MAPPING = {
    'Delivery': 'Zuper Team for Delivery',
    'Onboarding': 'Zuper Team for Onboarding',
    'Pickup': 'Zuper Team for Pickup'
}

def get_team_members(api_key: str, team_name: str, for_project: bool = False) -> list:
    """Get active team members with correct structure for projects vs jobs"""
    headers = {
        'Content-Type': 'application/json',
        'x-api-key': api_key
    }
    try:
        # Get team summary to get team_uid
        url = f"{HOST}teams/summary?count=100&page=1&sort=DESC&sort_by=created_at&filter.keyword={team_name}"
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            return []
            
        team_data = response.json()
        if not team_data.get('data'):
            return []
            
        team_uid = team_data['data'][0]['team_uid']
        
        # Get team details with members
        detail_url = f"{HOST}team/{team_uid}"
        detail_response = requests.get(detail_url, headers=headers)
        if detail_response.status_code != 200:
            return []
            
        users = detail_response.json()['data']['users']
        
        # Format active users with correct structure based on usage
        assigned_users = []
        for user in users:
            if user['is_active']:
                if for_project:
                    # Project structure uses "team" and "user"
                    assigned_users.append({
                        "team": team_uid,
                        "user": user['user_uid']
                    })
                else:
                    # Job structure uses "team_uid" and "user_uid"
                    assigned_users.append({
                        "team_uid": team_uid,
                        "user_uid": user['user_uid']
                    })
                
        print(f"Found {len(assigned_users)} active users in team {team_name}")
        return assigned_users
        
    except Exception as e:
        print(f"Error getting team members: {str(e)}")
        return []
project_custom_fields = field_object.get_custom_field('PROJECT')
job_custom_fields = field_object.get_custom_field('JOB')
CUSTOM_FIELD_MAPPING = {
    'surgery_date': 'Surgery Date',
    'actual_delivery_date': 'actual_delivery_date',
    'RCM_Status': 'RCM Status',
    'actual_length_of_need': 'Length of Need',
    'date_of_birth': 'Date of Birth',
    'date_of_birth': 'Date Of Birth',
    'cpt_code': 'CPT',
    'ic10_code': 'ICD10',
    'romtech_portal_id': 'Portal ID',
    'rtm': 'RTM',
    'medicare': 'Medicare',
    'estimate_date': 'Order Date',
    'rental_business_type':'Order Type',
    'rental_order_number':'Rental Number',
    'Scheduled_Initial_Start_Date': 'Scheduled_Initial_Start_Date',
    'Last_Session_Date': 'Treatment End Date',
    'physician_npi': 'Physician NPI',
    'physician_name': 'Physician',
    'organisation_name': 'Referring Organization',
    'Patient_Prescriber_Specific_Information': 'Doctor Notes',
    'Study_Group_Status': 'Participating Studies',
    'custom_protocol_description': 'Custom Protocol Description',
    'DME': 'DME',
    'gender': 'Patient Gender',
    'order_owner': 'Order Owner (DME/ROMtech)',
    'Onboarded Date': 'Onboarded Date',
    'ECG PATCH WITH CHARGER BASE (REV 1)': 'ECG PATCH WITH CHARGER BASE (REV 1)',
    'PULSE OXIMETER WRIST-MOUNT (REV 1)': 'PULSE OXIMETER WRIST-MOUNT (REV 1)',
    'BLOOD PRESSURE MONITOR STANDARD (REV 1)': 'BLOOD PRESSURE MONITOR STANDARD (REV 1)',
    'CARDIAC PATIENT HUB': 'CARDIAC PATIENT HUB',
    'SAMSUNG GALAXY A14 PROVISIONED (REV 1)': 'SAMSUNG GALAXY A14 PROVISIONED (REV 1)',
    'ACCUANGLE ASSEMBLY (R5)': 'ACCUANGLE ASSEMBLY (R5)',
    'ACCUANGLE ASSEMBLY (R6)': 'ACCUANGLE ASSEMBLY (R6)',
    'ACCUANGLE ASSEMBLY (R7)': 'ACCUANGLE ASSEMBLY (R7)',
    'PORTABLE CONNECT FOR LEASE': 'PORTABLE CONNECT FOR LEASE'
}

# Update the prepare_custom_fields function
def prepare_custom_fields(custom_fields, df, row_index):
    """Helper function to prepare custom fields with mapping"""
    df_custom_field = []
    for field in custom_fields:
        temp = {
            'label': field['field_name'],
            'value': '',
            'type': field['field_type'],
            'hide_to_fe': field['hide_to_fe']
        }
        try:
            temp['group_name'] = field['group']['group_name']
            temp['group_uid'] = field['group']['group_uid']
        except KeyError:
            pass
        
        # Look for mapped column name in dataframe
        zuper_field_name = field['field_name']
        excel_field_name = next((k for k, v in CUSTOM_FIELD_MAPPING.items() if v == zuper_field_name), None)
        
        if excel_field_name and excel_field_name in df.columns:
            value = df.loc[row_index, excel_field_name]
            
            # Special handling for Order Type field
            if excel_field_name == 'rental_business_type':
                if str(value).lower() == 'orthopedic':
                    temp['value'] = 'Ortho'
                else:
                    temp['value'] = str(value)
            else:
                # Handle other fields as before
                if pd.notna(value) and value != '':
                    try:
                        if isinstance(value, str) and ('00:00:00' in value or '-' in value):
                            date_value = pd.to_datetime(value)
                            temp['value'] = date_value.strftime('%Y-%m-%d')
                        elif isinstance(value, (datetime, pd.Timestamp)):
                            temp['value'] = value.strftime('%Y-%m-%d')
                        elif pd.api.types.is_numeric_dtype(type(value)):
                            if float(value).is_integer():
                                temp['value'] = str(int(float(value)))
                            else:
                                temp['value'] = str(float(value))
                        else:
                            temp['value'] = str(value)
                    except:
                        temp['value'] = str(value)
                else:
                    temp['value'] = ''
            
        df_custom_field.append(temp)
    return df_custom_field
JOB_CATEGORIES = {
    'Delivery': '178b97e7-6a37-41b8-81d3-3c3157a15b15',
    'Pickup': 'a1aeac8d-6b85-41de-8274-c87e70b4bea5',
    'Onboarding': '4e03ee22-31ab-45bd-a971-048767a10566',
    'Service_Call': '4694b2b9-fc2f-43a7-bffe-dc2d84f0415e'
}

PROJECT_STATUS_MAPPING = {
    'Delivered': {
        'Ortho Order': 'b125d72a-c963-4bad-8aa8-b0b615ad77b9',  # Delivered status UID
        'Cardiac Order': '4923eaf5-49ce-4b40-af9b-5eef369150be'
    },
    'Delivered and Onboarded': {
        'Ortho Order': '1d17df41-4243-45b3-9393-9e8447d232bb',  # Onboarded status UID
        'Cardiac Order': '016d4611-2716-4c71-851d-f522228b307e'
    },
    
    'Confirmed': {
        'Ortho Order': '45291fed-0068-444d-bb02-b2700d67623d',  # Confirmed status UID
        'Cardiac Order': '22ac44ff-a172-4c26-824b-9aa8392d1467'
    }
}

def get_all_teams_for_project(df, i):
    """Get all available teams from all three columns for project"""
    team_columns = [
        'Zuper Team for Delivery',
        'Zuper Team for Onboarding',
        'Zuper Team for Pickup'
    ]
    
    project_assigned_users = []
    for column in team_columns:
        if column in df.columns:
            team_name = str(df.loc[i, column]).strip()
            if team_name:
                team_users = get_team_members(API, team_name, for_project=True)  # Note for_project=True
                if team_users:
                    project_assigned_users.extend(team_users)
                    print(f"Added {len(team_users)} users from {team_name}")
                else:
                    print(f"No active users found in team {team_name}")
    
    # Update pair tuple to use project keys
    unique_users = []
    seen_pairs = set()
    for user in project_assigned_users:
        pair = (user['team'], user['user'])  # Use project keys
        if pair not in seen_pairs:
            unique_users.append(user)
            seen_pairs.add(pair)
    
    return unique_users

PRODUCT_GROUPS = {
    'Cardiac': {
        'group_uid': '47e0880d-45ff-43c3-9065-617894840585',
        'group_name': 'Cardiac Group'
    },
    'Orthopedic': {
        'group_uid': '21937f30-e88d-4c96-a63c-54ff39599ff5',
        'group_name': 'Orthopedic Group'
    }
}

def get_product_group_items(api_key: str, group_uid: str) -> list:
    """Get products for a group"""
    headers = {
        'Content-Type': 'application/json',
        'x-api-key': api_key
    }
    
    url = f"{HOST}product/group/{group_uid}"
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        data = response.json()
        if data.get('type') == 'success':
            return [{
                'product_uid': product['product']['product_uid'],
                'product_name': product['product']['product_name'],
                 'quantity': 1,
                'product_group_uid': group_uid
            } for product in data['data']['associated_products']]
    return []

def get_team_for_type(df, i, job_type):
    """Get team based on job type, returns empty list if no team found"""
    team_column = JOB_TEAM_MAPPING.get(job_type)
    if not team_column:
        print(f"No team mapping found for type: {job_type}")
        return []
    
    if team_column not in df.columns:
        print(f"Team column {team_column} not found")
        return []
    
    team_name = str(df.loc[i, team_column]).strip()
    if not team_name:
        print(f"No team specified in {team_column} for row {i}")
        return []
    
    assigned_users = get_team_members(API, team_name)
    if not assigned_users:
        print(f"No active users found in team {team_name}")
        return []
        
    return assigned_users
def create_job(api_key, project_uid, customer_details, job_category, formatted_date, df, i, job_cf):
    rental_type = str(df.loc[i, 'rental_business_type']).strip()
    product_group = PRODUCT_GROUPS.get(rental_type)
    products = []
    if job_category == 'Delivery':
        rental_type = str(df.loc[i, 'rental_business_type']).strip()
        product_group = PRODUCT_GROUPS.get(rental_type)
        if product_group:
            products = get_product_group_items(api_key, product_group['group_uid'])
            print(f"Adding {len(products)} products to delivery job")

    
    assigned_users = get_team_for_type(df, i, job_category)
    if not assigned_users:
        print(f"Creating {job_category} job without team assignment")
    suffix = get_job_title_suffix(job_category)
    job_title = f"{df.loc[i,'rental_order_number']} - {suffix}"

    job_data = json.dumps({
        "job": {
            "customer_uid": customer_details['customer_uid'],
            "job_description": "",
            "customer_billing_address": {
                "street": customer_details['billing_address'].get('street', ''),
                "city": customer_details['billing_address'].get('city', ''),
                "state": customer_details['billing_address'].get('state', ''),
                "zip_code": customer_details['billing_address'].get('zip_code', ''),
                "geo_cordinates": customer_details['billing_address'].get('geo_cordinates', [])
            },
            "due_date": formatted_date,
            "products": products,
            #"property": df.loc[i,'property_uid'],
            "project": project_uid,
            "assigned_to":assigned_users,
            "job_tags": ["Migration Job_0805" ],
            "customer_address": {
                "street": customer_details['address'].get('street', ''),
                "city": customer_details['address'].get('city', ''),
                "state": customer_details['address'].get('state', ''),
                "zip_code": customer_details['address'].get('zip_code', ''),
                "geo_cordinates": customer_details['address'].get('geo_cordinates', [])
            },
            "customer_billing_address": {
                "street": customer_details['address'].get('street', ''),
                "city": customer_details['address'].get('city', ''),
                "state": customer_details['address'].get('state', ''),
                "zip_code": customer_details['address'].get('zip_code', ''),
                "geo_cordinates": customer_details['address'].get('geo_cordinates', [])
            },
            "job_priority": "LOW",
            "job_type": "NEW",
            "job_category": JOB_CATEGORIES[job_category],
            "job_title": job_title,
            "custom_fields": job_cf
        }
    }, default=str)
    
    job_url = "https://stagingv2.zuperpro.com/api/jobs"
    job_response = requests.post(job_url, headers={'x-api-key': api_key, 'Content-Type': 'application/json'}, data=job_data)
    return json.loads(job_response.text)

def create_romtech_quote(api_key: str, customer_details: dict, order_number: str, row_index: int):
    """Create quote in draft status for ROMTECH"""
    try:
        # Set current date and expiry date
        current_date = datetime.now()
        expiry_date = current_date + timedelta(days=60)

        # Prepare quote data
        quote_data = {
            "estimate": {
                "customer": customer_details['customer_uid'],
                "customer_billing_address": customer_details['billing_address'],
                "customer_service_address": customer_details['address'],
                "custom_fields": prepare_custom_fields(estimate_custom_fields, df, row_index),
                "estimate_date": current_date.strftime('%Y-%m-%d %H:%M:%S'),
                "expiry_date": expiry_date.strftime('%Y-%m-%d %H:%M:%S'),
                "proposal_title": order_number,
                "estimate_description": f"Quote for {order_number}",
                "tags": ["ROM_TECH", "Migration Quote_0805"],
                #"organization": "db094129-e487-42a0-88c1-e819694291d9"
            }
        }

        # Create quote (will be in draft status by default)
        quote_response = requests.post(
            f"{HOST}estimate",
            headers={'x-api-key': api_key, 'Content-Type': 'application/json'},
            data=json.dumps(quote_data, default=str)
        )
        
        if quote_response.status_code != 200:
            return None, f"Quote Creation Failed: {quote_response.status_code}"
        quote_result = quote_response.json()
        if quote_result.get('type') != 'success':
            return None, f"Quote Creation Error: {quote_result.get('message')}"

        quote_uid = quote_result['data']['estimate_uid']
        return quote_uid, "Success"

    except Exception as e:
        print(f"Error creating ROMTECH quote: {str(e)}")
        return None, f"Error: {str(e)}"

estimate_custom_fields = field_object.get_custom_field('ESTIMATE')
def create_and_update_quote(api_key: str, customer_details: dict, order_owner: str, order_number: str, project_uid: str = None):
    """Create quote and update its status based on order owner"""
    try:
        # Current date and expiry date (60 days from now)
        current_date = datetime.now()
        expiry_date = current_date + timedelta(days=60)

        # Prepare quote data
        quote_data = {
            "estimate": {
                "customer": customer_details['customer_uid'],
                "customer_billing_address": customer_details['address'],
                "customer_service_address": customer_details['address'],
                "custom_fields": prepare_custom_fields(estimate_custom_fields, df, i),
                "estimate_date": current_date.strftime('%Y-%m-%d %H:%M:%S'),
                "expiry_date": expiry_date.strftime('%Y-%m-%d %H:%M:%S'),
                "proposal_title": order_number,
                "estimate_description": "",
                "tags": ["ROM_TECH","Migration Quote_0805"],
                #"organization": "db094129-e487-42a0-88c1-e819694291d9"
            }
        }

        # Add project to quote data if provided
        if project_uid:
            quote_data["estimate"]["project"] = project_uid

        # Create quote
        quote_response = requests.post(
            f"{HOST}estimate",
            headers={'x-api-key': api_key, 'Content-Type': 'application/json'},
            data=json.dumps(quote_data, default=str)
        )
        
        if quote_response.status_code != 200:
            print(f"Failed to create quote: {quote_response.status_code}")
            return None, "Quote Creation Failed"

        quote_result = quote_response.json()
        if quote_result.get('type') != 'success':
            print(f"Quote creation error: {quote_result.get('message')}")
            return None, "Quote Creation Failed"

        quote_uid = quote_result['data']['estimate_uid']

        # Update quote status based on order owner
        if order_owner.upper() == 'DME':
            # For DME: AWAIT_RESPONSE -> APPROVED
            await_status = {
                "estimate_status": "AWAIT_RESPONSE",
                "estimate_uid": quote_uid,
                "remarks": "Quote created for DME"
            }
            
            # Set to AWAIT_RESPONSE first
            requests.put(
                f"{HOST}estimate/{quote_uid}/status",
                headers={'x-api-key': api_key, 'Content-Type': 'application/json'},
                data=json.dumps(await_status, default=str)
            )

            # Then set to APPROVED
            approved_status = {
                "estimate_status": "APPROVED",
                "estimate_uid": quote_uid,
                "remarks": "Quote approved for DME"
            }
            
            status_response = requests.put(
                f"{HOST}estimate/{quote_uid}/status",
                headers={'x-api-key': api_key, 'Content-Type': 'application/json'},
                data=json.dumps(approved_status, default=str)
            )

        return quote_uid, "Success"

    except Exception as e:
        print(f"Error in quote creation/update: {str(e)}")
        return None, f"Error: {str(e)}"

def get_job_title_suffix(job_category: str) -> str:
  
    suffix_map = {
        'Pickup': 'P',
        'Onboarding': 'O',
        'Delivery': 'D'
    }
    return suffix_map.get(job_category, '')
for i in range (1,len(df.index)):
        print(i)
        
        try:

            if df.loc[i, 'JobUID'] == 'Error Found':
                order_owner = str(df.loc[i, 'order_owner']).strip()
                order_status = str(df.loc[i, 'Type']).strip()
                if order_status == 'Delivered and Onboarded':
                    initial_job_type = 'Pickup'
                    print("Creating Pickup job...")
                elif order_status == 'Delivered':
                    initial_job_type = 'Onboarding'
                else:
                    initial_job_type = 'Delivery'
                    
                    # Handle ROMTECH Quotations
                if order_owner.upper() == 'ROMTECH' and order_status.upper() == 'QUOTATION':
                    patient_id = str(df.loc[i, 'patient_id']).strip()
                    customer_details = get_customer_details(API, patient_id)
                    
                    if not customer_details:
                        print(f"Could not find customer for patient ID: {patient_id}")
                        df.loc[i, 'QuoteUID'] = 'Customer Not Found'
                        df.loc[i, 'Project_uid'] = 'ROMTECH Quotation - No Project Required'
                        df.loc[i, 'JobUID'] = 'ROMTECH Quotation - No Jobs Required'
                        continue
                    
                    # Create quote for ROMTECH
                    quote_uid, status = create_romtech_quote(API, customer_details, df.loc[i, 'rental_order_number'],i)
                    if quote_uid:
                        print(f"ROMTECH Quote created in draft status: {quote_uid}")
                        df.loc[i, 'QuoteUID'] = quote_uid
                        df.loc[i, 'Project_uid'] = 'ROMTECH Quotation - No Project Required'
                        df.loc[i, 'JobUID'] = 'ROMTECH Quotation - No Jobs Required'
                    else:
                        print(f"Quote creation failed: {status}")
                        df.loc[i, 'QuoteUID'] = status
                        df.loc[i, 'Project_uid'] = 'ROMTECH Quotation - Quote Failed'
                        df.loc[i, 'JobUID'] = 'ROMTECH Quotation - No Jobs Required'
                    
                    continue  

                patient_id = str(df.loc[i, 'patient_id']).strip()
                customer_details = get_customer_details(API, patient_id)
                project_cf = prepare_custom_fields(project_custom_fields, df, i)
                if not customer_details:
                    print(f"Could not find customer for patient ID: {patient_id}")
                    df.loc[i, 'Project_uid_1'] = 'Customer Not Found'
                    continue

                rental_type = str(df.loc[i, 'rental_business_type']).strip()
                category_uid = CATEGORY_MAPPING.get(rental_type)
                if not category_uid:
                    print(f"Invalid rental business type: {rental_type}")
                    df.loc[i, 'Project_uid_1'] = 'Invalid Category'
                    continue

                if rental_type.lower() == 'cardiac':
    # For Cardiac: today + 30 days
                    as_date = datetime.now() + timedelta(days=30)
                elif rental_type.lower() == 'orthopedic':
                    # For Orthopedic: Surgery Date + 1 day
                    try:
                        surgery_date = pd.to_datetime(df.loc[i, 'surgery_date'])
                        as_date = surgery_date + timedelta(days=1)
                    except (ValueError, KeyError) as e:
                        print(f"Error processing Surgery Date for row {i}: {str(e)}")
                        df.loc[i, 'Project_uid_1'] = 'Invalid Surgery Date'
                        continue
                else:
                    print(f"Unknown rental business type: {rental_type}")
                    df.loc[i, 'Project_uid_1'] = 'Unknown Business Type'
                    continue
                formatted_date = as_date.strftime('%Y-%m-%d %H:%M:%S')
                assigned_users = get_all_teams_for_project(df, i)      
                data =json.dumps({
                "project":{
                "project_name":df.loc[i,'rental_order_number'],
                "project_category": category_uid,
                "project_start_date":"",
                "project_end_date":"",
                "project_due_date":formatted_date,
                "project_assigned_to": assigned_users, 
                "project_description":'',
                "customer": customer_details['customer_uid'],
                "project_tags": ["Migration Project_0805"],
                "project_service_address": {
                    "street": customer_details['address'].get('street', ''),
                    "city": customer_details['address'].get('city', ''),
                    "state": customer_details['address'].get('state', ''),
                    "country": customer_details['address'].get('country', 'United States'),
                    "zip_code": customer_details['address'].get('zip_code', ''),
                    "geo_cordinates": customer_details['address'].get('geo_cordinates', [])
                },
                
                # Billing address (from API)
                "project_billing_address": {
                    "street": customer_details['address'].get('street', ''),
                    "city": customer_details['address'].get('city', ''),
                    "state": customer_details['address'].get('state', ''),
                    "country": customer_details['address'].get('country', 'United States'),
                    "zip_code": customer_details['address'].get('zip_code', ''),
                    "geo_cordinates": customer_details['address'].get('geo_cordinates', [])
                },       
                "custom_fields": project_cf,

                                    }   
            }, default = str)
            # print(data)
                product_url = "https://stagingv2.zuperpro.com/api/projects" 
                
                job_headers = {
                    'x-api-key': API,
                    'Content-Type': 'application/json'
                }
                response = requests.request("POST", product_url, headers = job_headers,data=data)
                print(response)
                parse_json = json.loads(response.text)
                if parse_json['type'] == 'success':
                    project_uid = parse_json['data']['project_uid']
                    df.loc[i,'Project_uid'] = project_uid
                    print(f"Project created successfully: {project_uid}")
                    try:
                        # Get appropriate status UID based on order status and rental type
                        project_type = 'Ortho Order' if rental_type.lower() == 'orthopedic' else 'Cardiac Order'
                        status_uid = PROJECT_STATUS_MAPPING.get(order_status, {}).get(project_type)
                        
                        if status_uid:
                            # Construct status update URL
                            project_status_url = f"{HOST}projects/{project_uid}/status"
                            
                            # Prepare status update payload
                            status_data = json.dumps({
                                "status_uid": status_uid
                            }, default=str)
                            
                            # Make status update request
                            status_response = requests.put(project_status_url,headers=job_headers,data=status_data)
                            
                            # Handle response
                            status_result = status_response.json()
                            if status_response.status_code == 200 and status_result.get('type') == 'success':
                                df.loc[i, 'project_status'] = "success"
                                print(f"Successfully updated project status to {order_status}")
                            else:
                                df.loc[i, 'project_status'] = "failed"
                                print(f"Failed to update project status: {status_result.get('message', 'Unknown error')}")
                        else:
                            print(f"No status UID found for order status: {order_status} and type: {project_type}")
                            df.loc[i, 'project_status'] = "No matching status"
                            
                    except Exception as status_error:
                        print(f"Error updating project status: {str(status_error)}")
                        df.loc[i, 'project_status'] = f"Error: {str(status_error)}"
                    try:
                        # For DME Quotation orders, only create project and quote
                        if order_owner.upper() == 'DME' and order_status.upper() == 'QUOTATION':
                            df.loc[i,'JobUID'] = 'DME Quotation - No Jobs Created'
                            
                            # Create quote for DME
                            quote_uid, quote_status = create_and_update_quote(
                                API, 
                                customer_details, 
                                order_owner, 
                                df.loc[i,'rental_order_number'],
                                project_uid
                            )
                            df.loc[i,'QuoteUID'] = quote_uid if quote_uid else quote_status
                            print(f"DME Quote created and approved: {quote_uid}")
                            
                        else:
                            # For all other cases, create jobs based on order status
                            job_cf = prepare_custom_fields(job_custom_fields, df, i)
                            job_results = []
                            
                            if order_status == 'Confirmed':
                                job_results.append(create_job(API, project_uid, customer_details, 'Delivery', formatted_date, df, i, job_cf))
                                job_results.append(create_job(API, project_uid, customer_details, 'Onboarding', formatted_date, df, i, job_cf))
                                job_results.append(create_job(API, project_uid, customer_details, 'Pickup', formatted_date, df, i, job_cf))
                                
                            # Update the job creation section for 'Delivered' status
                            elif order_status == 'Delivered':
                                job_results = []
                                successful_jobs = []
                                
                                # Create Onboarding job
                                print("Creating Onboarding job...")
                                onboarding_result = create_job(API, project_uid, customer_details, 'Onboarding', formatted_date, df, i, job_cf)
                                if isinstance(onboarding_result, dict):
                                    print(f"Onboarding job response: {json.dumps(onboarding_result, indent=2)}")
                                    if onboarding_result.get('type') == 'success':
                                        job_uid = onboarding_result.get('job_uid')  # Changed from data.job_uid
                                        if job_uid:
                                            successful_jobs.append(job_uid)
                                            df.loc[i, 'OnboardingJobUID'] = job_uid
                                            df.loc[i, 'JobUID'] = job_uid
                                            print(f"Successfully created Onboarding job: {job_uid}")
                                    else:
                                        error_msg = onboarding_result.get('message', 'Unknown error')
                                        df.loc[i, 'OnboardingJobUID'] = f"Failed: {error_msg}"

                                # Create Pickup job
                                print("Creating Pickup job...")
                                pickup_result = create_job(API, project_uid, customer_details, 'Pickup', formatted_date, df, i, job_cf)
                                if isinstance(pickup_result, dict):
                                    print(f"Pickup job response: {json.dumps(pickup_result, indent=2)}")
                                    if pickup_result.get('type') == 'success':
                                        job_uid = pickup_result.get('job_uid')  # Changed from data.job_uid
                                        if job_uid:
                                            successful_jobs.append(job_uid)
                                            df.loc[i, 'PickupJobUID'] = job_uid
                                            if df.loc[i, 'JobUID']:
                                                df.loc[i, 'JobUID'] = f"{df.loc[i, 'JobUID']}, {job_uid}"
                                            else:
                                                df.loc[i, 'JobUID'] = job_uid
                                            print(f"Successfully created Pickup job: {job_uid}")
                                    else:
                                        error_msg = pickup_result.get('message', 'Unknown error')
                                        df.loc[i, 'PickupJobUID'] = f"Failed: {error_msg}"
                            elif order_status == 'Delivered and Onboarded':
                                # Create only Pickup job
                                job_results = []
                                successful_jobs = []
                                
                                # Create Pickup job
                                print("Creating Pickup job for Delivered and Onboarded status...")
                                pickup_result = create_job(API, project_uid, customer_details, 'Pickup', formatted_date, df, i, job_cf)
                                if isinstance(pickup_result, dict):
                                    print(f"Pickup job response: {json.dumps(pickup_result, indent=2)}")
                                    if pickup_result.get('type') == 'success':
                                        job_uid = pickup_result.get('job_uid')
                                        if job_uid:
                                            successful_jobs.append(job_uid)
                                            df.loc[i, 'PickupJobUID'] = job_uid
                                            df.loc[i, 'JobUID'] = job_uid
                                            print(f"Successfully created Pickup job: {job_uid}")
                                    else:
                                        error_msg = pickup_result.get('message', 'Unknown error')
                                        df.loc[i, 'PickupJobUID'] = f"Failed: {error_msg}"
                                        df.loc[i, 'JobUID'] = 'No Jobs Created'
                                        print(f"Failed to create Pickup job: {error_msg}")
                                                        # Final status update
                                if successful_jobs:
                                    df.loc[i, 'JobUID'] = ', '.join(successful_jobs)
                                    print(f"Created jobs: {', '.join(successful_jobs)}")
                                else:
                                    df.loc[i, 'JobUID'] = 'No Jobs Created'
                                    print("No jobs were created successfully")
                    except Exception as job_error:
                        print(f"Error creating jobs: {str(job_error)}")
                        df.loc[i,'JobUID'] = f'Error: {str(job_error)}'
        except Exception as e:
            print(f"Error in row {i}: {str(e)}")
            df.loc[i, 'Project_uid'] = 'Error Found'
            df.loc[i, 'JobUID'] = 'Error Found'
            df.loc[i, 'QuoteUID'] = f'Error: {str(e)}'
df.to_excel("D:\ROM_TECH\project_upload\Sample_project_upload_0805_updated.xlsx", index = False)
