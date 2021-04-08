import sys
import json
import os
from os import system
from os import environ

# upgrade pandas first
os.system(f"{sys.executable} -m pip install -U pandas==1.0.3")
import pandas as pd
print(pd.__version__)
from pandas.io.json import json_normalize
import pytd
import pytd.pandas_td as td
import json
import requests
from requests import auth

class NeustarAPIError(Exception):
    """Raised if there are issues getting data from Neustar CI API"""

def main():   
    # set variables & secrets
    # TD_API_KEY is resolved from environment variables by td
    TD_API_KEY = os.environ['TD_API_KEY']
    TD_API_SERVER = os.environ['TD_API_SERVER']
    PROFILES_DATABASE = os.environ['PROFILES_DATABASE']
    PROFILES_TABLE = os.environ['PROFILES_TABLE']
    ENRICHED_PROFILES_TABLE = os.environ['ENRICHED_PROFILES_TABLE']
    NSR_SERVICE_ID = os.environ['NSR_SERVICE_ID']
    NSR_USR = os.environ['NSR_USR'] 
    NSR_PWD = os.environ['NSR_PWD']


    # initialise client and con
    con = td.connect(apikey=TD_API_KEY, endpoint=TD_API_SERVER)
    client = pytd.Client(apikey=TD_API_KEY, endpoint=TD_API_SERVER, database=PROFILES_DATABASE)
    cdp_record = get_profiles_pii_in_td(client, PROFILES_TABLE)
    
    # initialise empty df
    enriched_data = pd.DataFrame(columns=['cdp_customer_id','ekey', 'hhid', 'firstNameMatch', 'middleNameMatch', 'lastNameMatch',
       'phoneMatches', 'emailMatches', 'phoneLinkageScores',
       'emailLinkageScores', 'dobMatch', 'gender', 'genderMatch', 'age',
       'ageConfidence', 'deceased', 'emails', 'addresses', 'name.first',
       'name.middle', 'name.last'])
    
    for row in cdp_record['data']:
        result = resolve_identity_from_neustar(NSR_USR, NSR_PWD, NSR_SERVICE_ID, row[1], row[2], row[3], row[4], row[5])
        df = json_normalize(data=result['6544'], record_path=['individuals'])
        #prepend the cdp_customer_id to the NSR data for easy profile deduplication later
        df.insert(0,'cdp_customer_id', row[0])
        enriched_data = enriched_data.append(df)

    
    # finally write data into new table in TD
    load_data_into_td(con, enriched_data, PROFILES_DATABASE, ENRICHED_PROFILES_TABLE)

## Functions ##

# TD standard table where all  profiles are located = customers
def get_profiles_pii_in_td (client, profiles_tbl):
    sql = """SELECT cdp_customer_id, firstname, lastname, middleinitial, email, phone1
    FROM {0} WHERE email is not null OR phone1 IS NOT NULL""".format(profiles_tbl)
    profiles = client.query(query = sql)
    return profiles

def resolve_identity_from_neustar (usr, pwd, sid, firstname, lastname, middleinitial, email, phone):
    url = "https://webgwy.neustar.biz/v2/access/query?elems=6544&1601=Email=3,Individual,Name,Household,Address&serviceid={0}&1={1}&572={2}&1395={3},{4},{5}".format(sid, phone, email, firstname, lastname, middleinitial)
    payload = ""
    headers = {
        'Content-Type': 'application/json',
        'X-Accept': 'json'
        }
        
    response = requests.request("GET", url, headers=headers, data=payload, auth=(usr, pwd))
    return json.loads(response.text)['response'][0] #LATER : this will become a list for batch queries to the CIAPI

# use pandas dataframe method to load table
# consider using incremental data loads for heavier volumes
def load_data_into_td (client, df, profiles_db, enriched_profiles_tbl):
    client.load_table_from_dataframe(df, '{0}.{1}'.format(profiles_db, enriched_profiles_tbl), writer = 'bulk_import', if_exists='overwrite')  

if __name__ == "__main__":
    main()