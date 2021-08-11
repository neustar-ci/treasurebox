import sys
import json
import os
from os import system
from os import environ

# upgrade pandas and pytd client
os.system(f"{sys.executable} -m pip install -U pandas==1.0.3")
os.system(f"{sys.executable} -m pip install -U pytd==1.0.0")
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
    
    # creating schema mapping , customizable for client
    src_schema = {}
    src_schema['EXTERNAL_ID'] = os.environ['SRC_EXTERNAL_ID']
    src_schema['EMAIL_COL'] = os.environ['SRC_EMAIL_COL']
    src_schema['PHONE_COL'] = os.environ['SRC_PHONE_COL']
    src_schema['FNAME_COL'] = os.environ['SRC_FNAME_COL']
    src_schema['MNAME_COL'] = os.environ['SRC_MNAME_COL']
    src_schema['LNAME_COL'] = os.environ['SRC_LNAME_COL']
    src_schema['ADDR1_COL'] = os.environ['SRC_ADDR1_COL']
    src_schema['ADDR2_COL'] = os.environ['SRC_ADDR2_COL']
    src_schema['CITY_COL'] = os.environ['SRC_CITY_COL']
    src_schema['STATE_COL'] = os.environ['SRC_STATE_COL']
    src_schema['ZIP_COL'] = os.environ['SRC_ZIP_COL']


    # initialise client and con
    con = td.connect(apikey=TD_API_KEY, endpoint=TD_API_SERVER)
    client = pytd.Client(apikey=TD_API_KEY, endpoint=TD_API_SERVER, database=PROFILES_DATABASE)
    cdp_record = get_profiles_pii_in_td(client, PROFILES_TABLE, src_schema)
    
    # initialise empty df
    enriched_data = pd.DataFrame(columns=['cdp_customer_id','ekey', 'hhid', 'firstNameMatch', 'middleNameMatch', 'lastNameMatch',
       'phoneMatches', 'emailMatches', 'phoneLinkageScores',
       'emailLinkageScores', 'dobMatch', 'gender', 'genderMatch', 'age',
       'ageConfidence', 'emails', 'addresses', 'name.first',
       'name.middle', 'name.last'])
    
    for row in cdp_record['data']:
        result = resolve_identity_from_neustar(NSR_USR, NSR_PWD, NSR_SERVICE_ID, row[1], row[2], row[3], row[4], row[5])
        df = json_normalize(data=result['6544'], record_path=['individuals'])
        #prepend the cdp_customer_id to the NSR data for easy profile deduplication later
        df.insert(0,'cdp_customer_id', row[0])
        # drop sensitive fields as below {client may drop based upon their need}
        # df.drop(columns = ['deceased'])
        enriched_data = enriched_data.append(df)

    
    # finally write data into new table in TD
    load_data_into_td(con, enriched_data, PROFILES_DATABASE, ENRICHED_PROFILES_TABLE)

## Functions ##
def xstr(s):
    if s is None:
        return ''
    return str(s)

# TD standard table where all  profiles are located = customers
# customise for client specific schema
def get_profiles_pii_in_td (client, profiles_tbl, schema):
    sql = """SELECT {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}, {10}, {11}
    FROM {0} WHERE email is not null OR phone1 IS NOT NULL""".format(profiles_tbl, schema['EXTERNAL_ID'], schema['FNAME_COL'], schema['LNAME_COL'], schema['MNAME_COL'], schema['EMAIL_COL'], schema['PHONE_COL'], schema['ADDR1_COL'], schema['ADDR2_COL'], schema['CITY_COL'], schema['STATE_COL'], schema['ZIP_COL'] )
    profiles = client.query(query = sql)
    return profiles

def resolve_identity_from_neustar (usr, pwd, sid, firstname, lastname, middleinitial, email, phone, address1, address2, city, state, zip):
    # create full address from address lines
    address = xstr(address1) + " " + xstr(address2)
    url = "https://webgwy.neustar.biz/v2/access/query?elems=6544&1601=Email=3,Individual,Name,Household,Address&serviceid={0}&1={1}&572={2}&1395={3},{4},{5}&1390={6}&1391={7}&1392={8}&1393={9}".format(sid, phone, email, firstname, lastname, middleinitial, address, city, state, zip)
    payload = ""
    headers = {
        'Content-Type': 'application/json',
        'X-Accept': 'json'
        }
    print("URL:" + url) 
    response = requests.request("GET", url, headers=headers, data=payload, auth=(usr, pwd))
    try:
        string_to_return = json.loads(response.text)['response'][0]
    return  #LATER : this will become a list for batch queries to the CIAPI

# use pandas dataframe method to load table
# consider using incremental data loads for heavier volumes
def load_data_into_td (client, df, profiles_db, enriched_profiles_tbl):
    client.load_table_from_dataframe(df, '{0}.{1}'.format(profiles_db, enriched_profiles_tbl), writer = 'bulk_import', if_exists='overwrite')  

if __name__ == "__main__":
    main()