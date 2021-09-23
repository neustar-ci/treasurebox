import sys
import json
import os
from os import system
from os import environ

# upgrade pandas and pytd client
os.system(f"{sys.executable} -m pip install -U pandas==1.0.3")
os.system(f"{sys.executable} -m pip install -U pytd==1.0.0")
import pandas as pd

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
    # now get all profiles for enrichment
    cdp_record = get_profiles_pii_in_td(client, PROFILES_TABLE, ENRICHED_PROFILES_TABLE, src_schema)
    
    # initialise empty df
    enriched_data = pd.DataFrame(columns=['cdp_customer_id','ekey', 'hhid', 'firstNameMatch', 'lastNameMatch', 'addressMatches',
       'emailMatches', 'addressLinkageScores', 'emailLinkageScores',
       'dobMatch', 'gender', 'genderMatch', 'age', 'ageConfidence',
       'phones', 'emails', 'addresses', 'name.first', 'name.middle',
       'name.last', 'household.hhid', 'household.MatchType',
       'household.NumberOfPersonsInLivingUnit',
       'household.NumberOfChildrenInLivingUnit',
       'household.NumberOfAdultsInLivingUnit',
       'household.Children_PresenceOfChild_0_18', 'household.Children_Age_0_3',
       'household.Children_Age_0_3_Score', 'household.Children_Age_0_3_Gender',
       'household.Children_Age_4_6', 'household.Children_Age_4_6_Score',
       'household.Children_Age_4_6_Gender', 'household.Children_Age_7_9',
       'household.Children_Age_7_9_Score', 'household.Children_Age_7_9_Gender',
       'household.Children_Age_10_12', 'household.Children_Age_10_12_Score',
       'household.Children_Age_10_12_Gender', 'household.Children_Age_13_15',
       'household.Children_Age_13_15_Score',
       'household.Children_Age_13_15_Gender', 'household.Children_Age_16_18',
       'household.Children_Age_16_18_Score',
       'household.Children_Age_16_18_Gender',
       'household.Estimated_Household_Income_Narrow',
       'household.Estimated_Household_Income_Wide',
       'household.Property_Realty_Property_Indicator',
       'household.Property_Realty_Home_Land_Value',
       'household.Estimated_Current_Home_Value',
       'household.Property_Realty_Home_Total_Value',
       'household.Property_Realty_Home_Median_Value',
       'household.Dwelling_Unit_Size', 'household.Dwelling_Type',
       'household.Homeowner_Combined_Homeowner_Renter',
       'household.Property_Realty_Year_Built_Confidence',
       'household.Property_Realty_Year_Built', 'household.Length_Of_Residence',
       'household.Presence_Of_Credit_Card',
       'household.Presence_Of_Premium_Credit_Card', 'household.Mail_Responder',
       'household.Home_Business', 'household.Activity_Date',
       'household.Census_2010_State_And_County',
       'household.Census_2010_Tract_And_Block_Group',
       'household.Core_Based_Statistical_Areas_CBSA',
       'household.Core_Based_Statistical_Area_Type',
       'household.Census_Rural_Urban_County_Size_Code',
       'household.Median_Family_Household_Income',
       'household.Household_Composition', 'household.E1_Segment',
       'household.E1_Segment_Match_Flag', 'household.Buying_Power_Score',
       'household.Credit_Flag', 'household.Net_Asset_Value'])
    
    for row in cdp_record['data']:
        result = resolve_identity_from_neustar(NSR_USR, NSR_PWD, NSR_SERVICE_ID, row[1], row[2], row[3] , row[4], row[5], row[6], row[7], row[8], row[9], row[10]) # change row indexes based on customer schema
        try:
          d=json.loads(result)['response'][0]
          df1 = pd.json_normalize(data=d['6544'], record_path=['individuals'])
          df2 = pd.json_normalize(data=d['6544'])
          df = df1.join(df2)
          #prepend the src_external_id to the NSR data for easy profile deduplication later
          df.insert(0,'cdp_customer_id', row[0])
          # drop duplicate or sensitive fields as below {client may drop more based upon their need}
          df.drop(columns = ['deceased','individuals'])
          enriched_data = enriched_data.append(df)
        except:
          pass
    
    # finally write data into new table in TD
    load_data_into_td(con, enriched_data, PROFILES_DATABASE, ENRICHED_PROFILES_TABLE)

## Functions ##
def xstr(s):
    if s is None:
        return ''
    return str(s)

# TD standard table where all  profiles are located = customers
# customise for client specific schema
def get_profiles_pii_in_td (client, profiles_tbl, enriched_profiles_tbl, schema):
    sql = """SELECT a.{1}, a.{2}, a.{3}, a.{4}, a.{5}, a.{6}, a.{7}, a.{8}, a.{9}
    FROM {0} a left join {10} b on a.{1}=b.cdp_customer_id WHERE b.cdp_customer_id is null AND {4} IS NOT NULL or {2} IS NOT NULL limit 1500""".format(profiles_tbl, schema['EXTERNAL_ID'], schema['FNAME_COL'], schema['LNAME_COL'], schema['MNAME_COL'], schema['PHONE_COL'],schema['EMAIL_COL'], schema['ADDR1_COL'], schema['ADDR2_COL'], schema['CITY_COL'], schema['STATE_COL'], schema['ZIP_COL'], enriched_profiles_tbl )
    #log generated sql , modify if necessary
    print(sql)
    profiles = client.query(query = sql)
    return profiles

def resolve_identity_from_neustar (usr, pwd, sid, firstname, lastname, middleinitial, email, phone, address1, address2, city, state, zip):
    # create full address from address lines
    address = xstr(address1) + ", " + xstr(address2)
    url = "https://webgwy.neustar.biz/v2/access/query?elems=6544&1601=Email=3,Individual,Name,Household,Address,Phone&serviceid={0}&1={1}&572={2}&1395={3},{4},{5}&1390={6}&1391={7}&1392={8}&1393={9}".format(sid, xstr(phone), xstr(email), xstr(firstname), xstr(lastname), xstr(middleinitial), xstr(address), xstr(city), xstr(state), xstr(zip))
    payload = ""
    headers = {
        'Content-Type': 'application/json',
        'X-Accept': 'json'
        }
    print(url)    
    response = requests.request("GET", url, headers=headers, data=payload, auth=(usr, pwd))
    print(response.text)
    return response.text #LATER : this will become a list for batch queries to the CIAPI

# use pandas dataframe method to load table
# consider using incremental data loads for heavier volumes
def load_data_into_td (client, df, profiles_db, enriched_profiles_tbl):
    client.load_table_from_dataframe(df, '{0}.{1}'.format(profiles_db, enriched_profiles_tbl), writer = 'bulk_import', if_exists='append')  

if __name__ == "__main__":
    main()