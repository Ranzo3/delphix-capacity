# %%

from typing import Optional, Tuple, List
import urllib3
import requests
import traceback
import json
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
from datetime import timezone
from datetime import timedelta



# CONFIGURATION SECTION
valid_response_status_codes = [200, 201]
base_header = {'Content-Type': 'application/json'}
pageSize = 100
timeout=(30, 300)

globalCookies = dict()



try:
   import http.client as http_client
except ImportError:
   # Python 2
   import httplib as http_client


# THIS IS TO DISABLE WARNINGS ON SELF SIGNED CA
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)



# %%

def post_call(url :str, request_headers :dict, request_data: dict =None, files: dict=None) -> Tuple[bool, Optional[dict]]:
    
    global globalCookies

    try:
        if request_data:
            response = requests.post(url, headers=request_headers, json=request_data, cookies=globalCookies, timeout=timeout, verify=False)
        if files:
            response = requests.post(url, headers=request_headers, files=files, data=request_data, cookies=globalCookies, timeout=timeout, verify=False)
        if not request_data and not files:
            print("Error:  POST to ",url," with no data and no files")
            return False, None

        if response.status_code not in valid_response_status_codes:
            print("Error: Response Status Code ",response.status_code)
            return False, None
        
        else:
            response_data = response.json()
            globalCookies=response.cookies
            if debug_level >= 2:
               print("COOKIES: ",globalCookies)

            if response_data["type"] == "ErrorResult" or response_data["type"] != "OKResult":
                status_code = response.status_code
                details = (response_data['error']['details'])
                action = (response_data['error']['action'])
                id = (response_data['error']['id'])
                
                print()
                print("ERROR: Call to ",url," Failed")
                print('Status Code:', status_code)
                print('Details:', details)
                print('Action:', action)
                print('Id:', id)
                
                return False, response_data
 
            else:
                if debug_level >= 1:
                    print()
                    print("Call to URL",url)
                    print("JSON in post_call",json.dumps(response.json(), indent=4, sort_keys=True))
                    print("COOKIE in post_call",response.cookies.get_dict())
                    print("STATUS CODE in post_call",response.status_code)
                
            return True, response_data


    except Exception as excp:
        print(traceback.format_exc())
        return False, None



# %%

def get_call(url :str, request_headers :dict, query_params :dict=None) -> Tuple[bool, Optional[List[dict]]]:
    '''
    Get all objects using page size defined in pageSize
    '''

    if not query_params:
        query_params = dict()

    page_number = 1
    query_params["page_size"] = pageSize
    query_params["page_number"] = page_number
    total = 0
    return_list = list()

    while True:   
        if query_params:            
            response = requests.get(url, headers=request_headers, cookies=globalCookies, params=query_params, timeout=timeout, verify=False)
        else:
            response = requests.get(url, headers=request_headers, cookies=globalCookies, timeout=timeout, verify=False)
        if response.status_code not in valid_response_status_codes:
            if "is outside of the acceptable range. The last page is" in response.text:
                if debug_level >= 1:
                    print()
                    print("Call to URL",url)
                    print("JSON in post_call",json.dumps(response.json(), indent=4, sort_keys=True))
                    print("COOKIE in post_call",response.cookies.get_dict())
                    print("STATUS CODE in post_call",response.status_code)
                return True, return_list
            else:
                response_data = response.json()
                status_code = response.status_code
                details = (response_data['error']['details'])
                action = (response_data['error']['action'])
                id = (response_data['error']['id'])

                print()
                print('Status Code:', status_code)
                print("ERROR: Call to ",url," Failed")
                print('Details:', details)
                print('Action:', action)
                print('Id:', id)

                return False, None

        response_obj = response.json()
        total = total + response_obj["_pageInfo"]["numberOnPage"]
        
        return_list.extend(response_obj["responseList"])
        if total >= response_obj["_pageInfo"]["total"]:
            break
        query_params["page_number"] = query_params["page_number"] + 1

    return True, return_list

# %%

def get_one_call(url :str, request_headers :dict, query_params :dict=None) -> Tuple[bool, Optional[dict]]:
    '''
    Get only one object - no pagination involved - url already should have an ID
    '''

    if query_params:
        response = requests.get(url, headers=request_headers, cookies=globalCookies, params=query_params, verify=False)
    else:
        response = requests.get(url, headers=request_headers, cookies=globalCookies, verify=False)

    if response.status_code not in valid_response_status_codes:
        status_code = response.status_code
        response_data = response.json()
        details = (response_data['error']['details'])
        action = (response_data['error']['action'])
        id = (response_data['error']['id'])

        print()
        print("ERROR: Call to ",url," Failed")
        print('Status Code:', status_code)
        print('Details:', details)
        print('Action:', action)
        print('Id:', id)

        return False, None

    else:
        if debug_level >= 1:
            print()
            print("Call to URL",url)
            print("JSON in post_call",json.dumps(response.json(), indent=4, sort_keys=True))
            print("COOKIE in post_call",response.cookies.get_dict())
            print("STATUS CODE in post_call",response.status_code)
        return True, response.json()
# %%

def get_capacity_history(base_url: str, container: str, st: str, et: str,resolution: int=86400) -> Tuple[bool, Optional[dict]]:


    '''
    get capacity history for one ref 
    '''

    api_url = f"{base_url}/capacity/consumer/historical"
    
    query_params = dict()
    query_params["container"] = container
    query_params["startDate"] = st
    query_params["endDate"] = et
    query_params["resolution"] = resolution

    success, response = get_one_call(api_url, base_header, query_params=query_params)

#    if not success:
#        return False, None
#    if 'executionId' not in response or 'errorMessage' in response:
#        return False, None

    return True, response
# %%
def get_source_containers(base_url: str, source_type: str=None) -> Tuple[bool, Optional[dict]]:

    '''
    get source containers 
    '''

    api_url = f"{base_url}/source"

    success, response = get_one_call(api_url, base_header)

#    if not success:
#        return False, None
#    if 'executionId' not in response or 'errorMessage' in response:
#        return False, None
    
    containers = []

    for i in response["result"]:
        if i["type"] == source_type:
            container=i["container"]
            containers.append(container)
            if debug_level >= 1:
                print(json.dumps(i, indent=4))  
                print(container)



    return True, containers



# %%

def login(base_url :str, user :str, password :str) -> Tuple[bool, Optional[dict]]:
    '''
    Create session to engine
    '''
    
    api_url = f"{base_url}/session"

    request_data = {'type' : 'APISession',
                                'version' : {
                                'type' : 'APIVersion',
                                'major' : 1,
                                'minor' : 10,
                                'micro' : 0}
                                }

    success, response = post_call(url=api_url, request_headers=base_header, request_data=request_data)
    #success, response = post_call(url=api_url, request_headers=base_header)

    #response = requests.post(api_url, headers=base_header, json=request_data, timeout=timeout, verify=False)
    
    #response_data = response.json()

    if not success:
        print("Unable to establish a Session, Exiting")
        exit(1)


    '''
    login to engine
    '''
   
 
    api_url = f"{base_url}/login"

    request_data = {'type' : 'LoginRequest',
                    'username' : user,
                    'password' : password
                                          }

    success, response = post_call(url=api_url, request_headers=base_header, request_data=request_data)

    if not success:
        print("Unable to Login, Exiting")
        exit(1)

    return True, response

# %%
def processCapHistory(containers: list, et: str, st: str) -> Tuple[bool, Optional[dict]]:
    records = []
    record = {}
    for container in containers:
        ret_status, response = get_capacity_history(base_url, 
                                                    container=container, 
                                                    resolution=resolution, 
                                                    et=et, 
                                                    st=st)
        for i in response["result"]:
            timestamp=i["timestamp"]
            activeSpace=i["breakdown"]["activeSpace"]
            syncSpace=i["breakdown"]["syncSpace"]
            logSpace=i["breakdown"]["logSpace"]
            actualSpace=i["breakdown"]["actualSpace"]

            record = {
                "container": container,
                "timestamp": timestamp,
                "activeSpace": activeSpace,
                "syncSpace": syncSpace,
                "logSpace": logSpace,
                "actualSpace": actualSpace
            }

            if debug_level >= 1:
                print(record)

            records.append(record)
            if debug_level >= 1:
                print(records)
    
    df = pd.DataFrame.from_dict(records)
    if debug_level >= 1:
        print(df)


    return True,df
    

def getTopContainers(containers: list, topn: int=10) -> Tuple[bool, list]:
    records = []
    record = {}

    for i in containers:
        ret_status, response = get_capacity_history(base_url, 
                                                container=i, 
                                                resolution=86400, 
                                                et=now.strftime('%Y-%m-%dT%H:%M:%S.000Z'), 
                                                st=weekAgo.strftime('%Y-%m-%dT%H:%M:%S.000Z'))
        
        #Determine how many data points we got, and just keep the last one (most recent)
        length = len(response["result"])
        lastrecord =  response["result"][length-1]
        
        timestamp=lastrecord["timestamp"]
        activeSpace=lastrecord["breakdown"]["activeSpace"]
        syncSpace=lastrecord["breakdown"]["syncSpace"]
        logSpace=lastrecord["breakdown"]["logSpace"]
        actualSpace=lastrecord["breakdown"]["actualSpace"]
        
        record = {
                "container": i,
                "timestamp": timestamp,
                "activeSpace": activeSpace,
                "syncSpace": syncSpace,
                "logSpace": logSpace,
                "actualSpace": actualSpace
            }
        
        if debug_level >= 1:
            print(record)

        records.append(record)
        if debug_level >= 1:
            print(records)

        df = pd.DataFrame.from_dict(records)
        if debug_level >= 1:
            print(df)
        
        df=df.sort_values(by=['actualSpace'],ascending=False).head(topn)

    top10Containers=df["container"].values.tolist()

    return top10Containers







# %%
#if __name__ == "__main__":

globalCookies = None
now = datetime.now(timezone.utc)
weekAgo = now - timedelta(days=1)
yearAgo = now - timedelta(days=365)

'''
parser = argparse.ArgumentParser(description='Run a capacity report')
parser.add_argument('--engine_fqdn', type=str, help='engine fqdn')
parser.add_argument('--job_name', type=str, help='job name', required=False)
parser.add_argument('--job_id', type=int, help='job id', required=False)
parser.add_argument('--username', type=str, help='username')
parser.add_argument('--password', type=str, help='password')
parser.add_argument('--details', help='print details', action='store_true')
parser.add_argument('--debug', type=int, help='debug level 0-4', required=False, default=0)
parser.add_argument('--resolution', type=int, help='resolution in seconds', required=False, default=2592000)
parser.add_argument('--file', type=str, help='output file', required=False, default='/tmp/output.csv')
parser.add_argument('--topn', type=int, help='The number of largest objects, or 0 for all', required=False, default=10)
parser.add_argument('--st', type=str, help='Start Time string. ex: 2022-01-01T00:00:00.000Z.  Default 1 year ago.', required=False)
parser.add_argument('--et', type=str, help='Start Time string. ex: 2022-01-01T00:00:00.000Z.  Default is today.', required=False)
parser.add_argument('--type', type=str, help='Delphix object type. Ex: MSSqlLinkedSource', required=False, default='MSSqlLinkedSource')
parser.add_argument('--devmode', type=str, help='devmode', required=False, default=False)



#args, unknown = parser.parse_known_args()
args = parser.parse_args()

engine_fqdn = args.engine_fqdn
masking_job_name = args.job_name
masking_job_id = args.job_id
username = args.username
password = args.password
debug_level = args.debug
resolution = args.resolution
output_file = args.file
topn = args.topn
source_type = args.type
st = args.st
et = args.et
type = args.type
devmode = args.devmode
'''

devmode=True
if devmode:
#Hard codings for Testing only

    engine_fqdn="uvo1b8qgxzzd9cusbfa.vm.cld.sr"
    username="admin"
    password="Delphix_123!"
    debug_level=1
    resolution=86400
    output_file="~/Downloads/test.csv"
    topn=1
    st=""
    et=""
    startTime=yearAgo.strftime('%Y-%m-%dT%H:%M:%S.000Z')
    endTime=now.strftime('%Y-%m-%dT%H:%M:%S.000Z')
    source_type="MSSqlLinkedSource"


if not et:
    endTime=now.strftime('%Y-%m-%dT%H:%M:%S.000Z')

if not st:
    startTime=yearAgo.strftime('%Y-%m-%dT%H:%M:%S.000Z')

if debug_level >= 2:
    print("Turn on HTTP Debug")
    http_client.HTTPConnection.debuglevel = 1

base_url = f"https://{engine_fqdn}/resources/json/delphix"

ret_status, response = login(base_url, username, password)
#%%
ret_status, containers = get_source_containers(base_url, source_type=source_type)

#%%
topContainers = getTopContainers(containers,topn)
# %%
ret_status, dataframe = processCapHistory(topContainers, et=endTime, st=startTime)

dataframe.to_csv(output_file)

#dataframe.plot(x="timestamp",y="actualSpace")
#dataframe.plot.bar(stacked=True)

print("Wrote File: ",output_file)




# %%
