import numpy as np
import pandas as pd
import os, glob
import requests, json
from astropy import units as u
from astropy.coordinates import SkyCoord
from astropy.cosmology import FlatLambdaCDM
import astropy.units as u
from astropy import constants as const
from astropy.table import Table
import threading
import time
from tqdm import tqdm
import warnings
from datetime import date

today = date.today()
warnings.filterwarnings('ignore')

# Define global parameters
global TOKEN, BASEURL, USR_LAST_NAME, db

with open('user_info.json') as usr:
    # User Infomation
    usr_data = json.load(usr)

GETTOKEN = usr_data['user']['FritzToken']
USR_LAST_NAME = usr_data['user']['user_last_name']
BASEURL = 'https://fritz.science/'

def api(method, endpoint, data=None):
    ''' Info : Basic API query, takes input the method (eg. GET, POST, etc.), the endpoint (i.e. API url)
               and additional data for filtering
        Returns : response in json format
        CAUTION! : If the query doesn't go through, try putting the 'data' input in 'data' or 'params'
                    argument in requests.request call
    '''
    headers = {'Authorization': f'token {GETTOKEN}'}

    response = requests.request(method, endpoint, json=data, headers=headers)

    return response.json()

def get_all_sources(group_id):
    base_url = "https://fritz.science/api/sources"
    token = "d3abacda-ccc5-4ded-9a17-2836be9b1d7a"
    all_sources = []
    num_per_page = 500
    page = 1
    total_matches = None
    retry_attempts = 0
    max_retry_attempts = 10


    while retry_attempts <= max_retry_attempts:
        r = requests.get(
            f"{base_url}?group_ids={group_id}&pageNumber={page}&numPerPage={num_per_page}&totalMatches={total_matches}",
            headers={"Authorization": f"token {token}"},
        )
        data = r.json()
        if data["status"] != "success":
            print(data)  # log as appropriate
            retry_attempts += 1
            time.sleep(1)
            continue

        if retry_attempts != 0:
            retry_attempts = 0

        all_sources.extend(data["data"]["sources"])
        total_matches = data["data"]["totalMatches"]

        print(f"Fetched {len(all_sources)} of {total_matches} sources.")

        if len(all_sources) >= total_matches:
            break
        page += 1
    
    return all_sources



def get_classifications(ID):
    base_url = "https://fritz.science/api/sources/"
    token = "d3abacda-ccc5-4ded-9a17-2836be9b1d7a"


    r = requests.get(base_url+ID+"/classifications",
        headers={"Authorization": f"token {token}"},
    )
    data = r.json()['data']

    return data



# download list of objectIDs from groups on Fritz

out = get_all_sources(348)
ids = [i['id'] for i in out]
ras = [i['ra'] for i in out]
decs = [i['dec'] for i in out]

# make a dataframe
df = pd.DataFrame({'id':ids,'ra':ras,'dec':decs})

# preset all columns to 0
colnames = "variable,periodic,pulsator,RR Lyrae,Cepheid,multi periodic,sinusoidal,non-variable,bogus,bright star,long timescale,irregular,blend,galaxy,eclipsing,EW,flaring,ccd artifact,binary star,detached eclipsing MS-MS,W Uma,RR Lyrae ab,RR Lyrae c,Delta Scu,RR Lyrae d,Cepheid type-II,RS CVn,EB,Beta Lyr,sawtooth,RR Lyrae Blazhko,EA,YSO,dipping,wrong period,double period,AGN,RV Tau,LPV,Mira,half period,SRV,W Virginis,compact binary,eclipsing WD+dM (NN Ser),nice,niice,F,O,BL Her,OSARG,eclipsing sdB+dM (HW Vir),elliptical,Redback pulsar,CV,W UMa".split(',')

# for each object, download its classification
for i in colnames:
    df[i] = 0

# loop over the rows in the df and download the data from Fritz (might not be the best way to do this...)
for index, row in df.iterrows():
    ID = row['id']
    c = get_classifications(ID)
    if len(c) > 0:
        for e in c:
            if e['classification'] not in df.keys():
                print('trying to add a classification "%s"' %e['classification'])
                continue
            df.loc[index,e['classification']] = e['probability']
        


