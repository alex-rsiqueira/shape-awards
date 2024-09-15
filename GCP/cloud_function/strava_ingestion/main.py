import support
import requests
import numpy as np 
import pandas as pd
from datetime import datetime

def main(event, context):

    print('Start - Strava Ingestion')

    try:
        print('Get Strava Accounts')
        strava_accounts = support.get_strava_accounts()

        print('Loop through accounts')
        for user in strava_accounts:

            user_name = user['Name']
            print(f'Start - User: {user_name}')

            #user_id = user['UserID']
            client_id = user['Client_ID']
            client_secret = user['Client_Secret']
            authorization_code = user['Authorization_Code']
            refresh_token = user['Refresh_Token']

            #print('Read Strava API secret')
            #secret_key = support.read_secret('Strava_' + user_id)
            
            token = support.refresh_token(client_id,client_secret,authorization_code,refresh_token)

            authorization = f'Bearer {token}'
            headers = {"Authorization": authorization}

            print('Get athlete data')
            r = requests.get('https://www.strava.com/api/v3/athlete', headers=headers)

            # Save Dataframe
            df_athlete = pd.DataFrame([r.json()])
            dtinsert = datetime.today().strftime('%Y-%m-%d %H:%M:%S')
            df_athlete['dt_insert'] = dtinsert
            
            # Insert to BigQuery
            support.insert_db(df_athlete,'tb_strava_athlete','raw','shape-awards-2024')

            
            print('Get activities data')
            result = []
            
            for i in range(1,100):
                page = requests.get(f'https://www.strava.com/api/v3/athlete/activities?page={i}&per_page=200', headers=headers).json()
                
                if page != []:
                    result += page 
                else:
                    break
            
            data = []
            # Get athlete ID
            for row in result:
                row['athlete'] = row['athlete']['id']
                data.append(row)

            # Save Dataframe
            df_activities = pd.DataFrame(data)
            dtinsert = datetime.today().strftime('%Y-%m-%d %H:%M:%S')
            df_activities['dt_insert'] = dtinsert

            if 'device_watts' in df_activities:
                df_activities['device_watts'] = df_activities['device_watts'].astype(bool)
            
            # Insert to BigQuery
            support.insert_db(df_activities,'tb_strava_activities','raw','shape-awards-2024')

            print(f'End - User: {user_name}')

    except Exception as e:
        support.log_error(e)

    return "End - Strava Ingestion"
