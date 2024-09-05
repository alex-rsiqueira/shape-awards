import support
import requests
import pandas as pd
from google.cloud import bigquery

def main(event, context):
    
    try:
        print('Get Strava Accounts')
        strava_accounts = support.get_strava_accounts()

        print('Loop through accounts')
        for row in strava_accounts:
            user_id = row['UserID']
            client_id = row['Client_ID']
            client_secret = row['Client_Secret']
            authorization_code = row['Authorization_Code']
            refresh_token = row['Refresh_Token']

            #print('Read Strava API secret')
            #secret_key = support.read_secret('Strava_' + user_id)
            
            token = support.refresh_token(client_id,client_secret,authorization_code,refresh_token)

            authorization = f'Bearer {token}'
            headers = {"Authorization": authorization}

            print('Get athlete data')
            r = requests.get('https://www.strava.com/api/v3/athlete', headers=headers)

            data = []

            # Get athlete ID
            for row in r.json():
                row['athlete'] = row['athlete']['id']
                data.append(row)

            # Save Dataframe
            df_athlete = pd.DataFrame([r.json()])
            
            # Insert to BigQuery
            support.insert_db(df_athlete,'tb_strava_athlete','raw','shape-awards-2024')

            print('Get activities data')
            r = requests.get('https://www.strava.com/api/v3/athlete', headers=headers)

            data = []

            # Get athlete ID
            for row in r.json():
                row['athlete'] = row['athlete']['id']
                data.append(row)

            # Save Dataframe
            df_activities = pd.DataFrame(data)
            
            # Insert to BigQuery
            support.insert_db(df_activities,'tb_strava_activities','raw','shape-awards-2024')

    except Exception as e:
        support.log_error(e)

    return "Strava Ingestion - Complete"
