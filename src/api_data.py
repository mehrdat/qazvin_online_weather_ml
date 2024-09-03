
from feast import FeatureStore,Entity,ValueType,Feature,FeatureView
import requests
from datetime import timedelta
import pandas as pd
import xml.etree.ElementTree as ET
#data=Entity()
from tenacity import retry, stop_after_attempt, wait_fixed

url = "https://api.open-meteo.com/v1/forecast?latitude=52.52&longitude=13.41&current=temperature_2m,wind_speed_10m&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m"

def weth(url):

    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for HTTP errors

        # Check if the response content is not empty
        if response.content:
            try:
                data = response.json()
                
                #jsn=pd.json_normalize(data['hourly'])
                #jsn=pd.DataFrame.from_dict(data,orient='split')
                #df = pd.DataFrame(jsn)
                #print(data)
                
                json_data = pd.DataFrame(data['hourly'], columns=data['hourly']).to_json(orient='split')
                df = pd.read_json(json_data, orient='split')
                return df
            except requests.exceptions.JSONDecodeError as e:
                print(f"Failed to parse JSON. Error: {e}")
        else:
            print("Empty response received from the API.")
    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch data from API. Error: {e}")


#weth(url).head()