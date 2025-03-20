import requests
from datetime import datetime, timedelta, timezone
import xml.etree.ElementTree as ET
import pandas as pd
import os
import mysql.connector


# Namespace handling
ns = {"ns": "urn:iec62325.351:tc57wg16:451-6:generationloaddocument:3:0"}
lags = [1, 2, 4]  # 15 min, 30 min, 1 hr
feature_col_names=['lag_'+str(i) for i in lags]+['diff_1hr','rolling_mean_1hr','rolling_std_1hr','minute','hour','day_of_week','month','year']
ENTSOE_API_KEY=os.getenv('ENTSOE_API_KEY')



def extract(start_time,end_time):
    bidding_zone = '10Y1001A1001A82H'

    # Updated ENTSO-E API endpoint


    url = (
        f'https://web-api.tp.entsoe.eu/api?documentType=A65&processType=A16'
        f'&outBiddingZone_Domain={bidding_zone}&periodStart={start_time}'
        f'&periodEnd={end_time}&securityToken={ENTSOE_API_KEY}'
    )

    try:

        response = requests.get(url)

    except requests.RequestException as e:
        print(f"Error fetching data: {e}")

    # Load and parse the XML file
    root = ET.fromstring(response.text)
    # Extract Points from XML
    data = []
    timestamps=get_timestamps(start_time, end_time)
    for timestamp, point in zip(timestamps, root.findall(".//ns:Point", ns)):
        # position = point.find("ns:position", ns).text
        quantity = point.find("ns:quantity", ns).text
        data.append((timestamp, float(quantity)))

    # Convert to DataFrame
    df = pd.DataFrame(data, columns=["utc_timestamp", "DE_load_actual_entsoe_transparency"])
    df['utc_timestamp'] = pd.to_datetime(df['utc_timestamp'])

    return df


def round_off_timestamp(input_timestamp):

    curr_minute =input_timestamp.minute
    print(input_timestamp)

    if curr_minute <15:
        return input_timestamp.strftime('%Y%m%d%H00')
    elif 15 <curr_minute <30:
        return input_timestamp.strftime('%Y%m%d%H15')
    elif 30 <curr_minute <45:
        return input_timestamp.strftime('%Y%m%d%H30')
    else:
        return input_timestamp.strftime('%Y%m%d%H45')


def get_timestamps(start_date, end_date):
    timestamps =[]
    current = datetime.strptime(start_date, '%Y%m%d%H%M')
    end = datetime.strptime(end_date, '%Y%m%d%H%M')

    while current <= end:
        current += timedelta(minutes=15)
        timestamps.append(current.strftime('%Y%m%d%H%M'))
    return timestamps


def transform(df):
    if len(df) < 4:
        print("Last four load intervals are required for prediction")
        return None

    for lag in lags:  # 15 min, 30 min, 1 hr:
        df[f'lag_{lag}'] = df["DE_load_actual_entsoe_transparency"].shift(lag)

    df['year'] = [i.year for i in df['utc_timestamp']]
    df['month'] = [i.month for i in df['utc_timestamp']]
    df['day_of_week'] = [i.day_of_week for i in df['utc_timestamp']]
    df['hour'] = [i.hour for i in df['utc_timestamp']]
    df['minute'] = [i.minute for i in df['utc_timestamp']]
    df['rolling_mean_1hr'] = df["DE_load_actual_entsoe_transparency"].rolling(window=4).mean()
    df['rolling_std_1hr'] = df["DE_load_actual_entsoe_transparency"].rolling(window=4).std()
    df['diff_1hr'] = df["DE_load_actual_entsoe_transparency"].diff(periods=4)
    return df

    #return df.loc[len(df)-1,feature_col_names]


def load(timestamp,forecasted_value,actual_value):
    try:

        conn = mysql.connector.connect(
            host="localhost",
            user="root",
            password=os.getenv('MYSQL_CONNECTOR_PASSWORD'),
            database="grafana-test"
        )
        if conn.is_connected():
            print("Connected to MySQL successfully!")

        cursor = conn.cursor()

    except Exception as e:
        print(' Error in mysql connection:', e)

    insert_query = """
        INSERT INTO `grafana-test`.`grafana-test-table` (`timestamp`, `DE_load_forecasted_entsoe_transparency`,`DE_load_actual_entsoe_transparency`)
    VALUES (%s, %s, %s);
        """
    #cursor.execute(insert_query, (df['utc_timestamp'][i].strftime('%Y-%m-%d %H:%M:%S'), forecasted_value))
    print("Latest values injected to DB")
    cursor.execute(insert_query, (timestamp, forecasted_value,actual_value))
    conn.commit()
    cursor.close()
    conn.close()

