#!/usr/bin/env python3

import requests
import json
import datetime
import pandas as pd
import sqlite3

pd.set_option('display.width', 1000)
columns = ['lastYearRptDt', 'lastYearEPS', 'time', 'symbol', 'name', 'marketCap', 'fiscalQuarterEnding', 'epsForecast', 'noOfEsts']
default_stocks = ['AFRM', 'SNOW', 'SNAP', 'MDB', 'CRM', 'NVDA','BYND', 'AMZN', 'NFLX', 'GOOGL', 'ADBE', 'ZM', 'SQ', 'DBX', 'ABNB', 'AMD', 'FB', 'BABA', 'MSFT', 'ROKU', 'AAPL', 'TSLA']
default_days = 30

def dateconvert(fmt1, fmt2, date):
    try:
        return datetime.datetime.strptime(date, fmt1).strftime(fmt2)
    except:
        return 'NA'

def getEarningScheduleOnDate(date):    
    url = 'https://api.nasdaq.com/api/calendar/earnings'
    headers = {
        'authority': 'api.nasdaq.com',
        'sec-ch-ua': '"Google Chrome";v="87", " Not;A Brand";v="99", "Chromium";v="87"',
        'accept': 'application/json, text/plain, */*',
        'sec-ch-ua-mobile': '?0',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 11_1_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.141 Safari/537.36',
        'origin': 'https://www.nasdaq.com',
        'sec-fetch-site': 'same-site',
        'sec-fetch-mode': 'cors',
        'sec-fetch-dest': 'empty',
        'referer': 'https://www.nasdaq.com/',
        'accept-language': 'en-US,en;q=0.9,ar;q=0.8,cs;q=0.7,fr;q=0.6,it;q=0.5,zh-CN;q=0.4,zh-TW;q=0.3,zh;q=0.2'
    }
    payload = {'date' : date.strftime('%Y-%m-%d')}
    resp = requests.get(url, params=payload, headers=headers)
    data = resp.json()
    if(data['data']['rows']):
        df = pd.DataFrame(data['data']['rows'])
    else:
        df = pd.DataFrame(columns=columns)
        
    #Convert JSON rows into a DataFrame and clean up 
    df['lastYearRptDt'] = df['lastYearRptDt'].apply(lambda x : dateconvert('%m/%d/%Y', '%Y-%m-%d', x))
    df['fiscalQuarterEnding'] = df['fiscalQuarterEnding'].apply(lambda x : dateconvert('%b/%Y', '%Y-%m', x))
    df['RptDt'] = date.strftime('%Y-%m-%d')
    df = df.set_index('symbol')
    
    return df

def saveEarningSchedule(df):
    conn = sqlite3.connect('db.sqlite')
    df.to_sql('EarningSchedule', conn, if_exists='replace', index=False)
    conn.close()

    
def getEarningSchedule2(symbols):
    query = "select * from EarningSchedule where symbol in {}".format(tuple(symbols))
    print(query)
    conn = sqlite3.connect('db.sqlite')
    df = pd.read_sql(query, conn)
    conn.close()
    return df

def getEarningSchedule(stocks=default_stocks, ndays=default_days):
    start = datetime.datetime.now()
    schedule = pd.DataFrame()
    for i in range(ndays):
        date = start + datetime.timedelta(days=i)
        schedule = pd.concat([schedule, getEarningScheduleOnDate(date)])
        
    stock_earing_schedule = schedule.loc[schedule.index.isin(stocks)]
    return stock_earing_schedule.loc[:, ['RptDt', 'epsForecast', 'noOfEsts']]

if __name__ == "__main__":
    print(getEarningSchedule())