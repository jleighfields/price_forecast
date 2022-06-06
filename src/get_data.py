# get raw data from miso, format, and save

import pandas as pd
import numpy as np
import datetime
import time
import os
from typing import List

import sqlite3
import sqlalchemy
from sqlalchemy import create_engine

import warnings
warnings.filterwarnings("ignore")


# url examples
# hourly forecasted and actual load by region, starts 20200101
# https://docs.misoenergy.org/marketreports/20210214_rf_al.xls
# hourly generation mix by region, starts 20200101
# https://docs.misoenergy.org/marketreports/20210214_sr_gfm.xlsx
# hourly region price report, starts 20200101
# https://docs.misoenergy.org/marketreports/20220603_rt_pr.xls

# base url for imports
base_url = 'https://docs.misoenergy.org/marketreports/'
load_file = '_rf_al.xls'
generation_file = '_sr_gfm.xlsx'
price_file = '_rt_pr.xls'

# list of days to query
start = datetime.datetime(2020, 1, 1)
end = datetime.datetime(2022, 6, 1)
days = pd.date_range(start, end)

# get YYYYMMDD string from date
days_str = [str(d).split(' ')[0].replace('-','') for d in days]


###############################################################
# create database, tables, indexes
###############################################################

# create database, tables, and indexes
def create_database():

    PROJECT_SRC = '/workspace/src'
    os.chdir(PROJECT_SRC)

    SQLALCHEMY_DATABASE_URI='sqlite:///../data/database.db'
    engine = create_engine(SQLALCHEMY_DATABASE_URI, echo=False)

    with engine.connect() as conn:

        gen_table='''
        CREATE TABLE IF NOT EXISTS GENERATION (
        dttm TEXT PRIMARY KEY, 
        wind REAL,
        solar REAL
        );'''
        conn.execute(gen_table)

        gen_idx='''
        CREATE UNIQUE INDEX IF NOT EXISTS GENERATION_IDX
        ON GENERATION(dttm);
        '''
        conn.execute(gen_idx)


        load_table='''
        CREATE TABLE IF NOT EXISTS LOAD (
        dttm TEXT PRIMARY KEY, 
        load_mwh REAL
        );'''
        conn.execute(load_table)

        load_idx='''
        CREATE UNIQUE INDEX IF NOT EXISTS LOAD_IDX
        ON LOAD(dttm);
        '''
        conn.execute(load_idx)

        price_table='''
        CREATE TABLE IF NOT EXISTS PRICE (
        dttm TEXT PRIMARY KEY, 
        price REAL
        );'''
        conn.execute(price_table)

        price_idx='''
        CREATE UNIQUE INDEX IF NOT EXISTS PRICE_IDX
        ON PRICE(dttm);
        '''
        conn.execute(price_idx)
        
    return engine

    
###############################################################
# functions to upsert data
###############################################################

def upsert_generation(
    generation_data: pd.core.frame.DataFrame, 
    engine: sqlalchemy.engine.base.Engine,
):
    
    with engine.connect() as conn:

        generation_data.to_sql('GENERATION_TMP', engine, if_exists='replace')

        upsert='''INSERT INTO GENERATION 
        SELECT *
        FROM GENERATION_TMP WHERE true
        ON CONFLICT (dttm) DO UPDATE SET Wind=excluded.Wind, Solar=excluded.Solar;
        '''

        conn.execute(upsert)

        drop_tbl='DROP TABLE GENERATION_TMP'
        conn.execute(drop_tbl)
        
        
def upsert_load(
    load_data: pd.core.frame.DataFrame, 
    engine: sqlalchemy.engine.base.Engine,
):
    
    with engine.connect() as conn:

        load_data.to_sql('LOAD_TMP', engine, if_exists='replace')

        upsert='''INSERT INTO LOAD 
        SELECT *
        FROM LOAD_TMP WHERE true
        ON CONFLICT (dttm) DO UPDATE SET load_mwh=excluded.load_mwh;
        '''

        conn.execute(upsert)

        drop_tbl='DROP TABLE LOAD_TMP'
        conn.execute(drop_tbl)
        
        
def upsert_prices(
    price_data: pd.core.frame.DataFrame, 
    engine: sqlalchemy.engine.base.Engine,
):
    
    with engine.connect() as conn:

        price_data.to_sql('PRICE_TMP', engine, if_exists='replace')

        upsert='''INSERT INTO PRICE 
        SELECT *
        FROM PRICE_TMP WHERE true
        ON CONFLICT (dttm) DO UPDATE SET price=excluded.price;
        '''

        conn.execute(upsert)

        drop_tbl='DROP TABLE PRICE_TMP'
        conn.execute(drop_tbl)
    
    
###############################################################
# define functions to get data
###############################################################

def get_load_data(
    days_idx: int, 
    base_url: str = base_url, 
    days_str: List[str] = days_str, 
    file: str = load_file,
):
    # build url and fetch data
    url = base_url + days_str[days_idx] + file
    load_data = pd.read_excel(url, header=1, skiprows=4).iloc[1:26,1:]
    
    # remove rows with missing hour ending values and rename columns
    load_data = load_data[~load_data.HourEnding.isna()]
    load_data = load_data[['Market Day', 'HourEnding', 'North ActualLoad (MWh)']]
    load_data = load_data.rename(
        columns={'Market Day':'day',
                'HourEnding':'he',
                'North ActualLoad (MWh)':'load_mwh'}
    )
    
    # convert data types
    load_data['he'] = load_data.he.astype(int)
    load_data['load_mwh'] = load_data.load_mwh.astype(float)
    load_data.day= pd.to_datetime(load_data.day)
    
    # create dttm index
    load_data['dttm'] = (
        load_data.day + 
        pd.to_timedelta(load_data.he, unit='h')
    )
    
    load_data = load_data[['dttm', 'load_mwh']].set_index('dttm')
    
    return load_data


def get_generation_data(
    days_idx: int, 
    base_url: str = base_url, 
    days_str: List[str] = days_str, 
    file: str = generation_file,
):
    # build url and fetch data
    url = base_url + days_str[days_idx] + file
    generation_data = pd.read_excel(url, skiprows=2, header=[1,2])
    
    # save he data
    # he = hour ending
    he = generation_data[('Unnamed: 0_level_0', 'Market Hour Ending')]
    
    # get wind, solar, other generation from north region
    generation_data = generation_data['North']
    cols = [c for c in generation_data.columns if c in ['Wind', 'Solar', 'Other']]
    generation_data = generation_data[cols]
    
    # set he data and remove non numeric values
    generation_data['he'] = he
    idx = generation_data.he.str.isnumeric() != False
    generation_data = generation_data[idx]
    
    # if solar generation was not reported estimate generation from other
    # by removing subtracting off night generation
    if 'Solar' not in generation_data.columns:
        generation_data['Solar'] = (generation_data.Other - 
                                    np.max((generation_data.Other[:5].max(), generation_data.Other[-5:].max()))
                                   )
        generation_data.loc[generation_data.Solar < 0, 'Solar'] = 0
    
    generation_data.drop('Other', axis=1, inplace=True)
    
    # create dttm index
    generation_data['day'] = pd.to_datetime(days_str[days_idx])
    
    generation_data['dttm'] = (
        generation_data.day + 
        pd.to_timedelta(generation_data.he, unit='h')
    )
    generation_data = generation_data.drop(['he', 'day'], axis=1).set_index('dttm')
    
    return generation_data


def get_price_data(
    days_idx: int, 
    base_url: str = base_url, 
    days_str: List[str] = days_str, 
    file: str = price_file,
):
    # build url and fetch data
    url = base_url + days_str[days_idx] + file
    price_data = pd.read_excel(url, skiprows=11, header=0)
    
    # will use minnesota hub prices
    # he = hour ending
    price_data = price_data.rename(columns={'Unnamed: 0':'he', 'Minnesota Hub':'prices'})
    
    # get only he row containing Hour
    idx = ['Hour' in he for he in price_data.he]
    price_data = price_data.loc[idx, ['he', 'prices']]
    
    # set day and get he as int
    price_data['day'] = pd.to_datetime(days_str[days_idx])
    price_data['he'] = [int(he.split()[1]) for he in price_data.he]
    
    # get dttm and drop day and he
    price_data['dttm'] = (
        price_data.day + 
        pd.to_timedelta(price_data.he, unit='h')
    )
    price_data = price_data.drop(['he', 'day'], axis=1).set_index('dttm')
    
    return price_data



def main():
    
    engine = create_database()
    
    for i in range(len(days_str)):
        print('\n_____________________________')
        print(f'working on day: {days_str[i]}')

        print('\t-getting load...')
        load_data = get_load_data(i)
        upsert_load(load_data, engine)
        print(f'\t\tload_data shape: {load_data.shape}')

        print('\t-getting generation...')
        generation_data = get_generation_data(i)
        upsert_generation(generation_data, engine)
        print(f'\t\tgeneration_data shape: {generation_data.shape}')

        print('\t-getting prices...')
        price_data = get_price_data(i)
        upsert_prices(price_data, engine)
        print(f'\t\tprice_data shape: {price_data.shape}')

        time.sleep(1)
        



if __name__ == '__main__':
    main()