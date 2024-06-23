
# import os
# import argparse
# import logging
# from glob import glob 

import configparser
import datetime as dt
import pandas as pd
import numpy as np
import oracledb
import re

config = configparser.ConfigParser()
config.read('../../../my_config.ini')
config.sections()

TDMDBPR_user = config['TDMDBPR']['username']
TDMDBPR_pwd = config['TDMDBPR']['password']
TDMDBPR_db = config['TDMDBPR']['db']
TDMDBPR_host = config['TDMDBPR']['host']
TDMDBPR_port = config['TDMDBPR']['port']

AKPIPRD_user = config['AKPIPRD']['username']
AKPIPRD_pwd = config['AKPIPRD']['password']
AKPIPRD_db = config['AKPIPRD']['db']
AKPIPRD_host = config['AKPIPRD']['host']
AKPIPRD_port = config['AKPIPRD']['port']



# Test
def sayhi():
    text = f'Hello Ruz!!!'
    return text


# Test
def bye():
    text = f'By Ruz!!!'
    return text


def src_update():

    # Read : SQL file
    with open('SQL/Import-TEST.sql', 'r') as sql_file:
        queries = sql_file.read().split(';')
        query = queries[0].strip()
        sql_file.close()
    # print(f'\n{query}')

    # Connect : TDMDBPR
    src_dsn = f'{TDMDBPR_user}/{TDMDBPR_pwd}@{TDMDBPR_host}:{TDMDBPR_port}/{TDMDBPR_db}'
    src_conn = oracledb.connect(src_dsn)
    print(f'\n{TDMDBPR_db} : Connected')
    src_cur = src_conn.cursor()

    try:
    # Create Dataframe
        src_cur.execute(query) # , v_param
        rows = src_cur.fetchall()
        print(f'\nCreate Dataframe...')
        src_df = pd.DataFrame.from_records(rows, columns=[x[0] for x in src_cur.description])
        print(f'\n   -> src_df : {src_df.shape[0]} rows, {src_df.shape[1]} columns') 

        print(f'\n{src_df}')

        src_cur.close()

    except oracledb.DatabaseError as e:
        print(f'\nError with Oracle : {e}')

    finally:
        src_conn.close()
        print(f'\n{TDMDBPR_db} : Disconnected')

    return 'src_update!!!'


# Main
def main():
    pass
    # sayhi()
    # bye()

    # src_update()

    # print(f'\n   -> {sayhi()}\n')

    # print(f'\n   -> {src_update()}\n')


if __name__ == "__main__":
    main()
