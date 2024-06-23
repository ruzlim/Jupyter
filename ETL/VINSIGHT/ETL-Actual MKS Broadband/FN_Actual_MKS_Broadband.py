
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


def src_update_to_fact(v_last_mth_fct, v_last_mth_src, v_target_schema, v_target_table, v_sql_update_fact):

    last_mth_fct = v_last_mth_fct
    last_mth_src = v_last_mth_src
    target_schema = v_target_schema
    target_table = v_target_table
    sql_update_fact = v_sql_update_fact

    v_param = dict(last_mth_fct=last_mth_fct, last_mth_src=last_mth_src)

    txt_param_input = f'\nParam input...\n   -> last_mth_fct\n   -> last_mth_src\n   -> target_schema\n   -> target_table\n   -> sql_update_fact'

    # Read : SQL file
    with open(f'SQL/{sql_update_fact}', 'r') as sql_file:
        queries = sql_file.read().split(';')
        query = queries[0].strip()
        sql_file.close()

    # Connect : TDMDBPR
    src_dsn = f'{TDMDBPR_user}/{TDMDBPR_pwd}@{TDMDBPR_host}:{TDMDBPR_port}/{TDMDBPR_db}'
    src_conn = oracledb.connect(src_dsn)
    print(f'\n{TDMDBPR_db} : Connected')
    src_cur = src_conn.cursor()

    # Connect : AKPIPRD
    tgt_dsn = f'{AKPIPRD_user}/{AKPIPRD_pwd}@{AKPIPRD_host}:{AKPIPRD_port}/{AKPIPRD_db}'
    tgt_conn = oracledb.connect(tgt_dsn)
    print(f'\n{AKPIPRD_db} : Connected')
    tgt_cur = tgt_conn.cursor()


    try:
        print(f'\nProcessing...')
        
        # Create Dataframe
        src_cur.execute(query, v_param)
        rows = src_cur.fetchall()
        print(f'\nCreate Dataframe...')
        src_df = pd.DataFrame.from_records(rows, columns=[x[0] for x in src_cur.description])
        print(f'\n   -> src_df : {src_df.shape[0]} rows, {src_df.shape[1]} columns') 

        # Truncate
        # tgt_cur.execute(f'TRUNCATE TABLE {target_schema}.{target_table}')
        # print(f'\n   -> TRUNCATE : "{target_table}" : Done !')

        # Delete
        tgt_cur.execute(f"""
            DELETE {target_schema}.{target_table} 
            WHERE TM_KEY_MTH > {last_mth_fct}
        """)
        print(f'\n   -> DELETE : "{target_table}" : Done !')
        
        # Insert
        tgt_cur.executemany(f"""
            INSERT INTO {target_schema}.{target_table}
            (TM_KEY_YR, TM_KEY_MTH, TRUE_TM_KEY_WK, TM_KEY_DAY, METRIC_CD, METRIC_NAME, COMP_CD, VERSION, AREA_NO, AREA_TYPE, AREA_CD, AREA_NAME, METRIC_VALUE, AGG_TYPE, FREQUENCY, REMARK) 
            VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16)
        """, rows)
        print(f'\n   -> INSERT : "{target_table}" : Done !')

        tgt_cur.close()
        tgt_conn.commit()
        

    except oracledb.DatabaseError as e:
        print(f'\nError with Oracle : {e}')


    finally:
        src_conn.close()
        print(f'\n{TDMDBPR_db} : Disconnected')
        tgt_conn.close()
        print(f'\n{AKPIPRD_db} : Disconnected')

        print(f'\nJob Done !!!')

    # return print(txt_param_input)


def main():
    pass


if __name__ == "__main__":
    main()