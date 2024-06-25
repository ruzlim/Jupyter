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
config.read('../../my_config.ini')
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

# ----------------------------------------------------------------------------------------------------------------------------------------------------


def my_metric_group(v_grp, v_cd, v_name):

    # Get : Parameter
    grp = v_grp
    cd = v_cd
    name = v_name
    flag = ''

    # Show : Parameter
    print(f'\nParam input...\n')
    print(f'   -> grp: {grp}')
    print(f'   -> cd: {cd}')
    print(f'   -> name: {name}')

    # CORP & MCOM
    if re.search('C$|H$|MCOM$', cd) and (not re.search('A[A-K]$', cd)): flag = 'CORP & MCOM'
    elif re.search('CUS$', cd): flag = 'Cust Location'
    # Revenue
    elif grp == 'Revenue' and any(x in name for x in ['New Revenue', 'Existing Revenue']): flag = 'New/Existing'
    elif grp == 'Revenue' and any(x in name for x in ['Paid Amount', 'On Due', 'Overdue']): flag = 'Paid Amount'
    elif grp == 'Revenue' and any(x in name for x in ['Revenue']): flag = 'Revenue'
    # Sales
    elif grp == 'Sales' and any(x in name for x in ['Inflow M1']): flag = 'Inflow M1'
    elif grp == 'Sales' and any(x in name for x in ['Inflow M2']): flag = 'Inflow M2'
    elif grp == 'Sales' and any(x in name for x in ['Gross Add']): flag = 'Gross Adds'
    elif grp == 'Sales' and any(x in name for x in ['%AP']): flag = '%AP'
    elif grp == 'Sales' and any(x in name for x in ['AP 1D']): flag = 'AP 1D'
    elif grp == 'Sales' and any(x in name for x in ['AP In Month']): flag = 'AP MTH'
    elif grp == 'Sales' and any(x in name for x in ['Activation Subs']): flag = 'Activation'
    elif grp == 'Sales' and any(x in name for x in ['Conversion']): flag = '%Conversion'
    elif grp == 'Sales' and any(x in name for x in ['GA ARPU', 'GA RC']): flag = 'GA ARPU/RC'
    # Subs
    elif grp == 'Subs' and any(x in name for x in ['Net Add']): flag = 'Net Adds'
    elif grp == 'Subs' and any(x in name for x in ['%NAD']): flag = '%NAD'
    elif grp == 'Subs' and any(x in name for x in ['%M4']): flag = '%M4'
    elif grp == 'Subs' and any(x in name for x in ['Reported Sub']): flag = 'Reported Subs'
    elif grp == 'Subs' and any(x in name for x in ['Usage Subs', 'Active Caller', 'Active Subs']): flag = 'Active Subs'
    elif grp == 'Subs' and any(x in name for x in ['NAD']): flag = 'NAD'
    elif grp == 'Subs' and any(x in name for x in ['Revenue Subs']): flag = 'Rev Subs'
    # MKS
    elif grp == 'Market Share' and any(x in name for x in ['Broadband']): flag = '%BB MKS'
    elif grp == 'Market Share' and (not any(x in name for x in ['Broadband'])) & any(x in name for x in ['(Subs)']): flag = 'MB MKS(Subs)'
    elif grp == 'Market Share' and (not any(x in name for x in ['Broadband', '(Subs)'])): flag = '%MB MKS'
    # Churn
    elif grp == 'Retention & Churn' and any(x in name for x in ['Churn Subs']): flag = 'Churn Subs'
    elif grp == 'Retention & Churn' and any(x in name for x in ['Churn Rate']): flag = '%Churn Rate'
    # Others
    elif any(x in name for x in ['ARPU']): flag = 'ARPU'
    elif any(x in name for x in ['SubBase']): flag = 'SubBase'
    elif any(x in name for x in ['New Subs']): flag = 'New Subs'
    elif any(x in name for x in ['Silent']): flag = 'Silent'
    elif any(x in name for x in ['60DPD']): flag = '60DPD'
    elif any(x in name for x in ['Quality']): flag = 'Quality'
    else: flag = 'Unknown'
 
    return flag

# ----------------------------------------------------------------------------------------------------------------------------------------------------


def src_update_to_fact(v_mth_end_fct, v_target_schema, v_target_table, v_sql_update_fact):

    # Get : Parameter
    mth_end_fct = v_mth_end_fct
    target_schema = v_target_schema
    target_table = v_target_table
    sql_update_fact = v_sql_update_fact
    v_query_param = dict(mth_end_fct=mth_end_fct)

    # Show : Parameter
    print(f'\nParam input...\n')
    print(f'   -> mth_end_fct: {mth_end_fct}')
    print(f'   -> target_schema: {target_schema}')
    print(f'   -> target_table: {target_table}')
    print(f'   -> sql_update_fact: {sql_update_fact}')
    print(f'   -> v_query_param: {v_query_param}')

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
        src_cur.execute(query, v_query_param)
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
            WHERE TM_KEY_MTH > {mth_end_fct}
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

# ----------------------------------------------------------------------------------------------------------------------------------------------------


def src_initial_to_fact(v_initial_mth_start, v_initial_mth_end, v_target_schema, v_target_table, v_sql_initial_fact):

    # Get : Parameter
    initial_mth_start = v_initial_mth_start
    initial_mth_end = v_initial_mth_end
    target_schema = v_target_schema
    target_table = v_target_table
    sql_initial_fact = v_sql_initial_fact
    v_query_param = dict(initial_mth_start=initial_mth_start, initial_mth_end=initial_mth_end)

    # Show : Parameter
    print(f'\nParam input...\n')
    print(f'   -> initial_mth_start: {initial_mth_start}')
    print(f'   -> initial_mth_end: {initial_mth_end}')
    print(f'   -> target_schema: {target_schema}')
    print(f'   -> target_table: {target_table}')
    print(f'   -> sql_initial_fact: {sql_initial_fact}')
    print(f'   -> v_query_param: {v_query_param}')

    # Read : SQL file
    with open(f'SQL/{sql_initial_fact}', 'r') as sql_file:
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
        src_cur.execute(query, v_query_param)
        rows = src_cur.fetchall()
        print(f'\nCreate Dataframe...')
        src_df = pd.DataFrame.from_records(rows, columns=[x[0] for x in src_cur.description])
        print(f'\n   -> src_df : {src_df.shape[0]} rows, {src_df.shape[1]} columns') 

        # Delete
        tgt_cur.execute(f"""
            DELETE {target_schema}.{target_table} 
            WHERE TM_KEY_MTH BETWEEN {initial_mth_start} AND {initial_mth_end}
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

# ----------------------------------------------------------------------------------------------------------------------------------------------------


def mockup_to_fact(v_mth_end_fct, v_target_schema, v_target_table, v_sql_mockup_fact):

    # Get : Parameter
    mth_end_fct = v_mth_end_fct
    target_schema = v_target_schema
    target_table = v_target_table
    sql_mockup_fact = v_sql_mockup_fact
    v_query_param = dict(mth_end_fct=mth_end_fct)

    # Show : Parameter
    print(f'\nParam input...\n')
    print(f'   -> mth_end_fct: {mth_end_fct}')
    print(f'   -> target_schema: {target_schema}')
    print(f'   -> target_table: {target_table}')
    print(f'   -> sql_mockup_fact: {sql_mockup_fact}')
    print(f'   -> v_query_param: {v_query_param}')

    # Read : SQL file
    with open(f'SQL/{sql_mockup_fact}', 'r') as sql_file:
        queries = sql_file.read().split(';')
        query = queries[0].strip()
        sql_file.close()
        
    # Connect : AKPIPRD
    tgt_dsn = f'{AKPIPRD_user}/{AKPIPRD_pwd}@{AKPIPRD_host}:{AKPIPRD_port}/{AKPIPRD_db}'
    tgt_conn = oracledb.connect(tgt_dsn)
    print(f'\n{AKPIPRD_db} : Connected')
    tgt_cur = tgt_conn.cursor()


    try:
        print(f'\nProcessing...')

        # Delete
        tgt_cur.execute(f"""
            DELETE {target_schema}.{target_table} 
            WHERE TM_KEY_MTH > {mth_end_fct}
        """)
        print(f'\n   -> DELETE : "{target_table}" : Done !')
        
        # Insert
        tgt_cur.execute(query, v_query_param)
        print(f'\n   -> INSERT : "{target_table}" : Done !')
        tgt_cur.close()
        tgt_conn.commit()
        
        # rows = tgt_cur.fetchall()
        # mock_df = pd.DataFrame.from_records(rows, columns=[x[0] for x in tgt_cur.description])
        # print(f'\n   -> mock_df : {mock_df.shape[0]} rows, {mock_df.shape[1]} columns') 


    except oracledb.DatabaseError as e:
        print(f'\nError with Oracle : {e}')


    finally:
        tgt_conn.close()
        print(f'\n{AKPIPRD_db} : Disconnected')

# ----------------------------------------------------------------------------------------------------------------------------------------------------


def main():
    pass


if __name__ == "__main__":
    main()