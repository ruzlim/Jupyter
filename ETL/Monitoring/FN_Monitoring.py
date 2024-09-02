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

# ----------------------------------------------------------------------------------------------------------------------------------------------------


def my_metric_group(v_grp, v_cd, v_name):

    # Get : Parameter
    grp = v_grp
    cd = v_cd
    name = v_name
    flag = ''

    # # Show : Parameter
    # print(f'\nParam input...\n')
    # print(f'   -> grp: {grp}')
    # print(f'   -> cd: {cd}')
    # print(f'   -> name: {name}')

    # CORP & MCOM
    if re.search('C$|H$|MCOM$', cd) and (not re.search('A[A-K]$', cd)): flag = 'CORP & MCOM'
    elif re.search('CUS$', cd): flag = 'Cust Location'
    # Finance
    elif grp == 'Finance': flag = 'Corporate'
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


def main():
    pass


if __name__ == "__main__":
    main()