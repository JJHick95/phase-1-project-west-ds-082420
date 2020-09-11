import psycopg2
import pandas as pd
import matplotlib.pyplot as plt
DBNAME = "opportunity_youth"
conn = psycopg2.connect(dbname=DBNAME)


def get_oy_db():
    #fetching oportunity youth in south king county
    skc_OY_df = pd.read_sql('''
        SELECT *
        FROM pums_2017
        WHERE puma SIMILAR TO '1161(0|1|2|3|4|5)'
        AND agep >= 16
        AND agep <= 24
        AND sch = '1'
        AND esr SIMILAR TO '%(3|6)%'
        ''', conn)
    
    return skc_OY_df

def get_all_youth_db():
    #fetching all residents from south king county within the OY age group

    skc_allRes_df = pd.read_sql('''
        SELECT *
        FROM pums_2017
        WHERE puma SIMILAR TO '1161(0|1|2|3|4|5)'
        AND agep >= 16
        AND agep <= 24
        ''', conn)
    
    return skc_allRes_df

def get_oy_2016_db():
    
    #fetching all opportunity youth from south king county in 2016
    
    csv_file_name = 'ss16pwa.csv'
    oy_2016_df = pd.read_csv(csv_file_name)
    puma_mask = oy_2016_df['PUMA'].isin(['11610', '11611', '11612', '11613', '11614', '11615'])
    oy_2016_df = oy_2016_df.loc[puma_mask]
    oy_mask = (oy_2016_df['AGEP'] >= 16) & (oy_2016_df['AGEP'] <= 24) & (oy_2016_df['SCH'].isin(['1'])) & (oy_2016_df['ESR'].isin(['3', '6']))
    oy_2016_df = oy_2016_df.loc[oy_mask]
    return oy_2016_df


def get_skc_oy_race():
    '''
    returns a dictionary with race names as keys and their coresponding pop_count as values
    '''
    skc_OY_df = get_oy_db()
    
    race_dict = {'1': 'White', '2': 'Black/ African American',
                 '3': 'American Indian or Alaska Native', '4': 'American Indian or Alaska Native',
                 '5': 'American Indian or Alaska Native', '6': 'Asian', '7': 'Native Hawaian/ Paciffic Islander',
                 '8': 'Other', '9': 'Two or More Races'}
    race_breakdown = skc_OY_df.groupby(by='rac1p').sum()['pwgtp']
    out_dict = {}
    for index in race_breakdown.index:
        if index in ['4', '5']:
            out_dict[race_dict[index]] += race_breakdown[index]
        else:
            out_dict[race_dict[index]] = race_breakdown[index]
    return out_dict


def get_skc_all_youth_race():
    '''
    returns a dictionary with race names as keys and their coresponding pop_count as values for all skc youth
    '''
    skc_allRes_df = get_all_youth_db()
    
    race_dict = {'1': 'White', '2': 'Black/ African American',
                 '3': 'American Indian or Alaska Native', '4': 'American Indian or Alaska Native',
                 '5': 'American Indian or Alaska Native', '6': 'Asian', '7': 'Native Hawaian/ Paciffic Islander',
                 '8': 'Other', '9': 'Two or More Races'}
    race_breakdown = skc_allRes_df.groupby(by='rac1p').sum()['pwgtp']
    out_dict = {}
    for index in race_breakdown.index:
        if index in ['4', '5']:
            out_dict[race_dict[index]] += race_breakdown[index]
        else:
            out_dict[race_dict[index]] = race_breakdown[index]
    return out_dict


def get_pums_youth_count():
    '''
    returns a dictionary with puma ID number as keys and their corresponding total youth count as values
    '''
    skc_all_youth_df = get_all_youth_db()
    
    puma_breakdown = skc_all_youth_df.groupby(by='puma').sum()['pwgtp']
    return puma_breakdown.to_dict()


def get_pums_oy_count():
    '''
    returns a dictionary with puma ID number as keys and their corresponding opportunity youth count as values
    '''
    skc_oy_df = get_oy_db()
    
    puma_breakdown = skc_oy_df.groupby(by='puma').sum()['pwgtp']
    return puma_breakdown.to_dict()











