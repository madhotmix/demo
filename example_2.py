# This is a script to automatically download advertising campaign data via api

import pandas as pd
import numpy as np
import requests
import csv
import sys
import re
import example_3  # My own function library

def instagram_campaign(x):
    try:
        return str(x[0]).replace('.0', '')
    except:
        return

def gender_age_geo(x, i):
    if len(x.split('_')) == 3:
        return str(x.split('_')[i])
    else:
        return
    
def publisher_campaign_id(x, i):
    if len(x.split('_')) == 2:
        return str(x.split('_')[i])
    else:
        return

def sequence(date_start, date_stop, name, data, engine):
    example_3.remove_from_sql(name, date_start, date_stop, engine, 'date')
    example_3.remove_from_sql(f'{name}_events', date_start, date_stop, engine, 'date')
    full_len = example_3.get_full_len(name, engine)
    df, df_events = events_processing(full_len, data)
    df = example_3.rename_columns_for_sql(df)
    df_events = example_3.rename_columns_for_sql(df_events)
    example_3.df_to_sql(df, name, date_start, date_stop, engine, 'date')
    example_3.df_to_sql(df_events, f'{name}_events', date_start, date_stop, engine, 'date')

def offer_0(date_start, date_stop, user_token, name, app_token, timezone, engine, s3):
    try:
        data = response_api_installs(date_start, date_stop, user_token, app_token, name, timezone, s3)
        data = offer_0_processing(data, engine)
        data = change_dtype(data)
        sequence(date_start, date_stop, name, data, engine)
    except:
        example_3.log_write('fail', name, date_start, date_stop)

def offer_0_processing(data, engine):
    data.loc[data['Network'] == 'clickheaven_mt', 'gender'] = data['Creative'].apply(lambda x: gender_age_geo(x, 0))
    data.loc[data['Network'] == 'clickheaven_mt', 'age'] = data['Creative'].apply(lambda x: gender_age_geo(x, 1))
    data.loc[data['Network'] == 'clickheaven_mt', 'geo'] = data['Creative'].apply(lambda x: gender_age_geo(x, 2))
    data.loc[data['Network'] == 'clickheaven_mt', 'source'] = 'mytarget'
    data['geo_code'] = data['geo'].apply(lambda x: str(x).replace('.0', ''))
    geo_dict = pd.read_sql("""SELECT * FROM geo_codes""", engine).set_index('code').to_dict()['name']
    data['geo'] = data['geo'].map(geo_dict).fillna(data['geo'])
    data.loc[data['Network'] == 'Instagram Installs', 'publisher'] = data['Campaign'].str.findall('\d{4}').apply(lambda x: instagram_campaign(x))
    data.loc[data['Network'] == 'Instagram Installs', 'source'] = 'facebook'
    data.loc[data['Network'] == 'clickheaven_mt', 'publisher'] = data['Campaign']
    data.loc[data['Network'] == 'Instagram Installs', 'campaign_id'] = data['Campaign'].str.findall('\(\d{0,}\)').apply(lambda x: str(x)[3:-3])
    data.loc[data['Network'] == 'clickheaven_mt', 'campaign_id'] = data['Adgroup']
    data['campaign_id'] = data['campaign_id'].apply(lambda x: str(x).replace('.0', ''))
    del data['Creative']
    return data.rename(columns={'OS Name': 'platform'})

def offer_2_3(date_start, date_stop, user_token, name, app_token, timezone, engine, s3):
    try:
        data = response_api_installs(date_start, date_stop, user_token, app_token, name, timezone, s3)
        data = change_dtype(data)
        mt = offer_2_3_processing_mt(data, engine)
        fbi = offer_2_3_processing_fbi(data)
        data = offer_2_3_processing(mt, fbi)
        sequence(date_start, date_stop, name, data, engine)
    except:
        example_3.log_write('fail', name, date_start, date_stop)

def parse_creative(x, i):
    try: return x.split('_')[i]
    except IndexError: return

def offer_2_3_processing_mt(mt, engine):
    mt = mt[mt['Network'] == 'ClickHeaven']
    geo_dict = pd.read_sql("""SELECT * FROM geo_codes""", engine).set_index('code').to_dict()['name']
    mt.loc[mt['Network'] == 'ClickHeaven', 'geo'] = mt['Adgroup'].map(geo_dict).fillna(mt['Adgroup'])
    mt['Creative'] = mt['Creative'].apply(lambda x: x.replace('banner_id', 'banner id'))
    mt['publisher'] = mt['Creative'].apply(lambda x: parse_creative(x, 0))
    mt['banner_id'] = mt['Creative'].apply(lambda x: parse_creative(x, 1))
    mt['gender'] = mt['Creative'].apply(lambda x: parse_creative(x, 2))
    mt['age'] = mt['Creative'].apply(lambda x: parse_creative(x, 3))
    mt['soure'] = 'mytraget'
    return mt

def offer_2_3_processing_fbi(fbi):
    fbi = fbi[(fbi['Network'] == 'Facebook Installs') | (fbi['Network'] == 'Instagram Installs')]
    fbi['campaign_name'] = fbi['Campaign']
    fbi['publisher'] = fbi['Campaign'].apply(lambda x: parse_creative(x, 2))
    fbi['campaign_id'] = fbi['Campaign'].str.extract(r'(\d{17})')
    fbi['Campaign'] = fbi['Campaign'].apply(lambda x: parse_creative(x, 1))
    fbi['banner_id'] = fbi['Creative'].str.extract(r'(\d{17})')
    fbi.loc[fbi['Network'] == 'Facebook Installs', 'source'] = 'facebook'
    fbi.loc[fbi['Network'] == 'Instagram Installs', 'source'] = 'instagram'
    return fbi

def offer_2_3_processing(mt, fbi):
    data = pd.concat([mt, fbi], sort=False).reset_index(drop=True)
    data = data.rename(columns={'OS Name': 'platform'})
    data['publisher'] = data['publisher'].apply(lambda x: x.replace('1957X', '1957'))
    return data

def offer_1(date_start, date_stop, user_token, name, app_token_android, app_token_ios, timezone, engine, s3):
    data_android = response_api_installs(date_start, date_stop, user_token, app_token_android, name, timezone, s3)
    data_ios = response_api_installs(date_start, date_stop, user_token, app_token_ios, name, timezone, s3)
    data = pd.concat([data_android, data_ios], sort=False).reset_index(drop=True)
    data = offer_1(data, engine)
    data = change_dtype(data)
    sequence(date_start, date_stop, name, data, engine)
        
def offer_1_processing(data, engine):
    data['publisher'] = data['Campaign'].apply(lambda x: publisher_campaign_id(x, 0))
    data['campaign_id'] = data['Campaign'].apply(lambda x: publisher_campaign_id(x, 1)).apply(lambda x: str(x).replace(')}', ''))
    data['age'] = data['Creative'].apply(lambda x: gender_age_geo(x, 2))
    data['geo_code'] = data['Creative'].apply(lambda x: gender_age_geo(x, 0))
    data['gender'] = data['Creative'].apply(lambda x: gender_age_geo(x, 1))
    geo_dict = pd.read_sql("""SELECT * FROM geo_codes""", engine).set_index('code').to_dict()['name']
    data['geo'] = data['geo_code'].map(geo_dict).fillna(data['geo_code'])
    data.loc[data['Network'] == 'Clickheaven MT', 'source'] = 'mytarget'
    del data['Campaign'], data['Creative']
    return data.rename(columns={'OS Name': 'platform', 'Adgroup': 'banner_id'})

def response_api_installs(date_start, date_stop, user_token, app_token, name, timezone, s3):
    print(f'installs {name}, {date_start} - {date_stop}, {pd.Timestamp.now()}')
    headers = {'Accept': 'text/csv'}
    params = {'user_token': user_token, 'start_date': date_start, 'end_date': date_stop, 'grouping': 'day,networks,campaigns,adgroups,creatives,countries,os_names',
              'Accept': 'text/csv', 'human_readable_kpis': 'true', 'utc_offset': timezone, 'Content-Type': 'text/csv'}
    params['kpis'] = ','.join(['clicks', 'impressions', 'installs', 'uninstalls', 'uninstall_cohort', 'reinstalls', 'click_conversion_rate', 'ctr', 
                               'impression_conversion_rate', 'reattributions', 'reattribution_reinstalls', 'deattributions', 'sessions', 'revenue_events', 
                               'revenue','cohort_revenue', 'daus', 'waus', 'maus', 'limit_ad_tracking_installs', 'limit_ad_tracking_install_rate', 
                               'limit_ad_tracking_reattributions', 'limit_ad_tracking_reattribution_rate', 'gdpr_forgets', 'events', 'first_events', 
                               'revenue_per_event', 'revenue_per_revenue_event', 'rejected_installs', 'rejected_installs_anon_ip',
                               'rejected_installs_too_many_engagements', 'rejected_installs_distribution_outlier', 'rejected_installs_click_injection',
                               'rejected_installs_invalid_signature','rejected_reattributions','rejected_reattributions_anon_ip',
                               'rejected_reattributions_too_many_engagements','rejected_reattributions_distribution_outlier','rejected_install_rate',
                               'rejected_install_anon_ip_rate','rejected_install_too_many_engagements_rate','rejected_install_distribution_outlier_rate',
                               'rejected_install_click_injection_rate','rejected_reattribution_rate','rejected_reattribution_anon_ip_rate',
                               'rejected_reattribution_too_many_engagements_rate','rejected_reattribution_distribution_outlier_rate',
                               'install_cost', 'click_cost', 'impression_cost', 'cost', 'paid_installs','paid_clicks', 'paid_impressions', 
                               'ecpc', 'ecpm', 'ecpi', 'cohort_gross_profit', 'return_on_investment'])
    params['event_kpis'] = 'all_revenue|events|revenue_events|first_events|revenue_per_event|revenue_per_revenue_event'
    response = requests.get("https://api.adjust.com/kpis/v1/" + app_token, params=params, headers=headers)
    return example_3.response_processing(response, date_start, date_stop, name, '', s3)

def events_processing(full_len, data):
    data.index = pd.RangeIndex(full_len, full_len + len(data))
    data['installs_index'] = data.index
    df_events = pd.DataFrame()
    for col in [s for s in data.columns.tolist() if '(' in s]:
        df = data[data[col].notnull()][['installs_index', 'Date', col]]
        df['event_name'] = '('.join(col.split(' (')[:-2])
        df['event_token'] = col.split(' (')[-2][:-1]
        df['event_class'] = col.split(' (')[-1][:-1]
        df = df.rename(columns={col: 'events'})
        df['events'] = df['events'].astype(float)
        for col_cat in ['event_name', 'event_token', 'event_class']:
            df[col_cat] = df[col_cat].astype('category')
        df_events = pd.concat([df_events, df], sort=False)
        del data[col]
    return data, df_events

def change_dtype(df):
    for col in ['Clicks', 'impressions', 'Installs', 'uninstalls', 'uninstall_cohort', 'reinstalls', 'Reattributions', 'reattribution_reinstalls', 'deattributions', 'Sessions', 'Daily Active Users', 'Weekly Active Users', 'Monthly Active Users', 'limit_ad_tracking_installs', 'limit_ad_tracking_reattributions', 'Events', 'First_events', 'rejected_installs', 'rejected_installs_anon_ip', 'rejected_installs_too_many_engagements', 'rejected_installs_distribution_outlier', 'rejected_installs_click_injection', 'rejected_installs_invalid_signature', 'rejected_reattributions', 'rejected_reattributions_anon_ip', 'rejected_reattributions_too_many_engagements', 'rejected_reattributions_distribution_outlier', 'paid_installs', 'paid_clicks', 'paid_impressions']:
        if col in df.columns:
            df[col] = df[col].astype(str).replace('nan', 0).fillna(0).astype(int)
    for col in ['Conversion', 'ctr', 'impression_conversion_rate', 'Revenue Events', 'Revenue', 'cohort_revenue', 'limit_ad_tracking_install_rate', 'limit_ad_tracking_reattribution_rate', 'gdpr_forgets', 'Revenue per Event', 'Revenue per Revenue Event', 'rejected_install_rate', 'rejected_install_anon_ip_rate', 'rejected_install_too_many_engagements_rate', 'rejected_install_distribution_outlier_rate', 'rejected_install_click_injection_rate', 'rejected_reattribution_rate', 'rejected_reattribution_anon_ip_rate', 'rejected_reattribution_too_many_engagements_rate', 'rejected_reattribution_distribution_outlier_rate', 'install_cost', 'click_cost', 'impression_cost', 'cost', 'ecpc', 'ecpm', 'ecpi', 'cohort_gross_profit', 'return_on_investment']:
        if col in df.columns:
            df[col] = df[col].astype(str).replace('nan', 0).fillna(0).astype('float16')
    return df
            
def daily(engine, s3, date_start, date_stop):
    user_token = 'user_token'
    timezone = '03:00'
    
    name = 'offer_0'
    app_token = 'token_0'
    offer_0(date_start, date_stop, user_token, name, app_token, timezone, engine, s3)

    name = 'offer_1'
    app_token_android = 'token_1_android'
    app_token_ios = 'token_1_ios'
    offer_1(date_start, date_stop, user_token, name, app_token_android, app_token_ios, timezone, engine, s3)
    
    if pd.Timestamp.today().dayofweek == 0:
        timezone = '00:00'
        name = 'offer_2'
        app_token = 'token_2'
        offers_2_3(date_start, date_stop, user_token, name, app_token, timezone, engine, s3)
        name = 'offer_3'
        app_token = 'token_3'
        offers_2_3(date_start, date_stop, user_token, name, app_token, timezone, engine, s3)

date_start = str((pd.Timestamp.today() - pd.Timedelta('1 days')).date())
date_stop = str(pd.Timestamp.today().date())
engine = example_3.engine()
s3 = example_3.s3()

daily(engine, s3, date_start, date_stop)