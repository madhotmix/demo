# This is a script for automatically updating google sheets files

from oauth2client import file, client, tools
from googleapiclient.errors import HttpError
from googleapiclient import discovery
from httplib2 import Http
import pandas as pd
import time
import sys

import example_3  # My own function library

engine = example_3.engine()
offers_ids = pd.read_sql("""SELECT * FROM offers_ids""", engine)
SCOPES = 'https://www.googleapis.com/auth/spreadsheets'
store = file.Storage('credentials.json')
creds = store.get()
service = discovery.build('sheets', 'v4', http=creds.authorize(Http()))
value_input_option = 'USER_ENTERED'

def error_processing(text):
    with open('log_upload', 'a') as log:
        log.write(f'{text}\n')
    print(text)

def lowercase_tarnslation(x):
    return x.lower().translate({ord(c): "_" for c in " !@#$%^&*()[]{};:,./<>?\|`~-=_+"})
    
def read_csv(name, page):
    try:
        if page == 'report':  # for joom specific offer's tables
            values = pd.read_csv(f'../timelapse/{name}.csv', header=-1).fillna(0).values.tolist()
        else:
            values = pd.read_csv(f'../timelapse/{name} {page}.csv', header=-1).fillna(0).values.tolist()
        return values
    except FileNotFoundError:
        error_processing(f'fail, {str(pd.Timestamp.now())[:19]}, file timelapse/{name} {page}.csv does not exist')

def read_spreadsheet_id(name):
    if name != 'summary':
        spreadsheet_id_series = offers_ids.loc[offers_ids['name'] == name]['spreadsheet_id'].values
    else:
        spreadsheet_id_series = offers_ids.loc[offers_ids['name'] == f'{name} {page[:7]}']['spreadsheet_id'].values
    if len(spreadsheet_id_series) > 0:
        return spreadsheet_id_series[0]
    else:
        error_processing(f'fail, {str(pd.Timestamp.now())[:19]}, there is no spreadsheet_id for {name} in offers_ids')

def read_spreadsheet_id(name):
    spreadsheet_id_series = offers_ids.loc[offers_ids['name'] == name]['spreadsheet_id'].values
    if len(spreadsheet_id_series) > 0:
        return spreadsheet_id_series[0]
    else:
        error_processing(f'fail, {str(pd.Timestamp.now())[:19]}, there is no spreadsheet_id for {name} in offers_ids')
        
def read_sheet_id(spreadsheet_id, name, page):
    if lowercase_tarnslation(page) in offers_ids.columns:
        return offers_ids.loc[offers_ids['name'] == name][lowercase_tarnslation(page)].values[0]
    else:
        return create_sheet(spreadsheet_id, name, page)
        
def create_sheet(spreadsheet_id, name, page):
    body = {"requests": [{"addSheet": {"properties": {"title": page, "gridProperties": {"rowCount": 20, "columnCount": 6}}}}]}
    response = service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()
    sheet_id = str(response['replies'][0]['addSheet']['properties']['sheetId'])
    offers_ids.loc[offers_ids['name'] == name, lowercase_tarnslation(page)] = sheet_id
    offers_ids.to_sql('offers_ids', engine, if_exists="replace", index=False)
    return sheet_id

def clear_sheet(spreadsheet_id, sheet_id):
    body = {"requests": [{"clearBasicFilter": {"sheetId": sheet_id}}]}
    service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()
    body = {"requests": [{"updateCells": {"range": {"sheetId": sheet_id}, "fields": "userEnteredValue"}}]}
    service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()

def write_data(spreadsheet_id, page, values):
    range_name = f'{page}!A1'
    body = {'values': values}
    service.spreadsheets().values().update(spreadsheetId=spreadsheet_id, range=range_name, valueInputOption=value_input_option, body=body).execute()

def autofit_columns(spreadsheet_id, sheet_id):
    body = {"requests": [{"autoResizeDimensions": {"dimensions": {"sheetId": sheet_id,"dimension": "COLUMNS"}}}]}
    service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()

def conditional_formatting(name, page, spreadsheet_id, sheet_id):
    if 'offer_1' not in name: return
    my_range = {'sheetId': sheet_id, 'startColumnIndex': 10, 'endColumnIndex': 11}
    requests = [{"deleteConditionalFormatRule": {"index": 0, "sheetId": sheet_id}}]
    body = {'requests': requests}
    for i in range(3):
        try:
            response = service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()
        except HttpError as err:
            if err.resp.status == 400:
                None
    requests = [{'addConditionalFormatRule': {'rule': {'ranges': [my_range],'booleanRule': {'condition': 
                {'type': 'NUMBER_LESS_THAN_EQ','values': [{'userEnteredValue':'0'}]},'format': 
                {'backgroundColor': {'red': 0.9}}}},'index': 0}},
                {'addConditionalFormatRule': {'rule': {'ranges': [my_range],'booleanRule': {'condition': 
                {'type': 'NUMBER_GREATER','values': [{'userEnteredValue':'0'}]},'format': 
                {'backgroundColor': {'red': 0.9, 'green': 0.9}}}},'index': 0}},
                {'addConditionalFormatRule': {'rule': {'ranges': [my_range],'booleanRule': {'condition': 
                {'type': 'NUMBER_GREATER','values': [{'userEnteredValue':'0.2'}]},'format': 
                {'backgroundColor': {'green': 0.9}}}},'index': 0}}]
    body = {'requests': requests}
    service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()

def google_api_bypass_limitations(func, args):
    try:
        return func(*args)
    except HttpError as err:
        if err.resp.status == 429:  # Limit of queries exceed
            time.sleep(5)
            google_api_bypass_limitations(func, args)
        if err.resp.status == 503:  # The service is currently unavailable
            time.sleep(1)
            google_api_bypass_limitations(func, args)
        else:
            error_processing(f'fail, {str(pd.Timestamp.now())[:19]}, {sys.exc_info()[:2]}')

def sheets_update(name, page, category=''):
    if len(offers_ids[offers_ids['name'].str.contains(f'{name}')]) > 0:
        print(f'{name} {page} {category}')
        values = read_csv(name, page)
        if category == 'common':
            name, page = f'summary {page}', name
        spreadsheet_id = read_spreadsheet_id(name)
        if values is not None and spreadsheet_id is not None:
            sheet_id = google_api_bypass_limitations(read_sheet_id, (spreadsheet_id, name, page))
            google_api_bypass_limitations(clear_sheet, (spreadsheet_id, sheet_id))
            google_api_bypass_limitations(write_data, (spreadsheet_id, page, values))
            google_api_bypass_limitations(autofit_columns, (spreadsheet_id, sheet_id))
            google_api_bypass_limitations(conditional_formatting, (name, page, spreadsheet_id, sheet_id))
    else:
        error_processing(f'fail, {str(pd.Timestamp.now())[:19]}, {name} is not in offers_ids')
    
def offers_list(page):
    for name in ['offer_1', 'offer_2', 'offer_3']:
        sheets_update(name, page)
        sheets_update(name, f'{page} (roi)')
        sheets_update(name, f'{page} (roi by offer)')

offers_list(pd.Timestamp.today().strftime('%Y-%m'))
if 20 >= pd.Timestamp.today().day >= 1:
    offers_list((pd.to_datetime(str(pd.Timestamp.today())[:7] + '-01') - pd.Timedelta(1)).strftime('%Y-%m'))
