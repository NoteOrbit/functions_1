import datetime
import polars as pl
import json
from ..repository.dbo_transactions import query_timestamp
import logging

def calculate_missing_timestamps(rows, frequency, start, end):

    '''
    sample parameter


    rows = [
        ['2021-01-01 00:00:00.000000'],
        ['2021-01-01 00:05:00.000000'],
        ['2021-01-01 00:10:00.000000'],
        ['2021-01-01 00:15:00.000000'],
        ['2021-01-01 00:20:00.000000'],
        ['2021-01-01 00:25:00.000000'],
        ['2021-01-01 00:30:00.000000'],
        ...

    ]
    frequency = '5min'
    start = '2021-01-01 00:00:00.000000'
    end = '2021-01-01 00:00:00.000000'


    return [
        
        '2021-01-01 00:00:00.000000',

    ]
    
    
    '''



    start = datetime.datetime.strptime(start, '%Y-%m-%d %H:%M:%S.%f')
    end = datetime.datetime.strptime(end, '%Y-%m-%d %H:%M:%S.%f')
    try:
        if not rows or rows == []:
            if frequency == '5min':
                return [dt.isoformat() for dt in pl.datetime_range(start, end, '5m', eager=True)]
            elif frequency == '30min':
                return [dt.isoformat() for dt in pl.datetime_range(start, end, '30m', eager=True)]
            elif frequency == 'H':
                return [dt.isoformat() for dt in pl.datetime_range(start, end, '1h', eager=True)]
        else:
            rows = [row[0] for row in rows]
            if frequency == '5min':
                complete = pl.datetime_range(start, end, '5m', eager=True)
                return [dt.isoformat() for dt in set(complete) - set(rows)]
            elif frequency == '30min':
                round_down = lambda dt: dt - datetime.timedelta(minutes=dt.minute % 30, seconds=dt.second, microseconds=dt.microsecond)
                rows = [round_down(row) for row in rows]
                complete = pl.datetime_range(start, end, '30m', eager=True)
                return [dt.isoformat() for dt in set(complete) - set(rows)]
            elif frequency == 'H':
                complete = pl.datetime_range(start, end, '1h', eager=True)
                return [dt.isoformat() for dt in set(complete) - set(rows)]
            
    except Exception as e:
        print(f"An error occurred: {e}")
        
        
def run(table_times):

    '''
    sample parameter


    table_times = {
        
        'REGIONSUM': {
            'start_timestamp': '2021-01-01 00:00:00.000000',
            'end_timestamp': '2021-01-01 00:00:00.000000'
        },
        'PRICE': {
            'start_timestamp': '2021-01-01 00:00:00.000000',
            'end_timestamp': '2021-01-01 00:00:00.000000'
        },
        'INTERCONNECTORRES': {
            'start_timestamp': '2021-01-01 00:00:00.000000',
            'end_timestamp': '2021-01-01 00:00:00.000000'
        },
    
    '''
    


    table_column_map = {
        'REGIONSUM': ['SETTLEMENTDATE', '5min'],
        'PRICE': ['SETTLEMENTDATE', '5min'],
        'INTERCONNECTORRES': ['SETTLEMENTDATE', '5min'],
        'PREDISPATCHPRICE': ['LASTCHANGED', '30min'],
        'PREDISPATCHREGIONSUM': ['LASTCHANGED', '30min'],
        'PREDISPATCHINTERCONNECTORRES': ['LASTCHANGED', '30min'],
        'P5MIN_REGIONSOLUTION': ['RUN_DATETIME', '5min'],
        'P5MIN_INTERCONNECTORSOLN': ['RUN_DATETIME', '5min'],
        'STPASA_REGIONSOLUTION': ['RUN_DATETIME', 'H'],
        'STPASA_INTERCONNECTORSOLN': ['RUN_DATETIME', 'H']
    }

    result = {"table_times": []}

    for table_name, times in table_times.items():
        start_timestamp = times.get('start_timestamp')
        end_timestamp = times.get('end_timestamp')

        if not start_timestamp or not end_timestamp or start_timestamp > end_timestamp:
            
            result['table_times'].append({
                'table_name': table_name,
                'error': 'INVALID TIMESTAMP',
            })

            continue
        
        if not table_name or table_name not in table_column_map:
            result['table_times'].append({
                'table_name': table_name,
                'error': 'TABLE NOT FOUND',
            })

            continue


        if start_timestamp and end_timestamp and table_name in table_column_map:
            column = table_column_map[table_name][0]
            frequency = table_column_map[table_name][1]
            logging.info(f"Column: {column}")
            data = query_timestamp(column, table_name, start_timestamp, end_timestamp)
            logging.info(f"Data: {data}")
            logging.info(f"Frequency: {frequency}")
            missing_timestamps = calculate_missing_timestamps(data, frequency, start_timestamp, end_timestamp)
            logging.warning(f"Missing timestamps: {missing_timestamps}")
            result['table_times'].append({
                'table_name': table_name,
                'missing_timestamps': missing_timestamps,
                'start_timestamp': start_timestamp,
                'end_timestamp': end_timestamp,
                'length': len(missing_timestamps),
                'frequency': frequency
            })
        else:
            result['table_times'].append({
                'table_name': table_name,
                'missing_timestamps': []
            })



    '''
    sample result 

    {
        "table_times": [
            {
                "table_name": "REGIONSUM",
                "missing_timestamps": [
                    "2021-01-01T00:00:00.000000",
                    "2021-01-01T00:05:00.000000",
                    "2021-01-01T00:10:00.000000",
                    "2021-01-01T00:15:00.000000",
                    "2021-01-01T00:20:00.000000",
                    "2021-01-01T00:25:00.000000",
                    "2021-01-01T00:30:00.000000",
                    "2021-01-01T00:35:00.000000",
                    "2021-01-01T00:40:00.000000",
                    "2021-01-01T00:45:00.000000",
                    "2021-01-01T00:50:00.000000",
                    "2021-01-01T00:55:00.000000"
                ],
                "start_timestamp": "2021-01-01 00:00:00.000000",
                "end_timestamp": "2021-01-01 00:00:00.000000",
                "length": 12,
                "frequency": "5min"
            },
    
    '''




        
    return json.dumps(result)
        
