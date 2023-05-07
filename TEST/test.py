# import demistomock as demisto
from CommonServerPython import *
from CommonServerUserPython import *

import json
import requests
import datetime
import csv
from io import StringIO
from typing import Dict


class Client:
    def __init__(self, base_url: str, api_keys: list, debug: bool = False):
        self.base_url = base_url
        self.api_keys = api_keys
        self.debug = debug

    def fetch_data(self, data: Dict) -> Dict:
        for api_key in self.api_keys:
            headers = {'Content-Type': 'application/x-www-form-urlencoded', 'x-api-key': api_key}
            response = requests.get(self.base_url + '/api/v2/data.json', params=data, headers=headers)

            if response.status_code == 200:
                if self.debug:
                    demisto.debug(f'http status: {response.status_code}')
                    demisto.debug(json.dumps(response.json(), indent=2, ensure_ascii=False))
                return response.json()
            else:
                demisto.error(f"Failed to fetch data with API key: {api_key}, status code: {response.status_code}")
        else:
            response.raise_for_status()


def test_module(client: Client) -> str:
    test_params = make_params_data_range(attribute='updated_at', start=make_yesterday_string(), limit='1')
    try:
        response = client.fetch_data(test_params)

        if "data" in response:
            return "ok"
        else:
            error_message = "Test failed. Please check your API keys and Base URL."
            demisto.error(error_message)
            return error_message
    except Exception as e:
        error_message = f"Test failed. Error: {str(e)}"
        demisto.error(error_message)
        return error_message


def make_yesterday_string():
    yesterday = datetime.datetime.today() - datetime.timedelta(days=1)
    return yesterday.strftime("%Y/%m/%d")


def make_params_data_range(gid='', fid='', tid='', start='', end='', attribute='', search_text='', m='', limit='', order_name='', order='ASC', unique='true', columns=['id', 'value', 'category', 'description', 'type', 'date', 'time', 'reliability', 'created_at', 'updated_at']):

    if start == '' and end == '':
        return_error('Be sure to specify start or end.')

    return {
        'gid': gid,
        'fid': fid,
        'tid': tid,
        'q[predicate]': 'range',
        'q[attribute]': attribute,
        'q[start]': start,
        'q[end]': end,
        'q[search_text]': search_text,
        'q[m]': m,
        'limit': limit,
        'order_name': order_name,
        'order': order,
        'columns[]': columns
    }


def write_to_csv(obj):
    data = {}
    if "data" in obj:
        data = obj['data']

    if len(data):
        output_buffer = StringIO()
        output = csv.writer(output_buffer)
        output.writerow(data[0].keys())
        for row in data:
            output.writerow(row.values())
        return output_buffer.getvalue()
    else:
        return ""


def main():
    command = demisto.command()

    base_url = demisto.params().get('base_url')
    api_key1 = demisto.params().get('api_key1')
    api_key2 = demisto.params().get('api_key2')
    debug = demisto.params().get('debug', False)

    client = Client(base_url, [api_key1, api_key2], debug)

    if command == 'test-module':
        result = test_module(client)
        return_results(result)
    elif command == 'fetch-data':
        yesterday = make_yesterday_string()
        params = make_params_data_range(attribute='updated_at', start=yesterday)
        data = client.fetch_data(params)

        csv_data = write_to_csv(data)

        demisto.results({
            'Type': entryTypes['note'],
            'Contents': csv_data,
            'ContentsFormat': formats['csv'],
            'File': 'data.csv',
            'FileMimeType': 'text/csv'
        })


if __name__ in ('MAIN_COMMANDS', 'main', '__builtin__', '__builtins__', '__main__'):
    main()
