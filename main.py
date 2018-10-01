"""
    This automation script will re-process all failed jobs of enabled API data streams.
    Use case: accounts with thousands of data streams (e.g: large trading desks)

    Instructions: assign the following 2 global variables:
    'auth_token' (string): should be set to the API user's Access Token, which may be
    found under the 'General Info' tab of the user's profile page
    e.g: auth_token = 'dato-api-xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx'

    'workspaces' (list): should be set to the list of workspaces whose streams you'd wish
    to have auto-processed
    e.g: workspaces = [31415, 16180, 27183]

    Todo: implement logging
"""

import requests
import datetime
import json

auth_token = 'dato-api-fb8a118d-d504-437d-b66a-1e45095d690e'
workspaces = [27405, 27254, 27500, 27501, 17259]

dr_request_header = {
        'Content-Type': 'application/json',
        'Authorization': auth_token,
        'Accept': 'application/json'
    }
process_datastreams_endpoint = 'https://api.datorama.com/v1/data-streams/process'


def get_api_streams(workspace_id):
    list_datastreams_endpoint = 'https://api.datorama.com/v1/workspaces/{}/data-streams'.format(workspace_id)
    get_datastreams_request = requests.get(url=list_datastreams_endpoint, headers=dr_request_header)


    if get_datastreams_request.status_code != 200:
        raise Exception(get_datastreams_request.text)

    # response header: print(get_datastreams_request.headers)

    connected_datastreams = get_datastreams_request.json()

    api_datastreams = []

    for stream in range(0, len(connected_datastreams)):
        if 'TotalConnect' not in connected_datastreams[stream]['dataSourceName']:
            api_datastreams.append(connected_datastreams[stream])

    return api_datastreams


def get_failed_streams(data_streams):
    failed_api_datastreams = []

    for stream in data_streams:
        if stream['enabled'] is True and stream['lastRunStatus'] is False:
            failed_api_datastreams.append(stream)

    print('  no. of failed streams: {}'.format(len(failed_api_datastreams)))
    return failed_api_datastreams


# currently processStatsCounts parameter is not working, use process_ds_execution_log(() instead
def process_streams(data_streams):
    ds_ids = []

    for stream in range(0, len(data_streams)):
        if data_streams[stream]['processStatsCounts'] == 0:
            ds_ids.append(data_streams[stream]['id'])

            payload = {
                'dataStreamIds': data_streams[stream]['id'],
                'startDate': data_streams[stream]['lastDataDate'],
                'endDate': datetime.datetime.today().strftime('%Y-%m-%d'),
                'create': 'false'
            }

            requests.post(url=process_datastreams_endpoint, headers=dr_request_header, data=payload)


# processes from last data date till today
def process_stream(data_stream):
    payload = {
        'dataStreamIds': [data_stream['id']],
        'startDate': data_stream['lastDataDate'],
        'endDate': datetime.datetime.today().strftime('%Y-%m-%d'),
        'create': False
    }

    print(payload)

    response = requests.post(url=process_datastreams_endpoint, headers=dr_request_header, data=json.dumps(payload))
    response = response.json()

    print(response)


# re-processes each of the failed statuses in log
def process_ds_execution_log(data_stream):
    endpoint = 'https://app.datorama.com/services/admin/lzjob/bdsi/{}/log'.format(data_stream['id'])
    headers = {'token': auth_token}

    payload = {
        'pageSize': 100,
        'pageNumber': 1,
        'filter': [
            {
                "field": "status",
                "operator": "EXACT",
                "value": "FAILURE"
            }
        ],
        'sort': [
            {
                'column': 'status',
                'sortOrder': 'ASC'
            }
        ]
    }

    response = requests.post(url=endpoint, headers=headers, json=payload)

    if response.status_code != 200:
        raise Exception(response.text)

    response = response.json()

    failed_jobs = response['data']

    for job in failed_jobs:
        print('    re-running job id {} that had failed due to {}'.format(job['jobId'], job['statusReason']))
        process_payload = {
            'dataStreamIds': [data_stream['id']],
            'startDate': job['startDay'],
            'endDate': job['endDay'],
            'create': False
        }

        response = requests.post(url=process_datastreams_endpoint, headers=dr_request_header, data=json.dumps(process_payload))
        response = response.json()

    return response


def is_processing(streamId):
    status_list = ['IN_PROGRESS', 'PENDING']

    endpoint = 'https://app.datorama.com/services/admin/lzjob/bdsi/{}/log'.format(streamId)
    headers = {'token':auth_token}

    for status in status_list:

        payload = {
            'pageSize': 1,
            'pageNumber':1,
            'filter': [
                {
                    "field": "status",
                    "operator": "EXACT",
                    "value": "{}".format(status)
                }
            ],
            'sort':[
                {
                    'column':'status',
                    'sortOrder':'ASC'
                }
            ]
        }

        response = requests.post(url=endpoint, headers=headers, json=payload)

        if response.status_code != 200:
            raise Exception(response.text)

        response = response.json()

        print(response)

        if response['total'] == 0:
            return False
        if response['data'][0]['status'] == 'IN_PROGRESS' or response['data'][0]['status'] == 'PENDING':
            return True


def main():
    print('program execution commences here')
    for workspace in workspaces:
        print('executing workspace id: {}'.format(workspace))

        workspace_streams = get_api_streams(workspace)
        workspace_failed_streams = get_failed_streams(workspace_streams)

        for datastream in workspace_failed_streams:
            print('  processing datastream id: {}'.format(datastream['id']))

            if is_processing(datastream['id']) is False:
                process_ds_execution_log(datastream)
            else:
                print("    already processing, we'll skip this for now")

    print('program terminates here')


if __name__ == '__main__':
    main()
