from datetime import date, timedelta
import requests
import json

auth_token = ''
excluded_auth_ids = []

dr_request_header = {
        'Content-Type': 'application/json',
        'Authorization': auth_token,
        'Accept': 'application/json'
    }

create_datastreams_endpoint = 'https://api.datorama.com/v1/data-streams'
process_datastreams_endpoint = 'https://api.datorama.com/v1/data-streams/process'

workspace_source_list = {
    27254: ['Facebook-Ads'],
    17259: ['Google AdWords'],
    27405: ['Bing Ads'],
    27501: ['Facebook-Ads']

}

new_datastream_ids = []

def get_api_streams(workspace_id, data_source_name):
    list_datastreams_endpoint = 'https://api.datorama.com/v1/workspaces/{}/data-streams'.format(workspace_id)
    get_datastreams_request = requests.get(url=list_datastreams_endpoint, headers=dr_request_header)

    if get_datastreams_request.status_code != 200:
        raise Exception(get_datastreams_request.text)

    # response header: print(get_datastreams_request.headers)
    connected_datastreams = get_datastreams_request.json()
    api_datastreams = []

    for i in range(len(connected_datastreams)):
        if data_source_name in connected_datastreams[i]['dataSourceName']:
            api_datastreams.append(connected_datastreams[i])

    return api_datastreams

#data_streams=get_api_streams('17259', 'Google AdWords')


def get_connected_profiles(datastreams):
    connected_profile_ids=[]

    for stream in datastreams:
        connected_profile_ids.append(stream['config']['externalIdentifier'])

    return connected_profile_ids


def get_all_profiles(data_source_auth_id):
    get_profiles_endpoint = 'https://app.datorama.com/services/admin/datasource/authentication/{}/getprofiles'.format(data_source_auth_id)
    response = requests.get(url=get_profiles_endpoint, headers=dr_request_header)

    if response.status_code != 200:
        raise Exception(response.text)

    profiles = response.json()

    return profiles


def get_auth_ids(datastreams):
    auth_ids = []

    for stream in datastreams:
        if stream['dataSourceAuthenticationId'] not in auth_ids:
            auth_ids.append(stream['dataSourceAuthenticationId'])

    return auth_ids


def create_datastreams(data_source, datastreams, auth_ids, workspace):
    global new_datastream_ids

    parent_instance_id = get_parent_instance_id(datastreams)

    for id in auth_ids:
        if id not in excluded_auth_ids:
            connected_profiles = get_connected_profiles(datastreams)
            all_profiles = get_all_profiles(id)

            for profile in all_profiles:
                if profile['externalIdentifier'] not in connected_profiles:
                    print('  creating datastream for profile: {}'.format(profile['name']))

                    payload = get_config(data_source)
                    payload = set_config(default_config=payload, data_source=data_source, auth_id=id,
                                         profile=profile, workspace_id=workspace, parent_id=parent_instance_id)
                    response = requests.post(url=create_datastreams_endpoint, headers=dr_request_header,
                                             data=json.dumps(payload))

                    if response.status_code != 201:
                        raise Exception(response.text)
                    else:
                        print('     created datastream successfully')

                    response = response.json()

                    new_datastream_ids.append(response['id'])


def get_config(data_source):
    prefix = data_source.lower().replace(' ', '_').replace('-', '_')

    with open('{}_config.json'.format(prefix)) as config:
        config_object=json.load(config)

        return config_object


def set_config(default_config, data_source, auth_id, profile, workspace_id, parent_id):
    """

    :return:type default_config: object
    """
    # assign 'global' variables first
    default_config['workspaceId'] = workspace_id
    default_config['dataSourceAuthenticationId'] = auth_id
    default_config['config']['account_id'] = profile['externalIdentifier']
    default_config['config']['externalIdentifier'] = profile['externalIdentifier']

    if data_source == 'Facebook-Ads' and workspace_id == 27254:
        # default_config['parentInstanceId'] = 1364659
        default_config['parentInstanceId'] = parent_id
        default_config['name'] = 'Facebook_Ads_ ({} ({}))'.format(profile['name'],
                                                                profile['externalIdentifier'])
        default_config['templateId'] = 23743
        default_config['customAttribute3'] = ''

    elif data_source == 'Facebook-Ads' and workspace_id == 27501:
        default_config['parentInstanceId'] = 'null'
        default_config['name'] = 'Facebook_Ads_Delivery_ ({} ({}))'.format(profile['name'],
                                                                 profile['externalIdentifier'])
        default_config['templateId'] = 23809
        default_config['customAttribute3'] = ''

    elif data_source == 'Google AdWords':
        # default_config['parentInstanceId'] = 1050813
        default_config['parentInstanceId'] = parent_id
        default_config['name'] = 'Google Ads_ ({} ({}))'.format(profile['name'],
                                                                         profile['externalIdentifier'])
        default_config['templateId'] = 27259
        default_config['customAttribute3'] = ''

    elif data_source == 'Bing Ads':
        # default_config['parentInstanceId'] = 1267837
        default_config['parentInstanceId'] = parent_id
        default_config['name'] = 'BingAds_ ({} ({}))'.format(profile['name'], profile['externalIdentifier'])
        default_config['templateId'] = 23250
        default_config['customAttribute3'] = ''

    return default_config


def process_datastreams(datastream_id_list):
    payload = {
        'dataStreamIds': datastream_id_list,
        'startDate': (date.today()-timedelta(1)).strftime('%Y-%m-%d'),
        'endDate': date.today().strftime('%Y-%m-%d'),
        'create': True
    }

    response = requests.post(url=process_datastreams_endpoint, headers=dr_request_header, data=json.dumps(payload))

    if response.status_code != 200:
        raise Exception(response.text)
    else:
        print('datastreams processed successfully')


# assumes one-to-one relationship between datasource and parent
def get_parent_instance_id(datastreams):
    parent_instance_id = ''

    for stream in datastreams:
        if stream['parentInstanceId'] is not None:
            parent_instance_id = stream['parentInstanceId']
            break

    return parent_instance_id


def main(workspace_source_list):
    print('python auto_create.py: \nProgram execution commences here')

    for workspace in workspace_source_list:
        print('\nexecuting task for workspace id: {}'.format(workspace))

        for data_source in workspace_source_list[workspace]:
            print('  data source: {}'.format(data_source))

            data_streams = get_api_streams(workspace, data_source)
            auth_ids = get_auth_ids(data_streams)

            create_datastreams(data_source, data_streams, auth_ids, workspace)

    process_datastreams(new_datastream_ids)

    print('Program terminated')
    pass


if __name__ == "__main__":
    main(workspace_source_list)