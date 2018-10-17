# Datorama Auto-Pylot
This repository contains automation scripts implemented in Python 3 for managing data streams in Datorama. Especially useful for large accounts (e.g: large trading desks with instances containing thousands of streams)

### auto_process.py
**Purpose** 
Re-processes all failed jobs of every enabled API data stream.

**Instructions**
Simply assign the following global variables:
- `auth_token` (string) should be set to the API user's Access Token:
```
auth_token = 'dato-api-xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx'
```
- `workspaces` (list) should be set to the list of workspace(s) whose data streams will be auto processed:
```
workspaces = [31415, 16180, 27183]
```

**Known Issues & Limitations**
Some functions within this script rely on endpoints that return cached results (with approx. 20 min lags) whereas others use endpoints that return live data. Therefore, running this script over relatively short intervals (e.g 10 minutes) may lead to temporary errors. These errors will usually be resolved once the lag period has passed.

### auto_create.py
**Purpose** 
Creates data streams for new advertiser profiles

**Instructions**
Assign the following:
- `auth_token` (string) should be set to the API user's Access Token (see example above)
- `workspace_source_list` (dictionary) key:value pairs where keys (integer) are set to workspace IDs and values (list) are set to the list of data source(s) on which the script will run:
```
workspace_source_list = {
    17254: ['Facebook-Ads', 'Google Analytics'],
    17259: ['Google AdWords'],
    87405: ['Bing Ads'],
    47501: ['Facebook-Ads']
}
```
- **Data stream settings**
      - Create a JSON configuration file for each data source containing the default settings of the stream to be created. 
      **Important:** configuration files provided in this repository are for illustration purposes only. It's *highly recommended* you create your own by making a GET request to the 'Find Data Stream by ID' endpoint and using the response object as a template. See [Datorama's developer documentation](https://developers.datorama.com/docs/platform-api/) for more information.
      Files *must* be in the working directory of python script file, and be named in the lowercase of the data source name (with spaces and dashes replaced with underscores) with '_config' appended to the file name. (for example, a configuration file for 'Facebook-Ads' should be named 'facebook_ads_config.json')
      - Settings that need to be assigned programatically may be done in the `set_config()` function. See function definition for examples.
      
- **Optional:**
      -`excluded_auth_ids` (list) should be set to a data source's authentication ID(s) that are to be excluded. Authentication IDs may be found within the 'Data Source Authentication' tab
      - Once streams have been created the script will call `process_datastreams()` to batch process newly created streams with a starting date of yesterday. For newly created workspaces (or newly added data sources) it might be appropriate to adjust the start date to an earlier date for the initial run. This may be done by modifying the value of the `startDate` key within the `payload` variable in the function definition.

**Known Issues & Limitations**
Where parent-child relationships exist, this script assumes no more than a singe parent exists for each data source. See `get_parent_instance_id()` function definition 