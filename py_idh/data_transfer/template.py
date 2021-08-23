from py_idh.database import PythonJdbc
import pandas as pd
import requests
import json
import datetime
import moment

def resolve_delta_date(table_config):
    delta_date = None
    if 'useDeltaDate' in table_config['configs'] and not table_config['configs']['useDeltaDate']:
        if table_config['configs'].get('lastRunAt'):
            # use the last run date as delta date
            if table_config['configs']['lastRunAt'].find('-') > -1:
                delta_date = int(datetime.fromisoformat(table_config['configs']['lastRunAt']).timestamp() * 1000)
            else:
                delta_date = table_config['configs']['lastRunAt']
        else:
            # set extraction start date to ONE DAY before today
            delta_date = int(datetime.fromisoformat(moment.now().sub(days=1).format('YYYY-MM-DD')).timestamp() * 1000)
    elif table_config['configs'].get('deltaDate'):
        # if the delta switch is off, use the delta date from the input box if any
        if table_config['configs']['deltaDate'].find('-') > -1:
            delta_date = int(datetime.fromisoformat(table_config['configs']['deltaDate']).timestamp() * 1000)
        else:
            delta_date = table_config['configs']['deltaDate']
    return delta_date

def main():
    db_connection_data = {
        'id': None,
        'database': None,
        'host': None,
        'port': None,
        'type': None,
        'user': None,
        'password': None,
        'schema': None,
        'tmpSchema': None,
        'batchSize': None
    }
    table_config = dict(
        src_name = src_table,
        name = target_table,
        schema = target_schema,
        columnData = columnData,# a list of dictionaries where each dictionary tells the information about a specific column, e.g. name, type, length, is_primary
        configs = configs
    )
    col_info = dict(
        name = name,
        type = data_type,
        length = length,
        isPrimary = isPrimary | col['isPrimary'],
        excluded = False,
        missingInDb = False,
        onlyInDb = False,
        dbType = col['type'],
        dbLength = str(col['length']),
        oldType = None,
        oldLength = None,
        oldIsPrimary = col['isPrimary'],
        oldExcluded = None
    )
    configs = {'rowOffset': 0, 'limit': 10000, 'deltaDate': '', 'useDeltaDate': False, 'lastRunAt': None, 'customQueries': []}
    token = ''
    connection_id = ''


    # Connect to Python jdbc
    jdbc = PythonJdbc()
    # Create schema if not exist
    # jdbc.create_schema(schema_name='bdo', connection_id=connection_id, connection_data=db_connection_data)
    if not jdbc.has_table(schema_name='bdo', table_name='problem', connection_id=connection_id, connection_data=db_connection_data):
        jdbc.create_table(schema_name='bdo', table_name='problem', connection_id=connection_id, connection_data=db_connection_data)

    # get row data via api (i.e, ServiceNow, HubSpot)
    table_name = 'problem'
    offset = 1000
    batch_size = 1000
    cloud_con = {
        'host': 'https://dev98000.service-now.com',
        'user': 'admin',
        'password': 'rrSyvUp5AWO0'
        }
    # Set the request parameters
    url = f"{cloud_con['host']}/api/now/table/{table_name}?sysparm_offset={offset}&sysparm_limit={batch_size}"
    # Set proper headers
    headers = {"Accept":"application/json"}
    # Do the HTTP request
    response = requests.get(url, auth=(cloud_con['user'], cloud_con['password']), headers=headers)
    # Check for HTTP codes other than 200
    if response.status_code != 200: 
        print('Status:', response.status_code, 'Headers:', response.headers, 'Error Response:', response.content)
        exit()
    # Decode the JSON response into a dictionary and use the data
    row_data = json.loads(response.content.decode())
    # print(json.dumps(result, indent=4))

    # Filter using pandas dataframe, i.e. delete row, delete column
    df = pd.DataFrame(row_data)


    # TODO: delta mechanism
    delta_date = resolve_delta_date(table_config)

    # Convert data back to list after filtering
    final_data = df.to_list()

    # Insert data into data base
    jdbc.execute_batch(final_data, db_connection_data)

if __name__ == '__main__':
    main()