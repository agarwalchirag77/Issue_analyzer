import requests as requests

from utils import Utils, Analysis
from dotenv import load_dotenv
from fastapi import FastAPI, Body
import json

# initialization


app = FastAPI()
load_dotenv()


# async def root(background_tasks: BackgroundTasks, data: dict = Body()):

@app.post("/data", status_code=200)
async def root(data: dict = Body()):
    payload = {
        "integration": [],
        "connector_task": [],
        "handyman_connector_poll": [],
        "handyman_copy_job": [],
        "sideline": [],
        "sink": [],
        "grafana": {}
    }
    print (data)

    int_id = Utils.get_integration_id(data.get('pipeline_no'), data.get('cluster_name'), data.get('account_name'))
    integration = Utils.get_integration_details(int_id)
    integration['data']['integration_id'] = int_id
    integration['data']['cluster_name'] = data.get('cluster_name')
    # print(integration)
    # integration
    payload['integration'].append({"Attribute": "Integration ID", "Value": f"{int_id}"})
    payload['integration'].append(
        {"Attribute": "Ingestion Frequency",
         "Value": f"{integration['data']['execution_policy']['message']}".replace("<b>", "").replace("</b>", "")})
    payload['integration'].append(
        {"Attribute": "Status", "Value": f"{integration['data']['pipeline_status']['display_name']}"})
    payload['integration'].append({"Attribute": "Loading Frequency",
                                   "Value": f"{integration['data']['dest_execution_policy']['message']}".replace("<b>",
                                                                                                                 "").replace(
                                       "</b>", "")})

    # # Handyman
    handyman_rows = Utils.get_handyman_tasks_details(integration['data']['integration_id'],
                                                     data.get('sources'))
    for each in handyman_rows:
        handyman_row = {'id': each['id'],
                        'unique_id': each['unique_id'],
                        'status': each['status'],
                        'retry_count': each['retry_count'],
                        'failure_message': each['failure_message'],
                        'created_ts': each['created_ts'],
                        'processed_ts': each['processed_ts'],
                        'time_diff_in_secs': each['time_diff_in_secs'],
                        'rate_limiting_key': each['rate_limiting_key']}
        payload['handyman_connector_poll'].append(handyman_row)

    handyman_rows = Utils.get_handyman_load_tasks_details(integration['data']['integration_id'],
                                                          data.get('destinations'))
    for each in handyman_rows:
        handyman_row = {'id': each['id'],
                        'unique_id': each['unique_id'],
                        'status': each['status'],
                        'retry_count': each['retry_count'],
                        'failure_message': each['failure_message'],
                        'created_ts': each['created_ts'],
                        'processed_ts': each['processed_ts'],
                        'time_diff_in_secs': each['time_diff_in_secs'],
                        'rate_limiting_key': each['rate_limiting_key']}
        payload['handyman_copy_job'].append(handyman_row)

    upper_limit = Analysis.fetch_current_handyman_limit(payload['handyman_connector_poll'][0]['rate_limiting_key'])
    stalled_task = Analysis.fetch_stalled_task_count(1, payload['handyman_connector_poll'][0]['rate_limiting_key'])
    total_task = Analysis.fetch_all_task_count(1, integration['data']['source_config']['type'],
                                               payload['handyman_connector_poll'][0]['rate_limiting_key'])
    payload['integration'].append({"Attribute": "Total_Unique_task", "Value": f"{total_task['total_task']}"})
    payload['integration'].append({"Attribute": "Handyman_upper_limit", "Value": f"{upper_limit}"})
    payload['integration'].append({"Attribute": "stalled_task", "Value": f"{stalled_task}"})
    hint = f"Current Handyman Limit for this source is {upper_limit} and Total number of unique tasks are {total_task['total_task']}. Hint:"
    if total_task['total_task'] > 50:
        limit = 50
    else:
        limit = total_task['total_task']
    if total_task['total_task'] > upper_limit:
        hint = hint + f'Since total unique tasks is greater than current handyman limit, consider increasing the handyman limit to {limit}'
    else:
        hint = hint + "There shouldn't be ingestion lag due to handyman limit"
        payload['integration'].append({"Attribute": "Hint", "Value": f"{hint}"})


    # Sideline
    sideline_details = Utils.get_sideline_details(integration['data']['integration_id'])
    for each in sideline_details:
        sideline_row = {
            'schema_name': each['schema_name'],
            'stage': each['stage'],
            'reason': each['reason'],
            'total_records': each['total_records'],
            'status': each['status'],
            'code': each['code'],
            'params': each['params'],
            'status_description': each['status_description']}
        payload['sideline'].append(sideline_row)

    # # sink
    sink_details = Utils.get_sink_consumer_details(integration['data']['destination_id'],
                                                   integration['data']['dest_type'],
                                                   data.get('destinations'))

    for each in sink_details:
        sink_row = {'Total_records': each['Total_records'], 'status': each['status'], 'table_name': each['table_name'],
                    'failure_reason': each['failure_reason']}
        payload['sink'].append(sink_row)


    # grafana

    topic_id = Utils.get_destination_details(integration['data']['destination_id'])[0]
    topic_info = Utils.get_destination_topic_info(topic_id['topic_id'])[0]
    payload['grafana']['kafka_lags'] = Utils.get_grafana_link_destination_topic(integration['data']['cluster_name'],
                                                                                topic_info['name'],
                                                                                topic_info['consumer_group_type'],
                                                                                topic_info['exclusive_group'],
                                                                                topic_info['exclusive_group_id'])
    payload['grafana']['dest_copy_timings'] = Utils.get_grafana_destination_copy_timings(
        integration['data']['cluster_name'], integration['data']['destination_id'],
        data.get('destinations'))
    if integration['data']['source_config']['type'] == 'wal':
        payload["grafana"]['Binlog_stats'] = Utils.get_grafana_binlog_stats(integration['data']['cluster_name'],
                                                                            integration['data']['integration_id'])

    # connector task
    connector_details = Utils.get_connector_task_details(integration['data']['integration_id'],
                                                         data.get('sources'))
    for each in connector_details:
        connector_row = {
            'name': str(each['name']),
            'last_processed_ts': each['last_processed_ts'],
            'offset': each['offset'],
            'meta': each['meta'],
            'last_failure_message': each['last_failure_message'],
            'last_failure_ts': each['last_failure_ts'],
            'last_records_processed': each['last_records_processed'],
            'created_ts': each['created_ts'],
            'message': each['message'],
            'category': each['category'],
            'bootstrapped': each['bootstrapped'],
            'status': each['status'],
            'parent_task_name': each['parent_task_name'],
            'status_description': each['status_desc']}
        payload['connector_task'].append(connector_row)
    return payload


@app.post("/tables", status_code=200)
async def fetch_objects(data: dict = Body()):
    # {
    #     "pipeline_no":1,
    #     "cluster_name":"us",
    #     "account_name":"test"
    # }

    tables = {"src_objects": [],
              "dest_objects": []}
    pipeline_no = int(data.get('pipeline_no'))
    cluster = data.get('cluster_name')
    account = data.get('account_name')
    integration = {}
    int_id = Utils.get_integration_id(pipeline_no, cluster, account)
    # print(int_id)
    integration = Utils.get_integration_details(int_id)
    integration['data']['integration_id'] = int_id
    integration['data']['cluster_name'] = data.get('cluster_name')
    # print(integration)
    for each in Utils.get_src_objects(int_id):
        tables['src_objects'].append({"label": f"{each['key_level0']}.{each['key_level1']}",
                                      "value": f"{each['key_level0']}.{each['key_level1']}"})
    for each in Utils.get_dest_objects(int_id, integration['data']['destination_id']):
        tables['dest_objects'].append({"label": f"{each['dest_table_name']}", "value": f"{each['dest_table_name']}"})
    # tables['integration_details'] = integration

    # print(tables)
    # tables = {
    #     "source": [
    #         {"label": "MYSQL", "value": "MYSQL"},
    #         {"label": "ORACLE", "value": "ORACLE"}
    #     ],
    #     "destination": [
    #         {"label": "REDSHIFT", "value": "REDSHIFT"},
    #         {"label": "BIGQUERY", "value": "BIGQUERY"}
    #     ]
    # }
    return tables


@app.post("/reports", status_code=200)
async def fetch_report(data: dict = Body()):
    #     ,
    # "sources": "sources.selectedOptionLabels",
    # "destination": "destination_list.selectedOptionLabels"
    data_new = [
        {
            "ID": 123,
            "Name": "Chirag",
            "Role": "Testing"
        },
        {
            "ID": 234,
            "Name": "Rohit",
            "Role": "Nothing"
        }

    ]

    return data_new


@app.post("/replay", status_code=200)
async def replay_events(data: dict = Body()):
    # {
    #     "integration":{"integration_id":1},
    #     "replay":{"schema_name":"dept",
    #               "stage":"MAPPER",
    #               "code":300}
    #
    # }
    int_id = data['integration']['integration_id']
    schema_name = data['replay']['schema_name']
    stage = data['replay']['stage']
    code = data['replay']['code']
    url = f"https://s-denji.hevo.me/api/config/v1.0/sideline/{int_id}/replay-group"
    payload = json.dumps({
        "schema_name": schema_name,
        "stage": stage,
        "code": code,
        "forced": True
    })
    headers = {
        'Authorization': 'Bearer session:1:braavos_denji:5H0yJxHVAnfjvz1sMeJX6nD3uFagZhtUstNDi4dL',
        'Content-Type': 'application/json'
    }
    response = requests.request("PUT", url, headers=headers, data=payload)
    return None
