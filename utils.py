import json
import os
# import mysql.connector
import requests
import datetime
import redshift_connector
import snowflake.connector


class Querier:

    @staticmethod
    def fetch_service_data(query) -> list:
        mydb = redshift_connector.connect(
            host=os.getenv('COMMON_DB_HOST'),
            database=os.getenv('DB_NAME_SERVICE'),
            port=5439,
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD')
        )
        cursor = mydb.cursor()
        cursor.execute(query)
        query_result: tuple = cursor.fetchall()
        fields = [field[0] for field in cursor.description]
        t_result = [dict(zip(fields,
                             map(lambda x: x.decode('utf-8') if isinstance(x, (bytes, bytearray)) else x.strftime(
                                 '%Y-%m-%d %H:%M:%S') if isinstance(x, datetime.datetime) else x, record))) for record
                    in query_result]
        return t_result

    @staticmethod
    def fetch_groot_data(query) -> list:
        mydb = redshift_connector.connect(
            host=os.getenv('COMMON_DB_HOST'),
            database=os.getenv('DB_NAME_SERVICE'),
            port=5439,
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD')
        )
        cursor = mydb.cursor()
        cursor.execute(query)
        query_result: tuple = cursor.fetchall()
        fields = [field[0] for field in cursor.description]
        t_result = [dict(zip(fields,
                             map(lambda x: x.decode('utf-8') if isinstance(x, (bytes, bytearray)) else x.strftime(
                                 '%Y-%m-%d %H:%M:%S') if isinstance(x, datetime.datetime) else x, record))) for record
                    in query_result]
        return t_result

    # @staticmethod
    # def fetch_sec_groot_data(query) -> list:
    #     mydb = redshift_connector.connect(
    #         host=os.getenv('COMMON_DB_HOST'),
    #         user=os.getenv('DB_USER'),
    #         password=os.getenv('DB_PASSWORD'),
    #         database=os.getenv('DB_NAME_SEC_GROOT')
    #     )
    #     cursor = mydb.cursor()
    #     cursor.execute(query)
    #     query_result: tuple = cursor.fetchall()
    #     fields = [field[0] for field in cursor.description]
    #     t_result = [dict(zip(fields,
    #                          map(lambda x: x.decode('utf-8') if isinstance(x, (bytes, bytearray)) else x.strftime(
    #                              '%Y-%m-%d %H:%M:%S') if isinstance(x, datetime.datetime) else x, record))) for record
    #                 in query_result]
    #     return t_result
    #
    # @staticmethod
    # def fetch_mav_data(query) -> list:
    #     mydb = redshift_connector.connect(
    #         host=os.getenv('COMMON_DB_HOST'),
    #         user=os.getenv('DB_USER'),
    #         password=os.getenv('DB_PASSWORD'),
    #         database=os.getenv('DB_NAME_MAV')
    #     )
    #     cursor = mydb.cursor()
    #     cursor.execute(query)
    #     query_result: tuple = cursor.fetchall()
    #     fields = [field[0] for field in cursor.description]
    #     t_result = [dict(zip(fields,
    #                          map(lambda x: x.decode('utf-8') if isinstance(x, (bytes, bytearray)) else x.strftime(
    #                              '%Y-%m-%d %H:%M:%S') if isinstance(x, datetime.datetime) else x, record))) for record
    #                 in query_result]
    #     return t_result


class Mapping:
    destination_type = {
        "REDSHIFT": "sink_consumers_redshift_file_details",
        "BIGQUERY": "sink_consumers_gcs_file_details",
        "DATABRICKS": "sink_consumers_databricks_file_details",
        "MYSQL": "sink_consumers_jdbc_file_details",
        "MS_SQL": "sink_consumers_jdbc_file_details",
        "POSTGRES": "sink_consumers_jdbc_file_details",
        "AURORA": "sink_consumers_jdbc_file_details",
        "FIREBOLT": "sink_consumers_firebolt_file_details",
        "SNOWFLAKE": "ink_consumers_snowflake_file_details",
        "MANAGED_BIGQUERY": "sink_consumers_gcs_file_details"

    }
    task_status = {
        "10": "SKIPPED",
        "15": "INIT",
        "20": "NOT_INCLUDED",
        "30": "SCHEDULED",
        "35": "QUEUED",
        "40": "STREAMING",
        "45": "BOOTSTRAPPING",
        "48": "HISTORICAL_LOAD_FINISHED",
        "50": "PAUSED",
        "55": "AWAITING_MAPPING",
        "60": "DEFERRED",
        "80": "FINISHED"
    }
    ENV_ULRS = {"asia": 'https://asia-services.hevoapp.com/'}
    sideline_error_code = {"106": "Varchar columns are short but will be resized automatically",
                           "110": "Decimal columns are short but will be resized automatically",
                           "200": "Could not determine data type for fields. Please correct or drop the field via transformations",
                           "203": "Invalid values found in transformation for fields.",
                           "204": "More than  child events were generated through transformation script",
                           "300": "Event Type not mapped to a destination table",
                           "301": "Fields not mapped to destination columns",
                           "302": "Columns not found in destination table. Please add the columns in the destination table or ignore them via schema mapper",
                           "304": "Source Event Type has more than  fields. Please remove the fields that are not required via transformations and reset the Event Type from Schema Mapper",
                           "307": "Event Type not mapped to a destination table",
                           "308": "Pipeline has been paused",
                           "309": "Fields not mapped to destination columns.",
                           "311": "Incompatible source field to ERD field for -",
                           "400": "Mapped table doesnot exist in Destination. Please create the table or enable auto-mapping",
                           "402": "No values provided for non-null columns. Please provide some defaults via transformations or make the column nullable",
                           "403": "Values for columns in table are incompatible. Please update the values via transformations or update the data type of the destination column.",
                           "405": "Not able to connect to destination",
                           "406": "Destination doesnot allow more than  columns in a table. Please remove the columns that are not required via transformations or ignore them in the schema mapper.",
                           "407": "Destination is able to load a lesser number of events than what is being ingested from the source. Events will be replayed automatically as resources on the destination get freed up.",
                           "408": "Event is too large for the destination. Skip a few large columns",
                           "409": "Insufficient privileges while writing to table. Encountered error",
                           "410": "Constraint violation while writing to table. Encountered error",
                           "411": "Please create a column of larger capacity type and map it in the schema mapper",
                           "412": "Destination configuration is incorrect. Please update the destination configuration"}


class Utils:
    @staticmethod
    def get_dest_objects(int_id: int, dest_id: int, cluster) -> list:
        schema = cluster
        query = f"select distinct dest_table_name from {schema}.sink_consumers_table_mappings where integration_id={int_id} and " \
                f"destination_id={dest_id} and is_deleted=false"
        return Querier.fetch_service_data(query)

    @staticmethod
    def get_src_objects(int_id: int, cluster_name) -> list:
        schema = cluster_name
        query = f"select KEY_LEVEL0,KEY_LEVEL1 from {schema}.integrations_source_objects where integration_id={int_id} and status='ACTIVE'"
        return Querier.fetch_service_data(query)

    @staticmethod
    def get_integration_id(pipeline_no: int, cluster_name: str, team_id: int) -> int:
        # team_id = Utils.get_team_details(cluster_name, account_name)['id']
        schema = cluster_name
        query = f'select id from {schema}.config_integrations where team_id={team_id} and seq_id={pipeline_no}'

        result = Querier.fetch_service_data(query)
        if len(result) == 0:
            return 0
        else:
            return result[0]['id']

    @staticmethod
    def get_team_details(cluster_name: str, account_name: str) -> dict:
        # cluster_name = 'CENTRAL'
        # account_name = 'test'
        query = f"select id from hevo_security_teams where cluster_id='{cluster_name}' and name='{account_name}'"
        return Querier.fetch_groot_data(query)[0]

    @staticmethod
    def get_connector_task_details(int_id: int, task_name: list, cluster: str) -> list:
        print("received connector request")
        columns = 'name,' \
                  'last_processed_ts,' \
                  '"offset",' \
                  'meta,' \
                  'last_failure_message,' \
                  ' last_failure_ts,' \
                  'last_records_processed,' \
                  'created_ts,' \
                  'message,' \
                  'category,' \
                  'bootstrapped,' \
                  'status,' \
                  'parent_task_name'

        case_list = 'CASE '
        for key, value in Mapping.task_status.items():
            case_list = case_list + f" WHEN status = {key} THEN '{value}' "
        case_list = case_list + 'ELSE Null END AS status_desc'

        task_list = ''
        if len(task_name) == 0:
            condition = ''
        else:
            for each in task_name:
                task_list = task_list + "'" + each + "',"
            condition = f" and name in ({task_list[:-1]})"
        query = f"select {columns}, {case_list}  from {cluster}.connectors_tasks where integration_id={int_id} {condition} order by status "
        print(query)
        return Querier.fetch_service_data(query)

    # check
    @staticmethod
    def get_integration_details(int_id: int, cluster: str) -> dict:
        ENV_URL = Mapping.ENV_ULRS[cluster]
        url = ENV_URL + os.getenv('INTEGRATION_ENDPOINT').format(integration_id=int_id)
        headers = {
            'Authorization': 'Basic aW50ZXJuYWxhcGl1c2VyOjZEQFNPSmYxJDNtZ1dUY3FNUWpIVE5AN2U5WA=='
        }
        response = requests.request("GET", url, headers=headers)
        if response.status_code == 200:
            return json.loads(response.text)
        else:
            print("Failed to fetch integration details")
            return None

    @staticmethod
    def get_handyman_tasks_details(int_id: int, task_name: list, cluster: str) -> dict:
        columns = 'id,' \
                  'unique_id,' \
                  'status,' \
                  'retry_count,' \
                  'failure_message,' \
                  'created_ts,' \
                  'processed_ts,' \
                  'DATEDIFF(seconds,created_ts,processed_ts) as time_diff_in_secs,' \
                  'rate_limiting_key'
        task_filter = ''
        if len(task_name) == 0:
            task_filter = f"ht1.unique_id like '{int_id}--9999default9999-%' or"
        else:
            for each in task_name:
                task_filter = task_filter + f" ht1.unique_id like '{int_id}-{each}%' or"

        query = f"select {columns}  " \
                f"FROM (" \
                f"SELECT " \
                f"ht1.*," \
                f"ROW_NUMBER() OVER(PARTITION BY unique_id ORDER BY created_ts desc ) as r from {cluster}.handyman_tasks ht1 " \
                f"where type ='connector_poll_task_job' and ({task_filter[:-2]} or ht1.unique_id = '{int_id}-BinLog' " \
                f"or ht1.unique_id = '{int_id}-WAL' " \
                f"or ht1.unique_id = '{int_id}-Change Stream' " \
                f"or ht1.unique_id = '{int_id}-local.oplog.rs' )) WHERE r<4 " \
                f"order by id,unique_id desc"
        return Querier.fetch_service_data(query)
        # return query

    @staticmethod
    def get_handyman_load_tasks_details(int_id: int, task_name: list, cluster) -> dict:
        columns = 'id,' \
                  'unique_id,' \
                  'status,' \
                  'retry_count,' \
                  'failure_message,' \
                  'created_ts,processed_ts,' \
                  'DATEDIFF(seconds,created_ts,processed_ts) as time_diff_in_secs,' \
                  'rate_limiting_key'
        task_filter = ''
        if len(task_name) == 0:
            task_filter = f"ht1.unique_id like '{int_id}--9999default9999-%' or"
        else:
            for each in task_name:
                task_filter = task_filter + f" ht1.unique_id = '{int_id}-{each}' or"

        query = f"select {columns}  " \
                f"FROM (" \
                f"SELECT " \
                f"ht1.*," \
                f"row_number() over(partition by unique_id order by created_ts desc ) as r " \
                f"from {cluster}.handyman_tasks ht1 " \
                f"where type ='destination_table_copier_job' and ({task_filter[:-2]}) " \
                f" )  WHERE r<4 " \
                f"order by id,unique_id desc"
        return Querier.fetch_service_data(query)

    @staticmethod
    def get_sideline_details(int_id: int, cluster) -> list:
        columns = 'schema_name,' \
                  'stage,' \
                  'reason,' \
                  'sum(num_records) as total_records,' \
                  'status,' \
                  'code,' \
                  'params '
        # if schema_name is None:
        #     query = f"select  {columns}   " \
        #             f"from sideline_file_details " \
        #             f"where integration_id={int_id} " \
        #             f"and status!='REPLAYED'" \
        #             f"group by schema_name"
        # else:
        #
        #     schema_name_list = ','.join(schema_name)
        #     query = f"select  {columns}   " \
        #             f"from sideline_file_details " \
        #             f"where integration_id={int_id} " \
        #             f"and schema_name in ('{schema_name_list}') " \
        #             f"and status!='REPLAYED'" \
        #             f"group by schema_name, status"
        #     print(query)

        case_list = 'CASE '
        for key, value in Mapping.sideline_error_code.items():
            case_list = case_list + f" WHEN code = {key} THEN '{value}' "
        case_list = case_list + 'ELSE Null END AS sideline_description'
        query = f"select  {columns} ,{case_list}  " \
                f"from {cluster}.sideline_file_details " \
                f"where integration_id={int_id} " \
                f"and status!='REPLAYED'" \
                f"group by schema_name, status, stage, reason,code,params"
        return Querier.fetch_service_data(query)

    # @staticmethod
    # def get_connectors_task_details(int_id: int, task_name: str) -> dict:
    #     if task_name is None:
    #         query = f"select * from connectors_tasks where integration_id={int_id} "
    #     else:
    #         query = f"select * from connectors_tasks where integration_id={int_id} and name like '%{task_name}%'"
    #     return Querier.fetch_service_data(query)

    @staticmethod
    def get_sink_consumer_details(dest_id: int, dest_type: str, table_name: list, cluster) -> list:
        dest_table = Mapping.destination_type[dest_type]
        columns = 'sum(num_records) as Total_records,' \
                  'status,' \
                  'table_name,' \
                  'failure_reason'
        if len(table_name) == 0:
            query = f"select {columns} " \
                    f"from {cluster}.{dest_table} " \
                    f"where destination_id={dest_id} " \
                    f"and status!='PROCESSED' " \
                    f"group by table_name,status,failure_reason"
        else:
            table_list = ""
            for each in table_name:
                table_list = table_list + "'" + each + "',"
            query = f"select {columns} " \
                    f"from {cluster}.{dest_table} " \
                    f"where destination_id={dest_id} " \
                    f"and table_name in ({table_list[:-1]}) " \
                    f"and status!='PROCESSED' " \
                    f"group by table_name,status,failure_reason"
        return Querier.fetch_service_data(query)

    @staticmethod
    def get_destination_topic_info(topic_id: int,cluster: str) -> list:
        columns = 'partitions,' \
                  'name,' \
                  'consumer_group_type,' \
                  'exclusive_group,' \
                  'exclusive_group_id'
        query = f'select {columns} from {cluster}.config_independent_topic_info where id={topic_id}'
        return Querier.fetch_service_data(query)

    @staticmethod
    def get_destination_details(dest_id: int, cluster: str) -> list:
        columns = 'topic_id'
        query = f'select {columns} from {cluster}.config_destinations where id={dest_id}'
        return Querier.fetch_service_data(query)

    @staticmethod
    def get_grafana_link_destination_topic(cluster_id: str, name: str, consumer_group_type: str, exclusive_group: str,
                                           exclusive_group_id: str) -> str:
        base_link = f'https://grafana.hevo.me/d/p0Zv_DB4k/kafka-lags-{cluster_id}?' \
                    f'orgId=1&refresh=1m&var-rp=infra_measurements&var-client=hevo-{cluster_id}' \
                    f'&var-consumer_group={consumer_group_type}:{exclusive_group}:{exclusive_group_id}' \
                    f'&var-topic={name}&from=now-24h&to=now'

        return base_link

    @staticmethod
    def get_grafana_destination_copy_timings(cluster_id: str, dest_id: int, schema_name: list) -> str:
        if len(schema_name) == 0:
            base_link = f'https://grafana.hevo.me/d/62DllvBVz/destination-copy-timings-{cluster_id}?orgId=1&refresh=1m&from=now-24h&to=now&var-client=hevo-{cluster_id}&var-destination_id={dest_id}&var-schema=All'
        else:
            base_link = f'https://grafana.hevo.me/d/62DllvBVz/destination-copy-timings-{cluster_id}?orgId=1&refresh=1m&from=now-24h&to=now&var-client=hevo-{cluster_id}&var-destination_id={dest_id}'
            schema_link = ''
            for each in schema_name:
                schema_link = schema_link + f'&var-schema={each}'
            base_link = base_link + schema_link
        return base_link

    @staticmethod
    def get_grafana_binlog_stats(cluster_id: str, int_id: int) -> str:
        base_link = f'https://grafana.hevo.me/d/EspHlvBVz/binlog-stats-{cluster_id}?orgId=1&refresh=1m&var-rp=infra_measurements&var-client=hevo-{cluster_id}&var-Integration={int_id}&from=now-24h&to=now'
        return base_link


class Analysis:
    @staticmethod
    def fetch_current_handyman_limit(rate_limiting_key: str, cluster) -> int:
        key = rate_limiting_key.split('|')
        query = f"select upper_limit from {cluster}.config_resource_utilization_overrides where constraining_key='{key[0]}' and " \
                f"'key'='{key[1]}' "
        result = Querier.fetch_service_data(query)
        if len(result) == 0:
            return 14
        else:
            return result[0]['upper_limit']

    @staticmethod
    def fetch_stalled_task_count(team_id: int, rate_limiting_key, cluster) -> int:
        team_id = 1
        query = f"select count(*) as cnt from {cluster}.handyman_tasks where team_id ={team_id}  and rate_limiting_key = '{rate_limiting_key}' and status = 'STALLED'"
        result = Querier.fetch_service_data(query)
        return result[0]['cnt']

    @staticmethod
    def fetch_all_task_count(team_id: int, type: str, rate_limiting_key, cluster) -> list:
        if type == 'wal':
            query = f"select  count(DISTINCT entity_id) as total_task from {cluster}.handyman_tasks where team_id={team_id} and rate_limiting_key='{rate_limiting_key}' and type= 'connector_poll_task_job'"
        else:
            query = f"select count(DISTINCT unique_id) as total_task from {cluster}.handyman_tasks where team_id={team_id} and rate_limiting_key='{rate_limiting_key}' and type= 'connector_poll_task_job'"
        return Querier.fetch_service_data(query)[0]
