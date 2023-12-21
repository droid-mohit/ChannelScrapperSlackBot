from celery_app import celery, app


@celery.task
def periodic_data_fetch_job():
    with app.app_context():
        from utils.time_utils import get_current_time
        from persistance.db_utils import get_account_slack_connector, get_slack_connector_channel_key, \
            create_slack_connector_channel_scrap_schedule, get_latest_slack_connector_scrap_schedule_for_channel
        from datetime import datetime

        current_time = get_current_time()
        active_slack_connectors = get_account_slack_connector(is_active=True)
        if not active_slack_connectors:
            print(f"No active slack connectors found")
            return
        for connector in active_slack_connectors:
            slack_channel_keys = get_slack_connector_channel_key(account_slack_connector_id=connector.id,
                                                                 is_active=True)
            if not slack_channel_keys:
                print(f"No active slack channel keys found for connector: {connector.id}")
                continue
            for channel in slack_channel_keys:
                bot_auth_token = connector.metadata.get('bot_auth_token')
                workspace_id = connector.metadata.get('team_id')
                channel_id = channel.key

                latest_timestamp = current_time
                oldest_timestamp = ''

                latest_schedule = get_latest_slack_connector_scrap_schedule_for_channel(connector.id, channel_id)
                if latest_schedule:
                    if latest_schedule.metadata.get('data_extraction_from', ''):
                        oldest_datetime_str = latest_schedule.metadata.get('data_extraction_from')
                        if oldest_datetime_str:
                            oldest_timestamp = datetime.strptime(oldest_datetime_str, '%Y-%m-%d %H:%M:%S').timestamp()
                            oldest_timestamp = str(oldest_timestamp)

                print(f"Scheduling Data Fetch Job for channel_id: {channel_id} at epoch: {current_time}")

                task_run = data_fetch_job.delay(connector.account_id, connector.id, bot_auth_token, channel_id,
                                                workspace_id, latest_timestamp, oldest_timestamp)
                task_run_id = task_run.id
                data_extraction_to = datetime.fromtimestamp(float(latest_timestamp))
                data_extraction_from = None
                if oldest_timestamp:
                    data_extraction_from = datetime.fromtimestamp(float(oldest_timestamp))

                create_slack_connector_channel_scrap_schedule(connector.account_id, connector.id, channel_id,
                                                              task_run_id, data_extraction_to, data_extraction_from)


@celery.task
def data_fetch_job(account_id, connector_id, bot_auth_token: str, channel_id: str, workspace_id: str,
                   latest_timestamp: str, oldest_timestamp: str, is_first_run=False):
    with app.app_context():
        from processors.slack_webclient_apis import SlackApiProcessor
        from utils.time_utils import get_current_time
        from processors.phase_1_report_processor import full_function
        from utils.http_utils import send_report_intimation
        from persistance.db_utils import create_connector_extract_data, create_alert_count_data

        current_time = get_current_time()

        if not bot_auth_token or not channel_id:
            print(f"Invalid arguments provided for data fetch job.")
            return

        if not latest_timestamp:
            print(f"Invalid arguments provided for data fetch job. Missing latest_timestamp. "
                  f"Setting it to current time: {current_time}")
            latest_timestamp = current_time

        if not oldest_timestamp:
            oldest_timestamp = ''

        print(f"Initiating Data Fetch Job for channel_id: {channel_id} at epoch: {current_time} with "
              f"latest_timestamp: {latest_timestamp}, oldest_timestamp: {oldest_timestamp}")
        slack_api_processor = SlackApiProcessor(bot_auth_token)
        raw_data = slack_api_processor.fetch_conversation_history(workspace_id, channel_id, latest_timestamp,
                                                                  oldest_timestamp)
        if not raw_data.shape[0] > 0:
            print(
                f"Found no data for channel_id: {channel_id} at epoch: {current_time} with connector_id: {connector_id}")
            return
        for index, row in raw_data.iterrows():
            data_uuid = row['uuid']
            full_message = row['full_message']
            create_connector_extract_data(account_id=account_id, connector_id=connector_id, channel_id=channel_id,
                                          data_uuid=data_uuid, full_message=full_message)

        phase_1_dataset = full_function(raw_data, workspace_id, channel_id)
        for index, row in phase_1_dataset.iterrows():
            row_timestamp = str(row['timestamp'])
            create_alert_count_data(account_id=account_id, count_timestamp=row_timestamp, channel_id=channel_id,
                                    alert_type=row['alert_type'], count=row['count'])
        if is_first_run:
            print(f"First run for channel_id: {channel_id}. Publishing report.")
            send_report_intimation(account_id=account_id)
        return
