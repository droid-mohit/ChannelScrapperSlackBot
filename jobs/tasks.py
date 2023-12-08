from celery_app import celery, app


@celery.task
def periodic_data_fetch_job():
    with app.app_context():
        from persistance.db_utils import get_slack_bot_configs_by, get_last_slack_channel_scrap_schedule_for, \
            create_slack_channel_scrap_schedule
        import time
        from datetime import datetime
        slack_bot_configs = get_slack_bot_configs_by(is_active=True)
        if not slack_bot_configs:
            print(f"No active slack bot configs found")
            return
        current_time = str(time.time())
        for slack_bot_config in slack_bot_configs:
            latest_timestamp = current_time
            oldest_timestamp = None
            slack_channel_config_id = slack_bot_config.id
            channel_id = slack_bot_config.channel_id
            latest_schedule = get_last_slack_channel_scrap_schedule_for(slack_channel_config_id)
            if latest_schedule:
                oldest_timestamp = str(latest_schedule.data_extraction_to.timestamp())
            bot_auth_token = slack_bot_config.slack_workspace.bot_auth_token
            print(f"Scheduling Data Fetch Job for channel_id: {channel_id} at epoch: {current_time}")
            data_fetch_job.delay(bot_auth_token, channel_id, latest_timestamp, oldest_timestamp)
            data_extraction_to = datetime.fromtimestamp(float(latest_timestamp))
            data_extraction_from = None
            if oldest_timestamp:
                data_extraction_from = datetime.fromtimestamp(float(oldest_timestamp))
            create_slack_channel_scrap_schedule(slack_channel_config_id, data_extraction_from, data_extraction_to)


@celery.task
def data_fetch_job(bot_auth_token: str, channel_id: str, latest_timestamp=None, oldest_timestamp=None):
    import os
    import time
    import pandas as pd
    import boto3
    from slack_sdk import WebClient
    from http.client import IncompleteRead
    from datetime import datetime
    current_time = str(time.time())
    if not latest_timestamp:
        latest_timestamp = current_time
    print(f"Initiating Data Fetch Job for channel_id: {channel_id} at epoch: {current_time}")
    slack_web_client = WebClient(token=bot_auth_token)
    channel_info = None
    try:
        response = slack_web_client.conversations_info(channel=channel_id)
        if response:
            if 'ok' in response and response['ok']:
                channel_info = response['channel']
    except Exception as e:
        print(f"Exception occurred while fetching channel info for channel_id: {channel_id} with error: {e}")

    raw_data = pd.DataFrame(columns=["uuid", "full_message"])
    message_counter = 0
    visit_next_cursor = True
    next_cursor = None
    try:
        while visit_next_cursor:
            try:
                if oldest_timestamp:
                    response_paginated = slack_web_client.conversations_history(channel=channel_id, cursor=next_cursor,
                                                                                latest=latest_timestamp,
                                                                                oldest=oldest_timestamp, limit=100,
                                                                                timeout=300)
                else:
                    response_paginated = slack_web_client.conversations_history(channel=channel_id, cursor=next_cursor,
                                                                                latest=latest_timestamp, limit=100,
                                                                                timeout=300)
            except IncompleteRead as e:
                print(
                    f"IncompleteRead occurred while fetching conversation history for channel_id: {channel_id} "
                    f"with error: {e}")
                continue
            except Exception as e:
                print(f"Exception occurred while fetching conversation history for channel_id: {channel_id} with "
                      f"error: {e}")
                continue
            if not response_paginated:
                break
            if 'messages' in response_paginated:
                messages = response_paginated["messages"]
                if not messages or len(messages) <= 0:
                    break
                new_timestamp = response_paginated["messages"][0]['ts']
                if new_timestamp >= latest_timestamp:
                    break
                if oldest_timestamp and new_timestamp <= oldest_timestamp:
                    break
                for message in response_paginated["messages"]:
                    temp = pd.DataFrame([{"full_message": message, "uuid": message.get('ts')}])
                    raw_data = pd.concat([temp, raw_data])
                    message_counter = message_counter + 1
                print(f'{str(message_counter)}, messages published')
                print(f'Extracted Data till {datetime.fromtimestamp(float(new_timestamp))}')
            if 'response_metadata' in response_paginated and \
                    'next_cursor' in response_paginated['response_metadata']:
                next_cursor = response_paginated['response_metadata']['next_cursor']
                time.sleep(1.5)
            else:
                visit_next_cursor = False
    except Exception as e:
        print(
            f"Exception occurred while fetching conversation history for channel_id: {channel_id} with error: {e}")
        return None

    if raw_data.shape[0] > 0:
        raw_data = raw_data.reset_index(drop=True)
        raw_data = raw_data.sort_values(by=['uuid'])
        raw_data = raw_data.reset_index(drop=True)
        duplicates = raw_data[raw_data.duplicated(subset='uuid', keep=False)]
        if duplicates.shape[0] > 0:
            print(f"Handling {duplicates.shape[0]} duplicate messages for channel_id: {channel_id}")
            raw_data = raw_data.drop_duplicates(subset='uuid', keep='last')

        base_dir = os.path.dirname(os.path.abspath(__file__))
        latest_datetime = datetime.fromtimestamp(float(latest_timestamp))
        if channel_info and 'name' in channel_info and 'context_team_id' in channel_info:
            channel_name = channel_info['name']
            team_id = channel_info['context_team_id']
            csv_file_name = f"{team_id}-{channel_id}-{channel_name}-{latest_datetime}-raw_data.csv"
        else:
            csv_file_name = f"{channel_id}-{latest_datetime}-raw_data.csv"
        file_path = os.path.join(base_dir, csv_file_name)

        raw_data.to_csv(file_path, index=False)
        try:
            s3 = boto3.client('s3', aws_access_key_id='your_access_key_id',
                              aws_secret_access_key='your_secret_access_key')
            s3.upload_file(file_path, 'your_data_dump_bucket', csv_file_name)
        except Exception as e:
            print(f"Exception occurred while publishing file: {file_path} to s3 with error: {e}")
        print(f"Successfully extracted {message_counter} messages for channel_id: {channel_id}")
        try:
            os.remove(file_path)
            print(f"File '{file_path}' deleted successfully.")
        except FileNotFoundError:
            print(f"File '{file_path}' not found.")
        except PermissionError:
            print(f"Permission error. You may not have the necessary permissions to delete the file.")
        except Exception as e:
            print(f"An error occurred: {e}")
    else:
        print(f"Exception occurred ot No messages found for channel_id: {channel_id}")
