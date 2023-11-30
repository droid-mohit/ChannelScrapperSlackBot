import os
import time
from http.client import IncompleteRead

import pandas as pd
from datetime import datetime

from slack_sdk import WebClient

from env_vars import RAW_DATA_S3_BUCKET_NAME
from utils.publishsing_client import publish_object_file_to_s3


def fetch_channel_info(bot_auth_token, channel_id):
    try:
        client = WebClient(token=bot_auth_token)
        response = client.conversations_info(channel=channel_id)
        if response:
            if 'ok' in response and response['ok']:
                channel_info = response['channel']
                return channel_info
    except Exception as e:
        print(f"Exception occurred while fetching channel info for channel_id: {channel_id} with error: {e}")
    return None


def fetch_conversation_history(bot_auth_token, channel_id, latest_timestamp=None, oldest_timestamp=None):
    print(f"Initiating Bulk Extraction for channel_id: {channel_id}")
    channel_info = fetch_channel_info(bot_auth_token, channel_id)
    raw_data = pd.DataFrame(columns=["uuid", "full_message"])
    message_counter = 0
    if not latest_timestamp:
        latest_timestamp = str(time.time())
    visit_next_cursor = True
    next_cursor = None
    try:
        client = WebClient(token=bot_auth_token)
        while visit_next_cursor:
            try:
                if oldest_timestamp:
                    response_paginated = client.conversations_history(channel=channel_id, cursor=next_cursor,
                                                                      latest=latest_timestamp,
                                                                      oldest=oldest_timestamp, limit=100, timeout=300)
                else:
                    response_paginated = client.conversations_history(channel=channel_id, cursor=next_cursor,
                                                                      latest=latest_timestamp, limit=100, timeout=300)
            except IncompleteRead as e:
                print(f"IncompleteRead occurred while fetching conversation history for channel_id: {channel_id} "
                      f"with error: {e}")
                continue
            except Exception as e:
                print(
                    f"Exception occurred while fetching conversation history for channel_id: {channel_id} with error: {e}")
                continue
            if not response_paginated:
                break
            if 'messages' in response_paginated:
                messages = response_paginated["messages"]
                if not messages or len(messages) <= 0:
                    visit_next_cursor = False
                new_timestamp = response_paginated["messages"][0]['ts']
                if new_timestamp >= latest_timestamp:
                    visit_next_cursor = False
                for message in response_paginated["messages"]:
                    temp = pd.DataFrame([{"full_message": message, "uuid": message.get('ts')}])
                    raw_data = pd.concat([temp, raw_data])
                    message_counter = message_counter + 1
                print(message_counter, " messages published.")
                print("Extracted Data till", datetime.fromtimestamp(float(new_timestamp)))
            if 'response_metadata' in response_paginated and 'next_cursor' in response_paginated['response_metadata']:
                next_cursor = response_paginated['response_metadata']['next_cursor']
                time.sleep(1.5)
            else:
                visit_next_cursor = False
    except Exception as e:
        print(f"Exception occurred while fetching conversation history for channel_id: {channel_id} with error: {e}")
        return None

    if raw_data.shape[0] > 0:
        raw_data = raw_data.reset_index(drop=True)
        raw_data = raw_data.sort_values(by=['uuid'])
        raw_data = raw_data.reset_index(drop=True)
        duplicates = raw_data[raw_data.duplicated(subset='uuid', keep=False)]
        if duplicates.shape[0] > 0:
            raw_data = raw_data.drop_duplicates(subset='uuid', keep='last')

        base_dir = os.path.dirname(os.path.abspath(__file__))
        if channel_info:
            channel_name = channel_info['name']
            team_id = channel_info['team_id']
            csv_file_name = f"{team_id}-{channel_id}-{channel_name}-raw_data.csv"
        else:
            csv_file_name = f"{channel_id}-raw_data.csv"
        file_path = os.path.join(base_dir, csv_file_name)

        raw_data.to_csv(file_path, index=False)
        publish_object_file_to_s3(file_path, RAW_DATA_S3_BUCKET_NAME, csv_file_name)
        print(f"Successfully extracted {message_counter} messages for channel_id: {channel_id}")
    else:
        print(f"Exception occurred ot No messages found for channel_id: {channel_id}")
