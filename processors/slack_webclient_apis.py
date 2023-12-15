import logging
import os
import time
from http.client import IncompleteRead

import pandas as pd
from datetime import datetime

from slack_sdk import WebClient

from env_vars import RAW_DATA_S3_BUCKET_NAME, PUSH_TO_S3
from utils.publishsing_client import publish_object_file_to_s3

logger = logging.getLogger(__name__)


class SlackApiProcessor:
    client = None

    def __init__(self, bot_auth_token):
        self.__bot_auth_token = bot_auth_token
        self.client = WebClient(token=self.__bot_auth_token)

    def fetch_channel_info(self, channel_id):
        try:
            response = self.client.conversations_info(channel=channel_id)
            if response:
                if 'ok' in response and response['ok']:
                    channel_info = response['channel']
                    return channel_info
        except Exception as e:
            logger.error(f"Exception occurred while fetching channel info for channel_id: {channel_id} with error: {e}")
        return None

    def fetch_conversation_history(self, channel_id: str, latest_timestamp: str, oldest_timestamp: str):
        if not channel_id or not latest_timestamp or oldest_timestamp is None:
            logger.error(f"Invalid arguments provided for fetch_conversation_history")
            return False
        channel_info = self.fetch_channel_info(channel_id)
        raw_data = pd.DataFrame(columns=["uuid", "full_message"])
        message_counter = 0
        visit_next_cursor = True
        next_cursor = None
        try:
            while visit_next_cursor:
                try:
                    if oldest_timestamp is not None and oldest_timestamp != '':
                        response_paginated = self.client.conversations_history(channel=channel_id, cursor=next_cursor,
                                                                               latest=latest_timestamp,
                                                                               oldest=oldest_timestamp, limit=100,
                                                                               timeout=300)
                    else:
                        response_paginated = self.client.conversations_history(channel=channel_id, cursor=next_cursor,
                                                                               latest=latest_timestamp, limit=100,
                                                                               timeout=300)
                except IncompleteRead as e:
                    logger.error(
                        f"IncompleteRead occurred while fetching conversation history for channel_id: {channel_id} "
                        f"with error: {e}")
                    continue
                except Exception as e:
                    logger.error(
                        f"Exception occurred while fetching conversation history for channel_id: {channel_id} with error: {e}")
                    continue
                if not response_paginated:
                    break
                if 'messages' in response_paginated:
                    messages = response_paginated["messages"]
                    if not messages or len(messages) <= 0:
                        break
                    new_timestamp = response_paginated["messages"][0]['ts']
                    if float(new_timestamp) >= float(latest_timestamp):
                        break
                    if oldest_timestamp and float(new_timestamp) <= float(oldest_timestamp):
                        break
                    for message in response_paginated["messages"]:
                        temp = pd.DataFrame([{"full_message": message, "uuid": message.get('ts')}])
                        raw_data = pd.concat([temp, raw_data])
                        message_counter = message_counter + 1
                    logger.info(f'{str(message_counter)}, messages published')
                    logger.info(f'Extracted Data till {datetime.fromtimestamp(float(new_timestamp))}')
                if 'response_metadata' in response_paginated and \
                        'next_cursor' in response_paginated['response_metadata']:
                    next_cursor = response_paginated['response_metadata']['next_cursor']
                    time.sleep(0.5)
                else:
                    visit_next_cursor = False
                    break
        except Exception as e:
            logger.error(
                f"Exception occurred while fetching conversation history for channel_id: {channel_id} with error: {e}")
            return False

        if raw_data.shape[0] > 0:
            raw_data = raw_data.reset_index(drop=True)
            raw_data = raw_data.sort_values(by=['uuid'])
            raw_data = raw_data.reset_index(drop=True)
            duplicates = raw_data[raw_data.duplicated(subset='uuid', keep=False)]
            if duplicates.shape[0] > 0:
                logger.info(f"Handling {duplicates.shape[0]} duplicate messages for channel_id: {channel_id}")
                raw_data = raw_data.drop_duplicates(subset='uuid', keep='last')

            base_dir = os.path.dirname(os.path.abspath(__file__))
            latest_datetime = datetime.fromtimestamp(float(latest_timestamp))
            if channel_info:
                channel_name = channel_info['name']
                team_id = channel_info['context_team_id']
                csv_file_name = f"{team_id}-{channel_id}-{channel_name}-{latest_datetime}-raw_data.csv"
            else:
                csv_file_name = f"{channel_id}-{latest_datetime}-raw_data.csv"
            file_path = os.path.join(base_dir, csv_file_name)

            raw_data.to_csv(file_path, index=False)
            if PUSH_TO_S3:
                publish_object_file_to_s3(file_path, RAW_DATA_S3_BUCKET_NAME, csv_file_name)
                logger.info(f"Successfully extracted {message_counter} messages for channel_id: {channel_id}")
                try:
                    os.remove(file_path)
                    logger.error(f"File '{file_path}' deleted successfully.")
                except FileNotFoundError:
                    logger.error(f"File '{file_path}' not found.")
                except PermissionError:
                    logger.error(f"Permission error. You may not have the necessary permissions to delete the file.")
                except Exception as e:
                    logger.error(f"An error occurred: {e}")
        else:
            logger.error(
                f"No new messages found for channel_id: {channel_id} between {latest_timestamp} and {oldest_timestamp}")
            return False
        return True
