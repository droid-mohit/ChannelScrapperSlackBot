import logging
import os

import pandas as pd
from datetime import datetime, timezone

import requests

from env_vars import SENTRY_RAW_DATA_S3_BUCKET_NAME, PUSH_TO_S3
from utils.publishsing_client import publish_object_file_to_s3

logger = logging.getLogger(__name__)


class SentryApiProcessor:
    client = None

    def __init__(self, bearer_token, organization_slug, project_slug):
        self.__auth_token = f"Bearer {bearer_token}"
        self.__organization_slug = organization_slug
        self.__project_slug = project_slug
        self.base_url = f'https://sentry.io/api/0/projects/{self.__organization_slug}'

    def fetch_events(self, latest_timestamp: str, oldest_timestamp: str):
        if not latest_timestamp or oldest_timestamp is None:
            logger.error(f"Invalid arguments provided for fetch_events")
            return False

        headers = {
            "Authorization": self.__auth_token,
        }

        oldest_datetime = datetime.utcfromtimestamp(float(oldest_timestamp)).replace(tzinfo=timezone.utc).strftime(
            '%Y-%m-%dT%H:%M:%SZ')

        latest_datetime = datetime.utcfromtimestamp(float(latest_timestamp)).replace(tzinfo=timezone.utc).strftime(
            '%Y-%m-%dT%H:%M:%SZ')
        url = f"{self.base_url}/{self.__project_slug}/events/?until={latest_datetime}&since={oldest_datetime}"
        call_counter = 0
        message_counter = 0
        should_continue = True
        all_events = []
        try:
            while url and should_continue:
                response = requests.get(url, headers=headers)
                if response.status_code == 200:
                    data = response.json()
                    all_events.extend(data)
                    call_counter += 1
                    message_counter += len(data)
                    print(f"Call Counter : {call_counter}, Message Counter : {message_counter}, Events : {len(data)}")
                    # Check if there's a next page

                    for event in data:
                        if event['dateCreated'] > str(latest_datetime):
                            should_continue = False
                            break
                        if event['dateCreated'] < str(oldest_datetime):
                            should_continue = False
                            break
                    if response.links and response.links.get("next", None):
                        url = response.links["next"]["url"]
                    else:
                        break
                else:
                    # Handle errors
                    print(f"Error: {response.status_code}")
                    print(response.text)
                    break
        except Exception as e:
            logger.error(
                f"Exception occurred while fetching events for project_slug: {self.__project_slug} with error: {e}")
            return False
        raw_data = pd.DataFrame(all_events)
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
            csv_file_name = f"{self.__organization_slug}-{self.__project_slug}-{latest_datetime}-raw_events_data.csv"
            file_path = os.path.join(base_dir, csv_file_name)
            raw_data.to_csv(file_path, index=False)
            if PUSH_TO_S3:
                publish_object_file_to_s3(file_path, SENTRY_RAW_DATA_S3_BUCKET_NAME, csv_file_name)
                logger.info(f"Successfully extracted {message_counter} messages for project: {self.__project_slug}")
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
            logger.error(f"No events found for project_slug: {self.__project_slug}")
            return False
        return True
