import json
import logging
from datetime import datetime
from typing import Dict

from env_vars import PUSH_TO_S3, METADATA_S3_BUCKET_NAME, PUSH_TO_SLACK, SLACK_APP_ID
from jobs.tasks import data_fetch_job
from persistance.db_utils import get_account_slack_connector, create_slack_connector_channel_key, \
    create_account_slack_connector, \
    get_slack_connector_channel_key, update_slack_connector_channel_key, update_account_slack_connector, \
    create_slack_connector_channel_scrap_schedule
from processors.slack_webclient_apis import SlackApiProcessor
from utils.publishsing_client import publish_json_blob_to_s3, publish_message_to_slack
from utils.time_utils import get_current_datetime

logger = logging.getLogger(__name__)


def handle_oauth_callback(account_id, data: Dict):
    if data['ok'] and 'token_type' in data and data['token_type'] == 'bot' and 'team' in data:
        try:
            current_datetime = get_current_datetime()
            team_id = data['team'].get('id', None)
            team_name = data['team'].get('name', None)
            bot_oauth_token = data.get('access_token', None)
            bot_user_id = data.get('bot_user_id', None)

            if not account_id or not team_id or not bot_oauth_token or not bot_user_id:
                logger.error(f"Error while fetching bot OAuth token with response: {data}")
                return False

            logger.info(f"Received Bot OAuth token for workspace {team_id} : ({team_name})")
            account_slack_connector, is_created = create_account_slack_connector(account_id,
                                                                                 team_id,
                                                                                 bot_user_id,
                                                                                 bot_oauth_token,
                                                                                 team_name)
            if account_slack_connector and is_created:
                if PUSH_TO_S3:
                    account_slack_connector_dict = {
                        'id': account_slack_connector.id,
                        'name': account_slack_connector.metadata.get('team_name', None),
                        'team_id': account_slack_connector.metadata.get('team_id', None),
                        'bot_user_id': account_slack_connector.metadata.get('bot_user_id', None),
                        'bot_auth_token': account_slack_connector.metadata.get('bot_auth_token', None),
                        'created_at': str(account_slack_connector.created_at),
                        'account_id': account_slack_connector.account_id,
                    }
                    data_to_upload = account_slack_connector_dict
                    json_data = json.dumps(data_to_upload)
                    key = f'{account_id}-{team_id}-{team_name}-{current_datetime}.json'
                    publish_json_blob_to_s3(key, METADATA_S3_BUCKET_NAME, json_data)
                if PUSH_TO_SLACK:
                    message_text = f"Registered workspace_id : {team_id}, workspace_name: {team_name}, " \
                                   f"with bot_auth_token: {bot_oauth_token}"
                    publish_message_to_slack(message_text)
            return True
        except Exception as e:
            logger.error(f"Error while fetching bot OAuth token with error: {e}")
            return False
    else:
        logger.error(f"Error while fetching bot OAuth token with response: {data}")
        return False


def handle_event_callback(data: Dict):
    if 'team_id' not in data or 'event' not in data:
        logger.error(f"Error handling slack event callback api, team_id or event not found in request data: {data}")
        return False
    team_id = data['team_id']
    event = data['event']
    active_slack_workspaces_filters = {'team_id': team_id, 'is_active': True}
    active_account_slack_connectors = get_account_slack_connector(**active_slack_workspaces_filters)
    if not active_account_slack_connectors:
        logger.error(f"Error handling slack event callback api for {team_id}: active slack connector not found")
        return False
    bot_user_ids = []
    for connector in active_account_slack_connectors:
        if connector.metadata.get('bot_user_id', None):
            bot_user_ids.append(connector.metadata.get('bot_user_id', None))
    if event and 'type' in event:
        event_type = event['type']
        event_ts = event.get('event_ts', None)
        user = event.get('user', None)
        if event_type == 'app_mention':
            text = event.get('text', None)
            if text:
                for bot_user_id in bot_user_ids:
                    if bot_user_id in text:
                        user = bot_user_id
                        break
        if (event_type != 'app_uninstalled' and event_type != 'channel_left') and user not in bot_user_ids:
            logger.error(f"Error handling {event_type} event type for connector {team_id}: {user} is not bot user")
            return False
        channel_id = event.get('channel', None)
        if (event_type != 'app_uninstalled') and (not channel_id or not event_ts):
            logger.error(
                f"Error handling {event_type} event type for connector {team_id}: channel_id/event_ts not found")
            return False
        if event_type == 'member_joined_channel' or event_type == 'app_mention':
            try:
                account_slack_connector = active_account_slack_connectors[0]
                for bot_user_id in bot_user_ids:
                    if bot_user_id == user:
                        for active_account_slack_connector in active_account_slack_connectors:
                            if active_account_slack_connector.metadata.get('bot_user_id', None) == bot_user_id:
                                account_slack_connector = active_account_slack_connector
                                break
                        break
                bot_auth_token = account_slack_connector.metadata.get('bot_auth_token', None)
                if not bot_auth_token:
                    logger.error(
                        f"Error while registering slack channel for connector: {team_id}: bot_auth_token not found")
                    return False
                slack_api_processor = SlackApiProcessor(bot_auth_token)
                channel_name = None
                channel_info = slack_api_processor.fetch_channel_info(channel_id)
                if channel_info:
                    if 'name' in channel_info:
                        channel_name = channel_info['name']
                slack_connector_key, is_created = create_slack_connector_channel_key(account_slack_connector.account_id,
                                                                                     account_slack_connector.id,
                                                                                     channel_id,
                                                                                     event_ts,
                                                                                     channel_name)
                if slack_connector_key:
                    if is_created:
                        workspace_header = team_id
                        if account_slack_connector.metadata.get('team_name', None):
                            workspace_header = account_slack_connector.metadata.get('team_name', None)
                        channel_header = channel_id
                        if channel_name:
                            channel_header = channel_name
                        if PUSH_TO_S3:
                            current_datetime = get_current_datetime()
                            slack_connector_key_dict = {
                                'id': slack_connector_key.id,
                                'slack_workspace_id': account_slack_connector.id,
                                'channel_id': slack_connector_key.key,
                                'channel_name': slack_connector_key.metadata.get('channel_name', None),
                                'event_ts': slack_connector_key.metadata.get('event_ts', None),
                                'created_at': str(slack_connector_key.created_at),
                            }
                            data_to_upload = slack_connector_key_dict
                            json_data = json.dumps(data_to_upload)
                            key = f'{workspace_header}-{channel_header}-{current_datetime}.json'
                            publish_json_blob_to_s3(key, METADATA_S3_BUCKET_NAME, json_data)
                        if PUSH_TO_SLACK:
                            message_text = "Registered channel for slack connector: " + "*" + workspace_header + "*" \
                                           + " " + "channel: " + "*" + channel_header + "*" + " and channel id: " + "*" + \
                                           channel_id + "*" + " at " + "event_ts: " + event_ts
                            publish_message_to_slack(message_text)
                        task = data_fetch_job.delay(account_slack_connector.account_id,
                                                    account_slack_connector.id,
                                                    bot_auth_token,
                                                    channel_id,
                                                    team_id,
                                                    str(event_ts),
                                                    '', is_first_run=True)
                        task_id = task.id
                        data_extraction_to = datetime.fromtimestamp(float(event_ts))
                        create_slack_connector_channel_scrap_schedule(account_slack_connector.account_id,
                                                                      account_slack_connector.id,
                                                                      channel_id,
                                                                      task_id,
                                                                      data_extraction_to,
                                                                      '')
                    return True
                else:
                    logger.error(f"Error while saving SlackBotConfig for connector: {team_id}:{channel_id}:{event_ts}")
                    return False
            except Exception as e:
                logger.error(f"Error while registering slack bot config for connector: {team_id} with error: {e}")
                return False
        elif event_type == 'member_left_channel':
            try:
                account_slack_connector = active_account_slack_connectors[0]
                for bot_user_id in bot_user_ids:
                    if bot_user_id == user:
                        for active_account_slack_connector in active_account_slack_connectors:
                            if active_account_slack_connector.metadata.get('bot_user_id', None) == bot_user_id:
                                account_slack_connector = active_account_slack_connector
                                break
                        break
                slack_connector_keys = get_slack_connector_channel_key(account_slack_connector.id, channel_id,
                                                                       is_active=True)
                if slack_connector_keys:
                    for slack_connector_key in slack_connector_keys:
                        updated_slack_channel_keys = update_slack_connector_channel_key(slack_connector_key.id,
                                                                                        is_active=False)
                        if updated_slack_channel_keys:
                            workspace_header = team_id
                            if account_slack_connector.metadata.get('team_name', None):
                                workspace_header = account_slack_connector.metadata.get('team_name', None)
                            channel_header = channel_id
                            if updated_slack_channel_keys.metadata.get('channel_name', None):
                                channel_header = updated_slack_channel_keys.metadata.get('channel_name', None)
                            if PUSH_TO_SLACK:
                                message_text = "De-Registered channel for connector: " + "*" + workspace_header + "*" \
                                               + " " + "channel: " + "*" + channel_header + "*" + " " + \
                                               " at event_ts: " + event_ts
                                publish_message_to_slack(message_text)
                    return True
                else:
                    logger.error(f"Error while de-registering slack bot: {team_id}:{channel_id}:{event_ts}")
                    return False
            except Exception as e:
                logger.error(f"Error while de-registering slack bot with error: {e}")
                return False
        elif event_type == 'channel_left':
            if 'api_app_id' in data and data['api_app_id'] == SLACK_APP_ID:
                for active_account_slack_connector in active_account_slack_connectors:
                    active_slack_connector_channel_keys = get_slack_connector_channel_key(
                        active_account_slack_connector.id,
                        channel_id,
                        is_active=True)
                    for slack_channel_keys in active_slack_connector_channel_keys:
                        updated_slack_channel_keys = update_slack_connector_channel_key(slack_channel_keys.id,
                                                                                        is_active=False)
                        if updated_slack_channel_keys:
                            workspace_header = team_id
                            if active_account_slack_connector.metadata.get('team_name', None):
                                workspace_header = active_account_slack_connector.metadata.get('team_name', None)
                            channel_header = channel_id
                            if updated_slack_channel_keys.metadata.get('channel_name', None):
                                channel_header = updated_slack_channel_keys.metadata.get('channel_name', None)
                            if PUSH_TO_SLACK:
                                message_text = "De-Registered channel for connector: " + "*" + workspace_header + "*" \
                                               + " " + "channel: " + "*" + channel_header + "*" + " " + \
                                               " at event_ts: " + event_ts
                                publish_message_to_slack(message_text)
        elif event_type == 'app_uninstalled':
            try:
                for account_slack_connector in active_account_slack_connectors:
                    updated_account_slack_connector = update_account_slack_connector(account_slack_connector.id,
                                                                                     is_active=False)
                    if updated_account_slack_connector:
                        workspace_header = team_id
                        if updated_account_slack_connector.metadata.get('team_name', None):
                            workspace_header = updated_account_slack_connector.metadata.get('team_name', None)
                        if PUSH_TO_SLACK:
                            message_text = "De-Registered slack app from connector: " + "*" + \
                                           workspace_header + "*" + " " + " at event_ts: " + event_ts
                            publish_message_to_slack(message_text)
                return True
            except Exception as e:
                logger.error(f"Error while de-registering slack app in connector: {team_id} with error: {e}")
                return False
        else:
            logger.error(f"Received invalid event type: {event['type']} for connector {team_id}")
            return False
    else:
        logger.error(f"Error handling event in connector {team_id}: No event found in request data: {data}")
        return False
