import json
import logging
from typing import Dict

from env_vars import PUSH_TO_S3, METADATA_S3_BUCKET_NAME, PUSH_TO_SLACK
from persistance.db_utils import get_slack_workspace_config_by, create_slack_bot_config, create_slack_workspace_config, \
    get_slack_bot_configs_by, update_slack_bot_config, update_slack_workspace_config
from processors.slack_webclient_apis import SlackApiProcessor
from utils.publishsing_client import publish_json_blob_to_s3, publish_message_to_slack
from utils.time_utils import get_current_datetime

logger = logging.getLogger(__name__)


def handle_oauth_callback(data: Dict):
    if data['ok'] and 'token_type' in data and data['token_type'] == 'bot' and 'team' in data:
        try:
            current_datetime = get_current_datetime()
            team_id = data['team'].get('id', None)
            team_name = data['team'].get('name', None)
            bot_oauth_token = data.get('access_token', None)
            bot_user_id = data.get('bot_user_id', None)

            if not team_id or not bot_oauth_token or not bot_user_id:
                logger.error(f"Error while fetching bot OAuth token with response: {data}")
                return False

            logger.info(f"Received Bot OAuth token for workspace {team_id} : ({team_name})")
            slack_workspace, is_created = create_slack_workspace_config(team_id, bot_user_id, bot_oauth_token,
                                                                        team_name)
            if slack_workspace and is_created:
                if PUSH_TO_S3:
                    data_to_upload = slack_workspace.to_dict()
                    json_data = json.dumps(data_to_upload)
                    key = f'{team_id}-{team_name}-{current_datetime}.json'
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
    print(data)
    if 'team_id' not in data or 'event' not in data:
        logger.error(f"Error handling slack event callback api, team_id or event not found in request data: {data}")
        return False
    team_id = data['team_id']
    event = data['event']
    active_slack_workspaces = get_slack_workspace_config_by(team_id=team_id, is_active=True)
    if not active_slack_workspaces:
        logger.error(f"Error handling slack event callback api for {team_id}: active slack workspace not found")
        return False
    bot_user_ids = []
    for workspace in active_slack_workspaces:
        bot_user_ids.append(workspace.bot_user_id)
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
        if event_type != 'app_uninstalled' and user not in bot_user_ids:
            logger.error(f"Error handling {event_type} event type for workspace {team_id}: {user} is not bot user")
            return False
        channel_id = event.get('channel', None)
        if (event_type != 'app_uninstalled') and (not channel_id or not event_ts):
            logger.error(
                f"Error handling {event_type} event type for workspace {team_id}: channel_id/event_ts not found")
            return False
        if event_type == 'member_joined_channel' or event_type == 'app_mention':
            try:
                slack_workspace = active_slack_workspaces[0]
                for bot_user_id in bot_user_ids:
                    if bot_user_id == user:
                        slack_workspace = active_slack_workspaces[bot_user_ids.index(bot_user_id)]
                        break
                bot_auth_token = slack_workspace.bot_auth_token
                slack_api_processor = SlackApiProcessor(bot_auth_token)
                channel_name = None
                channel_info = slack_api_processor.fetch_channel_info(channel_id)
                if channel_info:
                    if 'name' in channel_info:
                        channel_name = channel_info['name']
                slack_bot_config, is_created = create_slack_bot_config(slack_workspace.id,
                                                                       channel_id,
                                                                       event_ts,
                                                                       channel_name)
                if slack_bot_config:
                    if is_created:
                        workspace_header = team_id
                        if slack_workspace.team_name:
                            workspace_header = slack_workspace.team_name
                        channel_header = channel_id
                        if channel_name:
                            channel_header = channel_name
                        if PUSH_TO_S3:
                            current_datetime = get_current_datetime()
                            data_to_upload = slack_bot_config.to_dict()
                            json_data = json.dumps(data_to_upload)
                            key = f'{workspace_header}-{channel_header}-{current_datetime}.json'
                            publish_json_blob_to_s3(key, METADATA_S3_BUCKET_NAME, json_data)
                        if PUSH_TO_SLACK:
                            message_text = "Registered channel for slack workspace: " + "*" + workspace_header + "*" \
                                           + " " + "channel: " + "*" + channel_header + "*" + " at " + "event_ts: " \
                                           + event_ts
                            publish_message_to_slack(message_text)
                    return True
                else:
                    logger.error(f"Error while saving SlackBotConfig for workspace: {team_id}:{channel_id}:{event_ts}")
                    return False
            except Exception as e:
                logger.error(f"Error while registering slack bot config for workspace: {team_id} with error: {e}")
                return False
        elif event_type == 'member_left_channel':
            try:
                slack_workspace = active_slack_workspaces[0]
                for bot_user_id in bot_user_ids:
                    if bot_user_id == user:
                        slack_workspace = active_slack_workspaces[bot_user_ids.index(bot_user_id)]
                        break
                slack_bot_configs = get_slack_bot_configs_by(slack_workspace.id, channel_id, is_active=True)
                if slack_bot_configs:
                    for slack_bot_config in slack_bot_configs:
                        updated_slack_bot_config = update_slack_bot_config(slack_bot_config, is_active=False)
                        if updated_slack_bot_config:
                            workspace_header = team_id
                            if slack_workspace.team_name:
                                workspace_header = slack_workspace.team_name
                            channel_header = channel_id
                            if updated_slack_bot_config.channel_name:
                                channel_header = updated_slack_bot_config.channel_name
                            if PUSH_TO_SLACK:
                                message_text = "De-Registered channel for workspace: " + "*" + workspace_header + "*" \
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
        elif event_type == 'app_uninstalled':
            try:
                for slack_workspace in active_slack_workspaces:
                    updated_slack_workspace = update_slack_workspace_config(slack_workspace, is_active=False)
                    if updated_slack_workspace:
                        workspace_header = updated_slack_workspace.team_id
                        if updated_slack_workspace.team_name:
                            workspace_header = updated_slack_workspace.team_name
                        active_slack_bots = get_slack_bot_configs_by(updated_slack_workspace.id, is_active=True)
                        for slack_bot in active_slack_bots:
                            updated_slack_bot_config = update_slack_bot_config(slack_bot, is_active=False)
                            if updated_slack_bot_config:
                                channel_header = updated_slack_bot_config.channel_id
                                if updated_slack_bot_config.channel_name:
                                    channel_header = updated_slack_bot_config.channel_name
                                if PUSH_TO_SLACK:
                                    message_text = "De-Registered slack app from workspace: " + "*" + \
                                                   workspace_header + "*" + " " + "channel: " + "*" + \
                                                   channel_header + "*" + " " + " at event_ts: " + event_ts
                                    publish_message_to_slack(message_text)
                return True
            except Exception as e:
                logger.error(f"Error while de-registering slack app in workspace: {team_id} with error: {e}")
                return False
        else:
            logger.error(f"Received invalid event type: {event['type']} for workspace {team_id}")
            return False
    else:
        logger.error(f"Error handling event in workspace {team_id}: No event found in request data: {data}")
        return False
