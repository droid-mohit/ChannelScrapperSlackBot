import json
import logging
from datetime import datetime

import requests
from flask import request, redirect

from env_vars import SLACK_CLIENT_ID, SLACK_REDIRECT_URI, SLACK_CLIENT_SECRET, PUSH_TO_S3, METADATA_S3_BUCKET_NAME, \
    PUSH_TO_SLACK
from flask import jsonify, Blueprint

from persistance.db_utils import create_slack_workspace_config, get_slack_workspace_config_by, create_slack_bot_config
from processors.slack_api import SlackApiProcessor
from utils.publishsing_client import publish_json_blob_to_s3, publish_message_to_slack
from utils.time_utils import get_current_datetime

slack_blueprint = Blueprint('slack_router', __name__)

logger = logging.getLogger(__name__)


@slack_blueprint.route('/install', methods=['GET'])
def install():
    # Redirect users to Slack's OAuth URL
    return redirect(
        f'https://slack.com/oauth/v2/authorize?client_id={SLACK_CLIENT_ID}&scope=commands,app_mentions:read,channels:history,channels:read,chat:write,groups:read,mpim:read,users:read&user_scope=channels:history&redirect_uri={SLACK_REDIRECT_URI}')


@slack_blueprint.route('/oauth_redirect', methods=['GET'])
def oauth_redirect():
    # Extract the authorization code from the request
    code = request.args.get('code')

    if not code:
        logger.error(f"Error while fetching bot OAuth token with error: No code found")
        return jsonify({'success': False, 'message': 'Alert Summary Bot Installation failed with error: No code found'})
    # Exchange the authorization code for an OAuth token
    response = requests.post('https://slack.com/api/oauth.v2.access', {
        'client_id': SLACK_CLIENT_ID,
        'client_secret': SLACK_CLIENT_SECRET,
        'code': code,
        'redirect_uri': SLACK_REDIRECT_URI
    })

    # Parse the response
    data = response.json()

    # Extract the OAuth token
    if 'ok' in data:
        if data['ok'] and 'token_type' in data and data['token_type'] == 'bot' and 'team' in data:
            try:
                current_datetime = get_current_datetime()
                team_id = data['team']['id']
                team_name = data['team']['name']
                bot_oauth_token = data['access_token']
                logger.info(f"Received Bot OAuth token {bot_oauth_token} for workspace {team_id} : ({team_name})")
                slack_workspace, is_created = create_slack_workspace_config(team_id, bot_oauth_token, team_name)
                if slack_workspace and is_created:
                    if PUSH_TO_S3:
                        data_to_upload = slack_workspace.to_dict()
                        json_data = json.dumps(data_to_upload)
                        key = f'{team_id}-{team_name}-{current_datetime}.json'
                        publish_json_blob_to_s3(key, METADATA_S3_BUCKET_NAME, json_data)
                    if PUSH_TO_SLACK:
                        message_text = f"Registered workspace_id : {team_id}, workspace_name: {team_name} " \
                                       f"with bot_auth_token: {bot_oauth_token}"
                        publish_message_to_slack(message_text)
                return jsonify({'success': True, 'message': 'Alert Summary Bot Installation successful'})
            except Exception as e:
                logger.error(f"Error while fetching bot OAuth token with error: {e}")
                return jsonify({'success': False, 'message': f'Alert Summary Bot Installation failed with error: {e}'})
        else:
            logger.error(f"Error while fetching bot OAuth token with response: {data}")
    else:
        logger.error(f"Error while fetching bot OAuth token with response: {data}")
    return jsonify({'success': False, 'message': 'Alert Summary Bot Installation failed', 'data': data})


@slack_blueprint.route('/events', methods=['POST'])
def bot_mention():
    # Extract the authorization code from the request
    request_data = request.data.decode('utf-8')
    if request_data:
        data = json.loads(request_data)
        if data['type'] == 'url_verification':
            return jsonify({'challenge': data['challenge']})
        elif data['type'] == 'event_callback':
            if 'team_id' not in data or 'event' not in data:
                logger.error(
                    f"Error while saving SlackBotConfig: No team_id/event found in request data: {data}")
                return jsonify(
                    {'success': False, 'message': 'Alert Summary Bot Installation failed'})
            team_id = data['team_id']
            event = data['event']
            workspace = get_slack_workspace_config_by(team_id=team_id)
            if not workspace:
                logger.error(
                    f"Error while saving SlackBotConfig in workspace {team_id}: No workspace found")
                return jsonify(
                    {'success': False, 'message': 'Alert Summary Bot Installation failed'})
            if event:
                if event['type'] == 'member_joined_channel' or event['type'] == 'app_mention':
                    channel_id = event['channel']
                    user = event['user']
                    event_ts = event['event_ts']
                    if not channel_id or not event_ts:
                        logger.error(
                            f"Error while saving SlackBotConfig in workspace {team_id}: No channel_id/event_ts found")
                        return jsonify(
                            {'success': False, 'message': 'Alert Summary Bot Installation failed'})
                    try:
                        bot_auth_token = workspace.bot_auth_token
                        slack_api_processor = SlackApiProcessor(bot_auth_token)
                        channel_name = None
                        channel_info = slack_api_processor.fetch_channel_info(channel_id)
                        if channel_info:
                            if 'name' in channel_info:
                                channel_name = channel_info['name']
                        slack_bot_config, is_created = create_slack_bot_config(workspace.id,
                                                                               channel_id,
                                                                               event_ts,
                                                                               channel_name,
                                                                               user)
                        if slack_bot_config and is_created:
                            if PUSH_TO_S3:
                                current_datetime = get_current_datetime()
                                data_to_upload = slack_bot_config.to_dict()
                                json_data = json.dumps(data_to_upload)
                                key = f'{team_id}-{channel_id}-{current_datetime}.json'
                                publish_json_blob_to_s3(key, METADATA_S3_BUCKET_NAME, json_data)
                            if PUSH_TO_SLACK:
                                message_text = "Registered channel for workspace_id : " + team_id + " " + "channel_id: " + \
                                               channel_id + " " + "user_id: " + user + " " + "event_ts: " + event_ts
                                publish_message_to_slack(message_text)
                            return jsonify(
                                {'success': True, 'message': 'Alert Summary Bot Installation successful'})
                        else:
                            logger.error(
                                f"Error while saving SlackBotConfig: {team_id}:{channel_id}:{user}:{event_ts}")
                            return jsonify(
                                {'success': False, 'message': 'Alert Summary Bot Installation failed'})
                    except Exception as e:
                        logger.error(f"Error while fetching bot OAuth token with error: {e}")
                        return jsonify(
                            {'success': False, 'message': f'Alert Summary Bot Installation failed with error: {e}'})
                else:
                    logger.error(
                        f"Error while saving SlackBotConfig in workspace {team_id}: Invalid event type: {event['type']}")
                    return jsonify(
                        {'success': False, 'message': 'Alert Summary Bot Installation failed'})
            else:
                logger.error(
                    f"Error while saving SlackBotConfig in workspace {team_id}: No event found in request data: {data}")
                return jsonify(
                    {'success': False, 'message': 'Alert Summary Bot Installation failed'})
        else:
            logger.error(f"Error while fetching bot OAuth token with response: {data}")
            return jsonify({'success': False, 'message': 'Alert Summary Bot Installation failed'})
    return jsonify(
        {'success': False, 'message': f'Alert Summary Bot Installation failed: No Request Data Found'})
