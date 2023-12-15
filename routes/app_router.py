import json
from datetime import datetime

from flask import request
from flask import jsonify, Blueprint

from persistance.db_utils import create_slack_channel_scrap_schedule, get_slack_bot_configs_by
from processors.slack_webclient_apis import SlackApiProcessor
from route_handlers.app_route_handler import handler_source_token_registration
from utils.time_utils import get_current_time

app_blueprint = Blueprint('app_router', __name__)


@app_blueprint.route('/health_check', methods=['GET'])
def health_check():
    print('Data Scrapper App Backend is Up and Running!')
    return jsonify({'success': True})


@app_blueprint.route('/slack/start_data_fetch', methods=['GET'])
def start_data_fetch():
    channel_id = request.args.get('channel')
    bot_auth_token = request.args.get('token')
    if not channel_id or not bot_auth_token:
        return jsonify({'success': False, 'message': 'Invalid arguments provided'})

    slack_bot_configs = get_slack_bot_configs_by(channel_id=channel_id, is_active=True)
    if not slack_bot_configs:
        return jsonify({'success': False, 'message': 'No active slack bot configs found for channel_id: {channel_id}'})

    slack_bot_config = slack_bot_configs[0]
    latest_timestamp = request.args.get('latest_timestamp')
    if not latest_timestamp:
        latest_timestamp = str(get_current_time())

    oldest_timestamp = request.args.get('oldest_timestamp')
    if not oldest_timestamp:
        oldest_timestamp = ''

    slack_api_processor = SlackApiProcessor(bot_auth_token)
    slack_api_processor.fetch_conversation_history(channel_id, latest_timestamp, oldest_timestamp)

    data_extraction_to = datetime.fromtimestamp(float(latest_timestamp))
    data_extraction_from = None
    if oldest_timestamp:
        data_extraction_from = datetime.fromtimestamp(float(oldest_timestamp))
    create_slack_channel_scrap_schedule(slack_bot_config.id, data_extraction_from, data_extraction_to)
    return jsonify({'success': True})


@app_blueprint.route('/slack/get_channel_info', methods=['GET'])
def get_channel_info():
    channel_id = request.args.get('channel')
    bot_auth_token = request.args.get('token')
    if not channel_id or not bot_auth_token:
        return jsonify({'success': False, 'message': 'Invalid arguments provided'})

    slack_bot_configs = get_slack_bot_configs_by(channel_id=channel_id, is_active=True)
    if not slack_bot_configs:
        return jsonify({'success': False, 'message': 'No active slack bot configs found for channel_id: {channel_id}'})

    slack_api_processor = SlackApiProcessor(bot_auth_token)
    channel_info = slack_api_processor.fetch_channel_info(channel_id)
    if channel_info:
        return jsonify(**channel_info)
    return jsonify({'success': False, 'message': 'Failed to fetch channel info'})


@app_blueprint.route('/register_source_token', methods=['POST'])
def register_source_token():
    request_data = request.data.decode('utf-8')
    if request_data:
        data = json.loads(request_data)
        if 'user_email' not in data or 'source' not in data or 'token_config' not in data:
            return jsonify({'success': False, 'message': 'Invalid arguments provided'})

        user_email = data['user_email']
        source = data['source']
        token_config = data['token_config']
        saved_token_config = handler_source_token_registration(user_email, source, token_config)
        if not saved_token_config:
            return jsonify({'success': False, 'message': 'Failed to register token config'})
        return jsonify({'success': True, 'message': 'Token config registered successfully'})
