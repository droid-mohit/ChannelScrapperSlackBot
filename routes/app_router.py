from flask import request
from flask import jsonify, Blueprint

from processors.slack_webclient_apis import SlackApiProcessor
from utils.time_utils import get_current_time

app_blueprint = Blueprint('app_router', __name__)


@app_blueprint.route('/health_check', methods=['GET'])
def health_check():
    print('Data Scrapper App Backend is Up and Running!')
    return jsonify({'success': True})


@app_blueprint.route('/start_data_fetch', methods=['GET'])
def start_data_fetch():
    channel_id = request.args.get('channel')
    bot_auth_token = request.args.get('token')
    if not channel_id or not bot_auth_token:
        return jsonify({'success': False, 'message': 'Invalid arguments provided'})

    latest_timestamp = request.args.get('latest_timestamp')
    if not latest_timestamp:
        latest_timestamp = str(get_current_time())

    oldest_timestamp = request.args.get('oldest_timestamp')
    if not oldest_timestamp:
        oldest_timestamp = None

    slack_api_processor = SlackApiProcessor(bot_auth_token)
    slack_api_processor.fetch_conversation_history(channel_id, latest_timestamp, oldest_timestamp)
    return jsonify({'success': True})
