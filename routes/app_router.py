from flask import request
from flask import jsonify, Blueprint

from processors.slack_webclient_apis import SlackApiProcessor

app_blueprint = Blueprint('app_router', __name__)


@app_blueprint.route('/health_check', methods=['GET'])
def health_check():
    print('Data Scrapper App Backend is Up and Running!')
    return jsonify({'success': True})


@app_blueprint.route('/start_data_fetch', methods=['GET'])
def start_data_fetch():
    channel_id = request.args.get('channel')
    bot_auth_token = request.args.get('token')
    slack_api_processor = SlackApiProcessor(bot_auth_token)
    slack_api_processor.fetch_conversation_history(channel_id)
    return jsonify({'success': True})
