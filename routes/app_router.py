from flask import request
from jobs.tasks import data_fetch_job
from flask import jsonify, Blueprint

app_blueprint = Blueprint('app_router', __name__)


@app_blueprint.route('/health_check', methods=['GET'])
def health_check():
    print('Data Scrapper App Backend is Up and Running!')
    return jsonify({'success': True})


@app_blueprint.route('/start_data_fetch', methods=['GET'])
def start_data_fetch():
    channel_id = request.args.get('channel')
    bot_auth_token = request.args.get('token')
    data_fetch_job.delay(bot_auth_token, channel_id)
    return jsonify({'success': True})
