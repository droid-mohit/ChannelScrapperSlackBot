import json
import uuid
from datetime import datetime

from flask import request
from flask import jsonify, Blueprint

from persistance.db_utils import create_slack_connector_channel_scrap_schedule, get_slack_connector_channel_key, \
    get_connector_by, get_account_slack_connector, get_connector_key_by
from processors.new_relic_rest_client import NewRelicRestApiProcessor
from processors.sentry_client_apis import SentryApiProcessor
from processors.slack_webclient_apis import SlackApiProcessor
from utils.time_utils import get_current_time

app_blueprint = Blueprint('app_router', __name__)


@app_blueprint.route('/health_check', methods=['GET'])
def app_health_check():
    print('Data Scrapper App Backend is Up and Running!')
    return jsonify({'success': True})


# @app_blueprint.route('/register_source_token', methods=['POST'])
# def app_register_source_token():
#     request_data = request.data.decode('utf-8')
#     if request_data:
#         data = json.loads(request_data)
#         if 'user_email' not in data or 'source' not in data or 'token_config' not in data:
#             return jsonify({'success': False, 'message': 'Invalid arguments provided'})
#
#         user_email = data['user_email']
#         source = data['source']
#         token_config = data['token_config']
#         saved_token_config = handler_source_token_registration(user_email, source, token_config)
#         if not saved_token_config:
#             return jsonify({'success': False, 'message': 'Failed to register token config'})
#         return jsonify({'success': True, 'message': 'Token config registered successfully'})


@app_blueprint.route('/slack/start_data_fetch', methods=['GET'])
def slack_start_data_fetch():
    channel_id = request.args.get('channel')
    team_id = request.args.get('team')
    bot_auth_token = request.args.get('token')
    if not channel_id or not bot_auth_token:
        return jsonify({'success': False, 'message': 'Invalid arguments provided'})

    slack_connector_channel_keys = get_slack_connector_channel_key(channel_id=channel_id, is_active=True)
    if not slack_connector_channel_keys:
        return jsonify(
            {'success': False, 'message': f'No active slack connector channel key found for channel_id: {channel_id}'})

    slack_connector_channel_key = slack_connector_channel_keys[0]
    slack_connector = get_account_slack_connector(record_id=slack_connector_channel_key.connector_id)
    slack_connector = slack_connector[0]

    latest_timestamp = request.args.get('latest_timestamp')
    if not latest_timestamp:
        latest_timestamp = str(get_current_time())

    oldest_timestamp = request.args.get('oldest_timestamp')
    if not oldest_timestamp:
        oldest_timestamp = ''

    slack_api_processor = SlackApiProcessor(bot_auth_token)
    slack_api_processor.fetch_conversation_history(team_id, channel_id, latest_timestamp, oldest_timestamp)

    data_extraction_to = datetime.fromtimestamp(float(latest_timestamp))
    data_extraction_from = None
    if oldest_timestamp:
        data_extraction_from = datetime.fromtimestamp(float(oldest_timestamp))
    task_run_id = 'MANUAL#' + uuid.uuid4().hex
    create_slack_connector_channel_scrap_schedule(slack_connector.account_id, slack_connector.id, channel_id,
                                                  task_run_id, data_extraction_to, data_extraction_from)
    return jsonify({'success': True})


@app_blueprint.route('/slack/get_channel_info', methods=['GET'])
def slack_get_channel_info():
    channel_id = request.args.get('channel')
    bot_auth_token = request.args.get('token')
    if not channel_id or not bot_auth_token:
        return jsonify({'success': False, 'message': 'Invalid arguments provided'})

    slack_bot_configs = get_slack_connector_channel_key(channel_id=channel_id, is_active=True)
    if not slack_bot_configs:
        return jsonify({'success': False, 'message': 'No active slack bot configs found for channel_id: {channel_id}'})

    slack_api_processor = SlackApiProcessor(bot_auth_token)
    channel_info = slack_api_processor.fetch_channel_info(channel_id)
    if channel_info:
        return jsonify(**channel_info)
    return jsonify({'success': False, 'message': 'Failed to fetch channel info'})


@app_blueprint.route('/sentry/start_data_fetch', methods=['GET'])
def sentry_start_data_fetch():
    account_id = request.args.get('account_id')
    project_slug = request.args.get('project')
    if not account_id or not project_slug:
        return jsonify({'success': False, 'message': 'Invalid arguments provided'})
    account_sentry_connectors = get_connector_by(account_id=account_id, connector_type=1, is_active=True)
    if not account_sentry_connectors:
        return jsonify(
            {'success': False, 'message': f'No active sentry connector found for account: {account_id}'})

    connector = account_sentry_connectors[0]
    latest_timestamp = request.args.get('latest_timestamp')
    if not latest_timestamp:
        latest_timestamp = str(get_current_time())

    oldest_timestamp = request.args.get('oldest_timestamp')
    if not oldest_timestamp:
        oldest_timestamp = ''

    sentry_connector_key = get_connector_key_by(connector_id=connector.id, is_active=True)
    if not sentry_connector_key:
        return jsonify(
            {'success': False, 'message': f'No active sentry connector key found for connector: {connector.id}'})
    source_token = sentry_connector_key[0].key
    sentry_api_processor = SentryApiProcessor(source_token.token_config['bearer_token'],
                                              source_token.token_config['organization_slug'], project_slug)
    data_fetch_success = sentry_api_processor.fetch_events(latest_timestamp, oldest_timestamp)
    if data_fetch_success:
        return jsonify({'success': True})
    return jsonify({'success': False, 'message': 'Failed to fetch events'})


@app_blueprint.route('/new_relic/fetch_alert_policies_nrql_conditions', methods=['GET'])
def new_relic_fetch_alert_policies_nrql_conditions():
    account_id = request.args.get('account_id')
    nr_policy_id = request.args.get('nr_policy_id')
    if not account_id or not nr_policy_id:
        return jsonify({'success': False, 'message': 'Invalid arguments provided'})
    account_new_relic_connectors = get_connector_by(account_id=int(account_id), connector_type=18, is_active=True)
    if not account_new_relic_connectors:
        return jsonify(
            {'success': False, 'message': f'No active new relic connector found for account: {account_id}'})
    connector = account_new_relic_connectors[0]
    connector_keys = get_connector_key_by(connector_id=connector.id, is_active=True)
    if not connector_keys:
        return jsonify(
            {'success': False, 'message': f'No active new relic connector key found for connector: {connector.id}'})

    nr_api_key = None
    nr_account_id = None
    nr_query_key = None
    for keys in connector_keys:
        if keys.key_type == 4:
            nr_api_key = keys.key
        elif keys.key_type == 5:
            nr_account_id = keys.key
        elif keys.key_type == 7:
            nr_query_key = keys.key
    if not nr_api_key or not nr_account_id:
        return jsonify({'success': False, 'message': 'Connector Keys not found'})
    new_relic_rest_api_processor = NewRelicRestApiProcessor(nr_api_key, nr_account_id, nr_query_key)
    policy_id = []
    if nr_policy_id:
        policy_id.append(nr_policy_id)
    all_policies_nrql_conditions = new_relic_rest_api_processor.fetch_alert_policies_nrql_conditions(policy_id)
    if not all_policies_nrql_conditions:
        return jsonify({'success': False, 'message': 'Failed to fetch alert policies nrql conditions'})

    return jsonify({'success': True})


@app_blueprint.route('/new_relic/fetch_alert_violations', methods=['GET'])
def new_relic_fetch_alert_violations():
    account_id = request.args.get('account_id')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    if not account_id:
        return jsonify({'success': False, 'message': 'Invalid arguments provided'})

    account_new_relic_connectors = get_connector_by(account_id=int(account_id), connector_type=18, is_active=True)
    if not account_new_relic_connectors:
        return jsonify(
            {'success': False, 'message': f'No active new relic connector found for account: {account_id}'})
    connector = account_new_relic_connectors[0]
    connector_keys = get_connector_key_by(connector_id=connector.id, is_active=True)
    if not connector_keys:
        return jsonify(
            {'success': False, 'message': f'No active new relic connector key found for connector: {connector.id}'})
    nr_api_key = None
    nr_account_id = None
    nr_query_key = None
    for keys in connector_keys:
        if keys.key_type == 4:
            nr_api_key = keys.value
        elif keys.key_type == 5:
            nr_account_id = keys.value
        elif keys.key_type == 7:
            nr_query_key = keys.value
    if not nr_api_key or not nr_account_id:
        return jsonify({'success': False, 'message': 'Connector Keys not found'})
    new_relic_rest_api_processor = NewRelicRestApiProcessor(nr_api_key, nr_account_id, nr_query_key)
    data_fetch_success = new_relic_rest_api_processor.fetch_alert_violations(start_date, end_date)
    if data_fetch_success:
        return jsonify({'success': True})
    return jsonify({'success': False, 'message': 'Failed to fetch alert violations'})
