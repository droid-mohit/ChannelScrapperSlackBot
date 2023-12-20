import hashlib
import json
import logging
from typing import Dict

from sqlalchemy import text

from persistance.models import db

logger = logging.getLogger(__name__)


def clean_sql_clause_values(filters: Dict):
    for f in filters:
        if isinstance(filters[f], str) and not filters[f].startswith("'") and not filters[f].endswith("'") and not \
                filters[f].startswith("jsonb_set"):
            filters[f] = f"'{filters[f]}'"
        if filters[f] is None:
            filters[f] = 'NULL'
    return filters


def get_data(table_name: str, filters: Dict = None, order_by_clause: str = None, limit: int = None):
    try:
        where_clause = None
        if filters:
            filters = clean_sql_clause_values(filters)
            where_clause = ' AND '.join([f'{key} = {value}' for key, value in filters.items()])
        sql_query = f"SELECT * FROM {table_name}"
        if where_clause:
            sql_query += f" WHERE {where_clause}"
        if order_by_clause:
            sql_query += f" ORDER BY {order_by_clause}"
        if limit:
            sql_query += f" LIMIT {limit}"
        with db.engine.connect() as connection:
            result = connection.execute(text(sql_query))
            return result.fetchall()
    except Exception as e:
        logger.error(f"Error while fetching {table_name} with error: {e}")
    return None


def update_data(table_name: str, record_id, updated_data: Dict):
    try:
        updated_data = clean_sql_clause_values(updated_data)
        set_clause = ", ".join([f'{key} = {value}' for key, value in updated_data.items()])
        where_clause = f'id = {record_id}'
        sql_query = f"UPDATE {table_name} SET {set_clause} WHERE {where_clause}"
        with db.engine.connect() as connection:
            result = connection.execute(text(sql_query))
            connection.commit()
            if result.rowcount > 0:
                filters = {'id': record_id}
                updated_rows = get_data(table_name, filters)
                if updated_rows:
                    return updated_rows[0]
    except Exception as e:
        logger.error(f"Error while updating {table_name} with error: {e}")
    return None


def create_data(table_name: str, data: Dict):
    try:
        data = clean_sql_clause_values(data)
        columns = ", ".join([f'{key}' for key in data.keys()])
        values = ", ".join([f'{value}' for value in data.values()])
        sql_query = f"INSERT INTO {table_name} ({columns}) VALUES ({values}) RETURNING *"
        with db.engine.connect() as connection:
            result = connection.execute(text(sql_query))
            new_row = result.fetchone()
            connection.commit()
            return new_row
    except Exception as e:
        logger.error(f"Error while updating {table_name} with error: {e}")
    return None


def get_account_for_user_email(user_email: str):
    """
    Fetch kenobi account id for user_email.
    """

    if not user_email:
        return None
    filters = {'email': user_email}
    return get_data('accounts_user', filters)


def get_account_slack_connector(team_id: str = None, account_id: int = None, team_name: str = None,
                                bot_user_id: str = None,
                                bot_auth_token: str = None, is_active: bool = None, record_id: int = None):
    """
    Fetch a AccountSlackConnector row based on different options.
    """
    if not record_id and not team_id:
        return None

    filters = {}
    if record_id:
        filters['id'] = record_id
    if team_name:
        filters[f"metadata->>'team_id'"] = team_id
    if account_id:
        filters['account_id'] = account_id
    if team_name:
        filters[f"metadata->>'team_name'"] = team_name
    if bot_user_id:
        filters[f"metadata->>'bot_user_id'"] = bot_user_id
    if bot_auth_token:
        filters[f"metadata->>'bot_auth_token'"] = bot_auth_token
    if is_active is not None:
        filters['is_active'] = is_active
    return get_data('connectors_connector', filters)


def create_account_slack_connector(account_id: int, team_id: str, bot_user_id: str, bot_auth_token: str,
                                   team_name: str = None, should_update=True):
    try:
        account_slack_connectors = get_account_slack_connector(team_id=team_id,
                                                               account_id=account_id,
                                                               team_name=team_name,
                                                               bot_user_id=bot_user_id,
                                                               bot_auth_token=bot_auth_token)
        if account_slack_connectors:
            account_slack_connector = account_slack_connectors[0]
            if not should_update:
                return account_slack_connector, False
            else:
                updated_slack_workspace_config = update_account_slack_connector(account_slack_connector.id,
                                                                                team_name,
                                                                                True)
                if updated_slack_workspace_config:
                    return updated_slack_workspace_config, True
                else:
                    return None, False

        metadata = {'team_id': team_id, 'bot_user_id': bot_user_id, 'team_name': team_name,
                    'bot_auth_token': bot_auth_token}
        metadata_json = json.dumps(metadata)
        metadata_md5 = hashlib.md5(metadata_json.encode('utf-8')).hexdigest()

        if not team_name:
            team_name = team_name
        new_account_slack_connector = create_data('connectors_connector', {
            'account_id': account_id,
            'name': team_name,
            'connector_type': 19,
            'metadata': metadata_json,
            'is_active': True,
            'metadata_md5': metadata_md5,
            'created_at': 'now()',
            'updated_at': 'now()'
        })
        if new_account_slack_connector:
            new_account_slack_connector_key = create_data('connectors_connectorkey', {
                'account_id': account_id,
                'connector_id': new_account_slack_connector.id,
                'key_type': 8,
                'key': bot_auth_token,
                'is_active': True,
                'created_at': 'now()',
                'updated_at': 'now()'
            })
        if new_account_slack_connector:
            return new_account_slack_connector, True
    except Exception as e:
        logger.error(f"Error while saving AccountSlackConnector: {team_id}:{team_name} with error: {e}")
    return None, False


def update_account_slack_connector(record_id, team_name: str = None, is_active: bool = None):
    """
    Update an existing AccountSlackConnector instance in the database.
    """
    try:
        updated_data = {}
        if team_name:
            updated_data['metadata'] = f"jsonb_set(metadata, '{{team_name}}', '\"{team_name}\"')"
        if is_active is not None:
            updated_data['is_active'] = is_active
        updated_row = update_data('connectors_connector', record_id, updated_data)
        if is_active is not None and updated_row:
            updated_row_keys = get_data('connectors_connectorkey', {'connector_id': record_id})
            if updated_row_keys:
                for updated_row_key in updated_row_keys:
                    update_data('connectors_connectorkey', updated_row_key.id, {'is_active': is_active})
        return updated_row
    except Exception as e:
        logger.error(f"Error while updating AccountSlackConnector with error: {e}")
    return None


def get_slack_connector_channel_key(account_slack_connector_id: str = None, channel_id: str = None,
                                    is_active: bool = None):
    """
    Fetch a SlackConnectorChannelKey row based on different options.
    """
    filters = {'key_type': 9}
    if account_slack_connector_id:
        filters['connector_id'] = account_slack_connector_id
    if channel_id:
        filters['key'] = channel_id
    if is_active is not None:
        filters['is_active'] = is_active

    return get_data('connectors_connectorkey', filters)


def create_slack_connector_channel_key(account_id, account_slack_connector_id, channel_id, event_ts, channel_name=None):
    """
    Create a new SlackConnectorChannelKey instance and add it to the database.
    """
    try:
        slack_connector_channel_keys = get_slack_connector_channel_key(account_slack_connector_id, channel_id)
        if slack_connector_channel_keys:
            slack_connector_channel_key = slack_connector_channel_keys[0]
            if slack_connector_channel_key and not slack_connector_channel_key.is_active:
                updated_slack_connector_channel_key = update_slack_connector_channel_key(slack_connector_channel_key.id,
                                                                                         is_active=True)
                if updated_slack_connector_channel_key:
                    return updated_slack_connector_channel_key, True
                else:
                    return None, False
            return slack_connector_channel_key, False

        new_slack_connector_channel_key = create_data('connectors_connectorkey', {
            'account_id': account_id,
            'connector_id': account_slack_connector_id,
            'key_type': 9,
            'key': channel_id,
            'metadata': json.dumps({'channel_name': channel_name, 'event_ts': event_ts}),
            'is_active': True,
            'created_at': 'now()',
            'updated_at': 'now()'
        })

        if new_slack_connector_channel_key:
            return new_slack_connector_channel_key, True
    except Exception as e:
        logger.error(
            f"Error while saving SlackConnectorChannelKey: {account_slack_connector_id}:{channel_id} with error: {e}")
    return None, False


def update_slack_connector_channel_key(record_id, event_ts: str = None, channel_name: str = None,
                                       is_active: bool = None):
    """
    Update an existing SlackConnectorChannelKey instance in the database.
    """
    try:
        updated_data = {}
        if channel_name:
            updated_data['metadata'] = f"jsonb_set(metadata, '{{channel_name}}', '\"{channel_name}\"')"
        if event_ts:
            updated_data['metadata'] = f"jsonb_set(metadata, '{{event_ts}}', '\"{event_ts}\"')"
        if is_active is not None:
            updated_data['is_active'] = is_active
        updated_row = update_data('connectors_connectorkey', record_id, updated_data)
        return updated_row
    except Exception as e:
        logger.error(f"Error while updating SlackConnectorChannelKey with error: {e}")
    return None


def create_slack_connector_channel_scrap_schedule(account_id, slack_connector_id, channel_id, task_run_id,
                                                  data_extraction_to, data_extraction_from):
    """
    Create a new SlackChannelDataScrapSchedule instance and add it to the database.
    """
    try:
        new_slack_connector_data_scrap_schedule = create_data('connectors_connectorperiodicrunmetadata', {
            'account_id': account_id,
            'connector_id': slack_connector_id,
            'metadata': json.dumps({'data_extraction_from': str(data_extraction_from),
                                    'data_extraction_to': str(data_extraction_to),
                                    'channel_id': channel_id}),
            'task_run_id': task_run_id,
            'status': 1,
            'started_at': 'now()',
            'finished_at': None,
        })
        if new_slack_connector_data_scrap_schedule:
            return new_slack_connector_data_scrap_schedule, True
    except Exception as e:
        logger.error(
            f"Error while saving SlackConnectorDataScrapSchedule: "
            f":{slack_connector_id}:{data_extraction_from}:{data_extraction_to} with error: {e}")
    return None, False


def get_latest_slack_connector_scrap_schedule_for_channel(connector_id, channel_id: str):
    filters = {'connector_id': connector_id, f"metadata->>'channel_id'": channel_id}
    slack_connector_channel_schedule = get_data('connectors_connectorperiodicrunmetadata', filters, 'started_at DESC',
                                                1)
    return slack_connector_channel_schedule[0] if slack_connector_channel_schedule else None


def create_connector_extract_data(account_id, connector_id, channel_id, data_uuid, full_message):
    """
        Create a new SlackConnectorChannelKey instance and add it to the database.
        """
    try:
        full_message_json = json.dumps(full_message, ensure_ascii=False)
        full_message_json = full_message_json.replace("'", "''")
        alert_count_data = create_data('connectors_connectorsourceextractdata', {
            'account_id': account_id,
            'connector_id': connector_id,
            'source': channel_id,
            'data_uuid': data_uuid,
            'data': full_message_json,
            'created_at': 'now()',
            'updated_at': 'now()'
        })

        if alert_count_data:
            return alert_count_data, True
    except Exception as e:
        print("Error while saving ConnectorExtractData: ", e)
    return None, False


def create_alert_count_data(account_id, count_timestamp, channel_id, alert_type, count):
    """
        Create a new SlackConnectorChannelKey instance and add it to the database.
        """
    try:

        alert_count_data = create_data('connectors_alertcountdata', {
            'account_id': account_id,
            'count_timestamp': count_timestamp,
            'source': channel_id,
            'type': alert_type,
            'count': count,
            'created_at': 'now()',
            'updated_at': 'now()'
        })

        if alert_count_data:
            return alert_count_data, True
    except Exception as e:
        logger.error(
            f"Error while saving AlertCountData: {account_id}:{channel_id}:{alert_type}:{count} with error: {e}")
    return None, False


def get_connector_by(record_id: str = None, account_id: int = None, name: str = None, connector_type: int = None,
                     metadata: Dict = None, is_active: bool = None):
    """
    Fetch a SourceTokenRepository row based on different options.
    """
    filters = {}
    if record_id:
        filters['id'] = record_id
    if account_id:
        filters['account_id'] = account_id
    if name:
        filters['name'] = name
    if connector_type:
        filters['connector_type'] = connector_type
    if metadata:
        for key, value in metadata.items():
            filters[f"metadata->>'{key}'"] = value
    if is_active is not None:
        filters['is_active'] = is_active

    connectors = get_data('connectors_connector', filters)
    return connectors


def get_connector_key_by(record_id: str = None, account_id: int = None, connector_id: int = None, key_type: int = None,
                         metadata: Dict = None, is_active: bool = None):
    """
    Fetch a SourceTokenRepository row based on different options.
    """
    filters = {}
    if record_id:
        filters['id'] = record_id
    if account_id:
        filters['account_id'] = account_id
    if connector_id:
        filters['connector_id'] = connector_id
    if key_type:
        filters['key_type'] = key_type
    if metadata:
        for key, value in metadata.items():
            filters[f"metadata->>'{key}'"] = value
    if is_active is not None:
        filters['is_active'] = is_active

    connectors = get_data('connectors_connectorkey', filters)
    return connectors

# def update_source_token_config(record_id, is_active: bool = None):
#     """
#     Update an existing SourceTokenRepository instance in the database.
#     """
#     try:
#         updated_data = {}
#         if is_active is not None:
#             updated_data['is_active'] = is_active
#         updated_row = update_data('source_token_repository', record_id, updated_data)
#         return updated_row
#     except Exception as e:
#         logger.error(f"Error while updating SourceTokenRepository: {record_id} with error: {e}")
#     return None


# def create_source_token_config(user_email, source, token_config):
#     """
#         Create a new SourceTokenRepository instance and add it to the database.
#     """
#     try:
#         token_config_md5 = hashlib.md5(json.dumps(token_config).encode('utf-8')).hexdigest()
#         source_token_configs = get_source_token_config_by(user_email=user_email, source=source,
#                                                           token_config_md5=token_config_md5)
#         if source_token_configs:
#             source_token_config = source_token_configs[0]
#             if source_token_config and not source_token_config.is_active:
#                 updated_source_token_config = update_source_token_config(source_token_config.id, is_active=True)
#                 if updated_source_token_config:
#                     return updated_source_token_config, True
#                 else:
#                     return None, False
#             else:
#                 return source_token_config, False
#
#         new_source_token = create_data('source_token_repository', {
#             'user_email': user_email,
#             'source': source,
#             'token_config': token_config,
#             'token_config_md5': token_config_md5,
#             'is_active': True
#         })
#         if new_source_token:
#             return new_source_token, True
#     except Exception as e:
#         logger.error(f"Error while saving Source Token: :{user_email}:{source} with error: {e}")
#     return None, False
