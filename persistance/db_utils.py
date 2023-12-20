import hashlib
import json
import logging
from typing import Dict

from sqlalchemy import text

from persistance.models import db

logger = logging.getLogger(__name__)


def clean_sql_clause_values(filters: Dict):
    for f in filters:
        if isinstance(filters[f], str) and not filters[f].startswith("'") and not filters[f].endswith("'"):
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


def get_account_id_for_user(user_email: str):
    """
    Fetch kenobi account id for user_email.
    """

    if not user_email:
        return None
    filters = {'email': user_email}
    return get_data('accounts_user', filters)


def get_slack_workspace_config_by(team_id: str, bot_user_id: str = None, bot_auth_token: str = None,
                                  team_name: str = None, account_id: int = None, user_email: str = None,
                                  is_active: bool = None):
    """
    Fetch a SlackWorkspaceConfig row based on different options.
    """
    filters = {}
    if team_id:
        filters['team_id'] = team_id
    if team_name:
        filters['team_name'] = team_name
    if bot_user_id:
        filters['bot_user_id'] = bot_user_id
    if bot_auth_token:
        filters['bot_auth_token'] = bot_auth_token
    if account_id:
        filters['account_id'] = account_id
    if user_email:
        filters['user_email'] = user_email
    if is_active is not None:
        filters['is_active'] = is_active
    return get_data('slack_workspace_config', filters)


def create_slack_workspace_config(team_id: str, bot_user_id: str, bot_auth_token: str, team_name: str = None,
                                  account_id: int = None, user_email: str = None, should_update=True):
    try:
        slack_workspace_configs = get_slack_workspace_config_by(team_id=team_id, bot_user_id=bot_user_id,
                                                                bot_auth_token=bot_auth_token)
        if slack_workspace_configs:
            slack_workspace_config = slack_workspace_configs[0]
            if not should_update:
                return slack_workspace_config, False
            else:
                updated_slack_workspace_config = update_slack_workspace_config(slack_workspace_config.id,
                                                                               team_name,
                                                                               account_id,
                                                                               user_email,
                                                                               True)
                if updated_slack_workspace_config:
                    return updated_slack_workspace_config, True
                else:
                    return None, False

        new_slack_workspace_config = create_data('slack_workspace_config', {
            'team_id': team_id,
            'team_name': team_name,
            'bot_user_id': bot_user_id,
            'bot_auth_token': bot_auth_token,
            'account_id': account_id,
            'user_email': user_email,
            'is_active': True
        })
        if new_slack_workspace_config:
            return new_slack_workspace_config, True
    except Exception as e:
        logger.error(f"Error while saving SlackWorkspaceConfig: {team_id}:{team_name} with error: {e}")
    return None, False


def update_slack_workspace_config(record_id, team_name: str = None, account_id: int = None, user_email: str = None,
                                  is_active: bool = None):
    """
    Update an existing SlackWorkspaceConfig instance in the database.
    """
    try:
        updated_data = {}
        if team_name:
            updated_data['team_name'] = team_name
        if account_id:
            updated_data['account_id'] = account_id
        if user_email:
            updated_data['user_email'] = user_email
        if is_active is not None:
            updated_data['is_active'] = is_active
        updated_row = update_data('slack_workspace_config', record_id, updated_data)
        return updated_row
    except Exception as e:
        logger.error(f"Error while updating SlackWorkspaceConfig with error: {e}")
    return None


def get_slack_bot_configs_by(slack_workspace_id: str = None, channel_id: str = None, is_active: bool = None):
    """
    Fetch a SlackBotConfig row based on different options.
    """
    filters = {}
    if slack_workspace_id:
        filters['slack_workspace_id'] = slack_workspace_id
    if channel_id:
        filters['channel_id'] = channel_id
    if is_active is not None:
        filters['is_active'] = is_active

    return get_data('slack_bot_config', filters)


def create_slack_bot_config(slack_workspace_id, channel_id, event_ts, channel_name=None):
    """
    Create a new SlackBotConfig instance and add it to the database.
    """
    try:
        slack_bot_configs = get_slack_bot_configs_by(slack_workspace_id, channel_id)
        if slack_bot_configs:
            slack_bot_config = slack_bot_configs[0]
            if slack_bot_config and not slack_bot_config.is_active:
                updated_slack_bot_config = update_slack_bot_config(slack_bot_config.id, is_active=True)
                if updated_slack_bot_config:
                    return updated_slack_bot_config, True
                else:
                    return None, False
            return slack_bot_config, False

        new_slack_bot_config = create_data('slack_bot_config', {
            'slack_workspace_id': slack_workspace_id,
            'channel_id': channel_id,
            'channel_name': channel_name,
            'event_ts': event_ts,
            'is_active': True
        })

        if new_slack_bot_config:
            return new_slack_bot_config, True
    except Exception as e:
        logger.error(f"Error while saving SlackBotConfig: {slack_workspace_id}:{channel_id} with error: {e}")
    return None, False


def update_slack_bot_config(record_id, event_ts: str = None, channel_name: str = None, is_active: bool = None):
    """
    Update an existing SlackBotConfig instance in the database.
    """
    try:
        updated_data = {}
        if channel_name:
            updated_data['channel_name'] = channel_name
        if event_ts:
            updated_data['event_ts'] = event_ts
        if is_active is not None:
            updated_data['is_active'] = is_active
        updated_row = update_data('slack_bot_config', record_id, updated_data)
        return updated_row
    except Exception as e:
        logger.error(f"Error while updating SlackBotConfig with error: {e}")
    return None


def create_slack_channel_scrap_schedule(slack_channel_id, data_extraction_from, data_extraction_to):
    """
    Create a new SlackChannelDataScrapSchedule instance and add it to the database.
    """
    try:
        new_slack_data_scrap_schedule = create_data('slack_channel_data_scraping_schedule', {
            'slack_channel_id': slack_channel_id,
            'data_extraction_from': data_extraction_from,
            'data_extraction_to': data_extraction_to,
        })
        if new_slack_data_scrap_schedule:
            return new_slack_data_scrap_schedule, True
    except Exception as e:
        logger.error(
            f"Error while saving SlackChannelDataScrapSchedule: "
            f":{slack_channel_id}:{data_extraction_from}:{data_extraction_to} with error: {e}")
    return None, False


def get_last_slack_channel_scrap_schedule_for(slack_channel_id):
    """
    Fetch a SlackBotConfig row based slack_workspace_id and slack_channel_id.
    """
    filters = {}
    if slack_channel_id:
        filters['slack_channel_id'] = slack_channel_id
    slack_channel_configs = get_data('slack_channel_data_scraping_schedule', filters, 'triggered_at DESC', 1)
    return slack_channel_configs[0] if slack_channel_configs else None


def get_source_token_config_by(user_email: str = None, source: str = None, token_config_md5: str = None,
                               is_active: bool = None):
    """
    Fetch a SourceTokenRepository row based on different options.
    """
    filters = {}
    if user_email:
        filters['user_email'] = user_email
    if source:
        filters['source'] = source
    if token_config_md5:
        filters['token_config_md5'] = token_config_md5
    if is_active is not None:
        filters['is_active'] = is_active

    source_token_configs = get_data('source_token_repository', filters)
    return source_token_configs


def update_source_token_config(record_id, is_active: bool = None):
    """
    Update an existing SourceTokenRepository instance in the database.
    """
    try:
        updated_data = {}
        if is_active is not None:
            updated_data['is_active'] = is_active
        updated_row = update_data('source_token_repository', record_id, updated_data)
        return updated_row
    except Exception as e:
        logger.error(f"Error while updating SourceTokenRepository: {record_id} with error: {e}")
    return None


def create_source_token_config(user_email, source, token_config):
    """
        Create a new SourceTokenRepository instance and add it to the database.
    """
    try:
        token_config_md5 = hashlib.md5(json.dumps(token_config).encode('utf-8')).hexdigest()
        source_token_configs = get_source_token_config_by(user_email=user_email, source=source,
                                                          token_config_md5=token_config_md5)
        if source_token_configs:
            source_token_config = source_token_configs[0]
            if source_token_config and not source_token_config.is_active:
                updated_source_token_config = update_source_token_config(source_token_config.id, is_active=True)
                if updated_source_token_config:
                    return updated_source_token_config, True
                else:
                    return None, False
            else:
                return source_token_config, False

        new_source_token = create_data('source_token_repository', {
            'user_email': user_email,
            'source': source,
            'token_config': token_config,
            'token_config_md5': token_config_md5,
            'is_active': True
        })
        if new_source_token:
            return new_source_token, True
    except Exception as e:
        logger.error(f"Error while saving Source Token: :{user_email}:{source} with error: {e}")
    return None, False
