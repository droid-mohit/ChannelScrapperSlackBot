import logging

from persistance.models import db, SlackWorkspaceConfig, SlackBotConfig

logger = logging.getLogger(__name__)


def get_slack_workspace_config_by(team_id: str, bot_user_id: str = None, bot_auth_token: str = None,
                                  team_name: str = None, is_active: bool = None):
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
    if is_active is not None:
        filters['is_active'] = is_active
    return SlackWorkspaceConfig.query.filter_by(**filters).all()


def create_slack_workspace_config(team_id: str, bot_user_id: str, bot_auth_token: str, team_name: str = None,
                                  should_update=True):
    try:
        slack_workspace_configs = get_slack_workspace_config_by(team_id=team_id, bot_user_id=bot_user_id,
                                                                bot_auth_token=bot_auth_token)
        if slack_workspace_configs:
            slack_workspace_config = slack_workspace_configs[0]
            if not should_update:
                return slack_workspace_config, False
            else:
                updated_slack_workspace_config = update_slack_workspace_config(slack_workspace_config,
                                                                               team_name,
                                                                               True)
                if updated_slack_workspace_config:
                    return updated_slack_workspace_config, True
                else:
                    return None, False
        new_slack_workspace_config = SlackWorkspaceConfig(team_id=team_id,
                                                          team_name=team_name,
                                                          bot_user_id=bot_user_id,
                                                          bot_auth_token=bot_auth_token)
        db.session.add(new_slack_workspace_config)
        db.session.commit()
        return new_slack_workspace_config, True
    except Exception as e:
        logger.error(f"Error while saving SlackWorkspaceConfig: {team_id}:{team_name} with error: {e}")
        db.session.rollback()
        return None


def update_slack_workspace_config(slack_workspace_config: SlackWorkspaceConfig, team_name: str = None,
                                  is_active: bool = None):
    """
    Update an existing SlackWorkspaceConfig instance in the database.
    """
    try:
        if not slack_workspace_config:
            logger.error(f"Error while updating SlackWorkspaceConfig: with error: slack_workspace_config is None")
            return None
        if team_name:
            slack_workspace_config.team_name = team_name
        if is_active is not None:
            slack_workspace_config.is_active = is_active
        db.session.commit()
        return slack_workspace_config
    except Exception as e:
        logger.error(f"Error while updating SlackWorkspaceConfig {slack_workspace_config.team_id} with error: {e}")
        db.session.rollback()
        return None


def get_slack_bot_configs_by(slack_workspace_id: str = None, channel_id: str = None, is_active: bool = None):
    """
    Fetch a SlackBotConfig row based on the bot_config_id.
    """
    filters = {}
    if slack_workspace_id:
        filters['slack_workspace_id'] = slack_workspace_id
    if channel_id:
        filters['channel_id'] = channel_id
    if is_active is not None:
        filters['is_active'] = is_active

    return SlackBotConfig.query.filter_by(**filters).all()


def create_slack_bot_config(slack_workspace_id, channel_id, event_ts, channel_name=None):
    """
    Create a new SlackBotConfig instance and add it to the database.
    """
    try:
        slack_bot_configs = get_slack_bot_configs_by(slack_workspace_id, channel_id)
        if slack_bot_configs:
            slack_bot_config = slack_bot_configs[0]
            if slack_bot_config and not slack_bot_config.is_active:
                updated_slack_bot_config = update_slack_bot_config(slack_bot_config, is_active=True)
                if updated_slack_bot_config:
                    return updated_slack_bot_config, True
                else:
                    return None, False
            return slack_bot_config, False

        new_slack_bot_config = SlackBotConfig(
            slack_workspace_id=slack_workspace_id,
            channel_id=channel_id,
            channel_name=channel_name,
            event_ts=event_ts,
            is_active=True
        )

        db.session.add(new_slack_bot_config)
        db.session.commit()
        return new_slack_bot_config, True
    except Exception as e:
        logger.error(f"Error while saving SlackBotConfig: {slack_workspace_id}:{channel_id} with error: {e}")
        db.session.rollback()
        return None, False


def update_slack_bot_config(slack_bot_config: SlackBotConfig, event_ts: str = None, channel_name: str = None,
                            is_active: bool = None):
    """
    Update an existing SlackBotConfig instance in the database.
    """
    try:
        if not slack_bot_config:
            logger.error(f"Error while updating SlackBotConfig: with error: slack_bot_config is None")
            return None
        if channel_name:
            slack_bot_config.channel_name = channel_name
        if event_ts:
            slack_bot_config.event_ts = event_ts
        if is_active is not None:
            slack_bot_config.is_active = is_active
        db.session.commit()
        return slack_bot_config
    except Exception as e:
        logger.error(f"Error while updating SlackBotConfig: {slack_bot_config.id} with error: {e}")
        db.session.rollback()
        return None
