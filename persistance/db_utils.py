import logging

from persistance.models import db, SlackWorkspaceConfig, SlackBotConfig

logger = logging.getLogger(__name__)


def get_slack_workspace_config_by(team_id: str, bot_auth_token: str = None, team_name: str = None):
    """
    Fetch a SlackWorkspaceConfig row based on different options.
    """
    filters = {}
    if team_id:
        filters['team_id'] = team_id
    if team_name:
        filters['team_name'] = team_name
    if bot_auth_token:
        filters['bot_auth_token'] = bot_auth_token

    slack_workspace_config = SlackWorkspaceConfig.query.filter_by(**filters).first()
    return slack_workspace_config


def create_slack_workspace_config(team_id: str, bot_auth_token: str, team_name: str = None, should_update=True):
    try:
        slack_workspace_config = get_slack_workspace_config_by(team_id=team_id)
        if slack_workspace_config:
            if not should_update:
                return slack_workspace_config, False
            else:
                return update_slack_workspace_config(team_id, bot_auth_token, team_name), True
        new_slack_workspace_config = SlackWorkspaceConfig(team_id=team_id, team_name=team_name,
                                                          bot_auth_token=bot_auth_token)
        db.session.add(new_slack_workspace_config)
        db.session.commit()
        return new_slack_workspace_config, True
    except Exception as e:
        logger.error(f"Error while saving SlackWorkspaceConfig: {team_id}:{team_name} with error: {e}")
        db.session.rollback()
        return None


def update_slack_workspace_config(team_id: str, bot_auth_token: str, team_name: str = None):
    """
    Update an existing SlackWorkspaceConfig instance in the database.
    """
    slack_workspace_config = get_slack_workspace_config_by(team_id, bot_auth_token, team_name)

    if slack_workspace_config:
        slack_workspace_config.team_id = team_id
        slack_workspace_config.team_name = team_name
        slack_workspace_config.bot_auth_token = bot_auth_token

        db.session.commit()
        return slack_workspace_config
    else:
        return None


def get_slack_bot_config_by_id(slack_workspace_id: str, channel_id: str = None):
    """
    Fetch a SlackBotConfig row based on the bot_config_id.
    """
    filters = {}
    if slack_workspace_id:
        filters['slack_workspace_id'] = slack_workspace_id
    if channel_id:
        filters['channel_id'] = channel_id

    slack_bot_config = SlackBotConfig.query.filter_by(**filters).first()

    return slack_bot_config


def create_slack_bot_config(slack_workspace_id, channel_id, event_ts, channel_name=None, user_id=None):
    """
    Create a new SlackBotConfig instance and add it to the database.
    """
    try:
        slack_bot_config = get_slack_bot_config_by_id(slack_workspace_id, channel_id)
        if slack_bot_config:
            return slack_bot_config, False

        new_slack_bot_config = SlackBotConfig(
            slack_workspace_id=slack_workspace_id,
            channel_id=channel_id,
            channel_name=channel_name,
            user_id=user_id,
            event_ts=event_ts
        )

        db.session.add(new_slack_bot_config)
        db.session.commit()
        return new_slack_bot_config, True
    except Exception as e:
        logger.error(f"Error while saving SlackBotConfig: {slack_workspace_id}:{channel_id} with error: {e}")
        db.session.rollback()
        return None, False


def update_bot_config(bot_config_id, channel_id, user_id, event_ts):
    """
    Update an existing SlackBotConfig instance in the database.
    """
    slack_bot_config = SlackBotConfig.query.get(bot_config_id)

    if slack_bot_config:
        slack_bot_config.channel_id = channel_id
        slack_bot_config.user_id = user_id
        slack_bot_config.event_ts = event_ts

        db.session.commit()

        return slack_bot_config
    else:
        return None
