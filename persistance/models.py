from datetime import datetime

from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()


class SlackWorkspaceConfig(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    team_id = db.Column(db.String(255), nullable=False)
    team_name = db.Column(db.String(255), nullable=False)
    bot_user_id = db.Column(db.String(255), nullable=False)
    bot_auth_token = db.Column(db.String(255), nullable=False)
    is_active = db.Column(db.Boolean, default=True)

    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=True)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=True)

    __table_args__ = (db.UniqueConstraint('team_id', 'bot_user_id', 'bot_auth_token'),)

    def to_dict(self):
        return {'id': self.id, 'name': self.team_name, 'team_id': self.team_id, 'bot_user_id': self.bot_user_id,
                'bot_auth_token': self.bot_auth_token, 'created_at': str(self.created_at)}

    def __repr__(self):
        if self.team_name:
            return f'<Slack Workspace {self.team_id}:{self.team_name}:{self.bot_user_id}>'
        return f'<Slack Workspace {self.team_id}:{self.bot_user_id}>'


class SlackBotConfig(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    slack_workspace_id = db.Column(db.Integer, db.ForeignKey('slack_workspace_config.id'), nullable=False)
    slack_workspace = db.relationship('SlackWorkspaceConfig', backref='bot_configs')
    channel_id = db.Column(db.String(255), nullable=False)
    event_ts = db.Column(db.String(255), nullable=False)
    is_active = db.Column(db.Boolean, default=True)

    channel_name = db.Column(db.String(255), nullable=True)

    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=True)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=True)

    __table_args__ = (db.UniqueConstraint('slack_workspace_id', 'channel_id'),)

    def to_dict(self):
        if self.channel_name:
            return {'id': self.id, 'slack_workspace_id': self.slack_workspace_id, 'channel_id': self.channel_id,
                    'channel_name': self.channel_name, 'event_ts': self.event_ts, 'created_at': str(self.created_at)}
        return {'id': self.id, 'slack_workspace_id': self.slack_workspace_id, 'channel_id': self.channel_id,
                'event_ts': self.event_ts, 'created_at': str(self.created_at)}

    def __repr__(self):
        if self.channel_name:
            return f'<Slack Bot {self.slack_workspace.team_id}:{self.channel_name}>'
        return f'<Slack Bot {self.workspace}:{self.channel_id}>'


class SlackChannelDataScrapingSchedule(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    slack_channel_id = db.Column(db.Integer, db.ForeignKey('slack_bot_config.id'), nullable=False)
    slack_channel = db.relationship('SlackBotConfig', backref='channel_configs')

    data_extraction_from = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    data_extraction_to = db.Column(db.DateTime, default=datetime.utcnow)

    triggered_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=True)

    __table_args__ = (
        db.UniqueConstraint('slack_channel_id', 'data_extraction_from', 'data_extraction_to'),)

    def __repr__(self):
        return f'<Schedule: {self.slack_channel_id}:{self.data_extraction_from}:{self.data_extraction_to}>'
