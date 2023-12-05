from datetime import datetime

from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()


class SlackWorkspaceConfig(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    team_id = db.Column(db.String(255), nullable=False, unique=True)
    team_name = db.Column(db.String(255), nullable=False)
    bot_auth_token = db.Column(db.String(255), nullable=False)

    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=True)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=True)

    def to_dict(self):
        return {'id': self.id, 'name': self.name, 'team_id': self.team_id, 'bot_auth_token': self.bot_auth_token,
                'created_at': str(self.created_at)}

    def __repr__(self):
        return f'<Slack Workspace {self.team_id}>'


class SlackBotConfig(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    slack_workspace_id = db.Column(db.Integer, db.ForeignKey('slack_workspace_config.id'), nullable=False)
    slack_workspace = db.relationship('SlackWorkspaceConfig', backref='bot_configs')
    channel_id = db.Column(db.String(255), nullable=False)
    event_ts = db.Column(db.String(255), nullable=False)

    channel_name = db.Column(db.String(255), nullable=True)
    user_id = db.Column(db.String(255), nullable=True)

    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=True)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=True)

    def to_dict(self):
        if self.channel_name:
            return {'id': self.id, 'slack_workspace_id': self.slack_workspace_id, 'channel_id': self.channel_id,
                    'channel_name': self.channel_name, 'user_id': self.user_id, 'event_ts': self.event_ts,
                    'created_at': str(self.created_at)}
        return {'id': self.id, 'slack_workspace_id': self.slack_workspace_id, 'channel_id': self.channel_id,
                'user_id': self.user_id, 'event_ts': self.event_ts, 'created_at': str(self.created_at)}

    def __repr__(self):
        return f'<Slack Bot {self.workspace}:{self.channel_id}>'
