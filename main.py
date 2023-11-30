import json
import os
import time
from datetime import datetime

import boto3 as boto3
import requests
from flask import Flask, request, jsonify, redirect
from flask_sqlalchemy import SQLAlchemy

from env_vars import SLACK_CLIENT_ID, SLACK_REDIRECT_URI, SLACK_CLIENT_SECRET, AWS_SECRET_KEY, AWS_ACCESS_KEY, \
    PUSH_TO_S3, PUSH_TO_LOCAL_DB, PUSH_TO_SLACK, METADATA_S3_BUCKET_NAME
from processors.slack_api import fetch_conversation_history
from utils.publishsing_client import publish_json_blob_to_s3, publish_message_to_slack

basedir = os.path.abspath(os.path.dirname(__file__))

DATABASE_FILE_PATH = os.path.join(basedir, 'database.db')

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///' + os.path.join(basedir, 'database.db')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)
s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY)


class Workspace(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    team_id = db.Column(db.String(255), unique=False, nullable=False)
    name = db.Column(db.String(255), unique=False, nullable=True)
    bot_auth_token = db.Column(db.String(255), unique=True, nullable=False)

    timestamp = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)

    def to_dict(self):
        return {'id': self.id, 'name': self.name, 'team_id': self.team_id, 'bot_auth_token': self.bot_auth_token,
                'timestamp': str(self.timestamp)}

    def __repr__(self):
        return f'<Workspace {self.name}>'


class SlackBotConfig(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    workspace = db.Column(db.Integer, db.ForeignKey('workspace.id'), nullable=False)
    channel_id = db.Column(db.String(255), unique=True, nullable=False)
    user_id = db.Column(db.String(255), unique=False, nullable=True)
    event_ts = db.Column(db.String(255), unique=False, nullable=True)

    timestamp = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)

    def to_dict(self):
        return {'id': self.id, 'workspace_id': self.workspace_id, 'channel_id': self.channel_id,
                'user_id': self.user_id, 'event_ts': self.event_ts, 'timestamp': str(self.timestamp)}

    def __repr__(self):
        return f'<SlackBotConfig {self.workspace}:{self.channel_id}>'


def create_tables():
    if not os.path.exists(DATABASE_FILE_PATH):
        with app.app_context():
            db.create_all()
            print("Tables created successfully.")


@app.route('/install', methods=['GET'])
def install():
    # Redirect users to Slack's OAuth URL
    return redirect(
        f'https://slack.com/oauth/v2/authorize?client_id={SLACK_CLIENT_ID}&scope=commands,app_mentions:read,channels:history,channels:read,chat:write,groups:read,mpim:read,users:read&user_scope=channels:history&redirect_uri={SLACK_REDIRECT_URI}')


@app.route('/oauth_redirect', methods=['GET'])
def oauth_redirect():
    # Extract the authorization code from the request
    code = request.args.get('code')

    # Exchange the authorization code for an OAuth token
    response = requests.post('https://slack.com/api/oauth.v2.access', {
        'client_id': SLACK_CLIENT_ID,
        'client_secret': SLACK_CLIENT_SECRET,
        'code': code,
        'redirect_uri': SLACK_REDIRECT_URI
    })

    # Parse the response
    data = response.json()

    # Extract the OAuth token
    if 'ok' in data and data['ok'] and 'token_type' in data and data['token_type'] == 'bot' and 'team' in data:
        bot_oauth_token = data['access_token']
        team_id = data['team']['id']
        team_name = data['team']['name']
        print(f"Received Bot OAuth token {bot_oauth_token} for workspace {team_id} : ({team_name})")

        if PUSH_TO_LOCAL_DB:
            workspace = Workspace.query.filter_by(team_id=team_id).first()
            if not workspace:
                try:
                    new_workspace = Workspace(team_id=team_id, name=team_name, bot_auth_token=bot_oauth_token)
                    db.session.add(new_workspace)
                    db.session.commit()
                except Exception as e:
                    print(f"Error while saving Workspace: {team_name} with error: {e}")
                    db.session.rollback()
        if PUSH_TO_S3:
            current_time = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
            data_to_upload = {'name': team_name, 'team_id': team_id, 'bot_auth_token': bot_oauth_token,
                              'timestamp': str(current_time)}
            json_data = json.dumps(data_to_upload)
            key = f'{team_id}-{team_name}-{current_time}.json'
            publish_json_blob_to_s3(key, METADATA_S3_BUCKET_NAME, json_data)
        if PUSH_TO_SLACK:
            message_text = f"Registered workspace_id : {team_id}, workspace_name: {team_name} " \
                           f"with bot_auth_token: {bot_oauth_token}"
            publish_message_to_slack(message_text)

        return jsonify({'success': True, 'message': 'Alert Summary Bot Installation successful'})
    else:
        print(data)
        return jsonify({'error': 'Alert Summary Bot Installation failed. Check permissions with workspace owner/admin'})


@app.route('/slack/events', methods=['POST'])
def bot_mention():
    # Extract the authorization code from the request
    request_data = request.data.decode('utf-8')
    if request_data:
        data = json.loads(request_data)
        if data['type'] == 'url_verification':
            return jsonify({'challenge': data['challenge']})
        elif data['type'] == 'event_callback':
            team_id = data['team_id']
            event = data['event']
            if event:
                channel_id = None
                user = None
                event_ts = None
                if event['type'] == 'app_mention':
                    channel_id = event['channel']
                    user = event['user']
                    event_ts = event['event_ts']
                elif data['type'] == 'member_joined_channel':
                    channel_id = data['channel']
                    user = data['user']
                    event_ts = data['event_ts']
                if channel_id and user and event_ts:
                    print(
                        f"Received bot mention in workspace {team_id} channel {channel_id}: by user {user} at ({event_ts})")
                    if PUSH_TO_LOCAL_DB:
                        try:
                            workspace = Workspace.query.filter_by(team_id=team_id).first()
                            if workspace:
                                slack_bot_config = SlackBotConfig.query.filter_by(workspace=workspace.id,
                                                                                  channel_id=channel_id).first()
                                if not slack_bot_config:
                                    try:
                                        new_slack_bot_config = SlackBotConfig(workspace=workspace.id,
                                                                              channel_id=channel_id,
                                                                              user_id=user, event_ts=event_ts)
                                        db.session.add(new_slack_bot_config)
                                        db.session.commit()
                                    except Exception as e:
                                        print(
                                            f"Error while saving SlackBotConfig: {team_id}:{channel_id} with error: {e}")
                                        db.session.rollback()
                        except Exception as e:
                            print(f"Error while saving SlackBotConfig: {team_id}:{channel_id} with error: {e}")

                    if PUSH_TO_S3:
                        current_time = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
                        data_to_upload = {'workspace_id': team_id, 'channel_id': channel_id, 'user_id': user,
                                          'event_ts': event_ts, 'timestamp': str(current_time)}
                        json_data = json.dumps(data_to_upload)
                        key = f'{team_id}-{channel_id}-{current_time}.json'
                        publish_json_blob_to_s3(key, METADATA_S3_BUCKET_NAME, json_data)
                    if PUSH_TO_SLACK:
                        message_text = "Registered channel for workspace_id : " + team_id + " " + "channel_id: " + \
                                       channel_id + " " + "user_id: " + user + " " + "event_ts: " + event_ts
                        publish_message_to_slack(message_text)
                    return jsonify({'success': True})

    return jsonify({'success': True})


@app.route('/start_data_fetch', methods=['GET'])
def start_data_fetch():
    channel_id = request.args.get('channel')
    bot_auth_token = request.args.get('token')
    print(f"Initiating Data Fetch for channel_id: {channel_id}")
    oldest_timestamp = time.time() - 172800
    fetch_conversation_history(bot_auth_token, channel_id, oldest_timestamp=str(oldest_timestamp))
    return jsonify({'success': True})


@app.route('/health_check', methods=['POST'])
def hello():
    print('Slack Alert App Backend is Up and Running!')
    return jsonify({'success': True})


create_tables()
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001, debug=False)
