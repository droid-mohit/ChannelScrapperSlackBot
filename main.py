import json
import os

import requests
from flask import Flask, request, jsonify, redirect
from flask_sqlalchemy import SQLAlchemy

from env_vars import SLACK_CLIENT_ID, SLACK_REDIRECT_URI, SLACK_CLIENT_SECRET

basedir = os.path.abspath(os.path.dirname(__file__))

DATABASE_FILE_PATH = os.path.join(basedir, 'database.db')

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///' + os.path.join(basedir, 'database.db')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)


class Workspace(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    team_id = db.Column(db.String(255), unique=True, nullable=False)
    name = db.Column(db.String(255), unique=True, nullable=True)
    bot_auth_token = db.Column(db.String(255), unique=True, nullable=False)

    def __repr__(self):
        return f'<Workspace {self.name}>'


class SlackBotConfig(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    workspace = db.Column(db.Integer, db.ForeignKey('workspace.id'), nullable=False)
    channel_id = db.Column(db.String(255), unique=True, nullable=False)
    user_id = db.Column(db.String(255), unique=True, nullable=True)
    event_ts = db.Column(db.String(255), unique=True, nullable=True)

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

        new_workspace = Workspace(team_id=team_id, name=team_name, bot_auth_token=bot_oauth_token)
        db.session.add(new_workspace)
        db.session.commit()

        return jsonify({'success': True, 'message': 'Alert Summary Bot Installation successful'})
    else:
        return jsonify({'error': 'Alert Summary Bot Installation failed'})


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
                    workspace = Workspace.query.filter_by(team_id=team_id).first()
                    if workspace:
                        slack_bot_config = SlackBotConfig.query.filter_by(workspace=workspace.id,
                                                                          channel_id=channel_id).first()
                        if slack_bot_config:
                            slack_bot_config.user_id = user
                            slack_bot_config.event_ts = event_ts
                            db.session.commit()
                            return jsonify({'success': True})
                        else:
                            new_slack_bot_config = SlackBotConfig(workspace=workspace.id, channel_id=channel_id,
                                                                  user_id=user, event_ts=event_ts)
                            db.session.add(new_slack_bot_config)
                            db.session.commit()
                            return jsonify({'success': True})
    return jsonify({'success': True})


@app.route('/health_check', methods=['GET'])
def hello():
    print('Slack Alert App Backend is Up and Running!')
    return jsonify({'success': True})


create_tables()
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001, debug=False)
