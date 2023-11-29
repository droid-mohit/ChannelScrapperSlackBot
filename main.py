import json

import requests
from flask import Flask, request, jsonify, redirect

from env_vars import SLACK_CLIENT_ID, SLACK_REDIRECT_URI, SLACK_CLIENT_SECRET

app = Flask(__name__)


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
        return jsonify({'success': True, 'message': 'Alert Summary Bot Installation successful'})
    else:
        return jsonify({'error': 'Alert Summary Bot Installation failed'})


@app.route('/bot_mention', methods=['POST'])
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
                if event['type'] == 'app_mention':
                    channel_id = event['channel']
                    user = event['user']
                    ts = event['ts']
                    event_ts = event['event_ts']
                    print(
                        f"Alert Summary Bot added in workspace {team_id} channel {channel_id}: by user {user} at {ts} ({event_ts})")
                elif data['type'] == 'member_joined_channel':
                    channel_id = data['channel']
                    user = data['user']
                    event_ts = data['event_ts']
                    print(
                        f"Received bot mention in workspace {team_id} channel {channel_id}: by user {user} at ({event_ts})")
    return jsonify({'success': True})


@app.route('/slack/events', methods=['POST'])
def slack_events():
    # Verify the request comes from Slack
    if request.headers.get('X-Slack-Signature') != request.headers.get('X-Slack-Signature'):
        return jsonify({'error': 'Invalid request'}), 400

    # Parse the request payload
    payload = request.get_data(as_text=True)
    data = request.json

    # Verify the challenge parameter for URL verification
    if 'challenge' in data:
        return jsonify({'challenge': data['challenge']})

    # Handle other events, e.g., message events
    if 'event' in data:
        event = data['event']

        # Check if it's a message event
        if event['type'] == 'message' and 'text' in event:
            channel_id = event['channel']
            message_text = event['text']

            # Process the message as needed (you can send it to another server, etc.)
            print(f"Received message in channel {channel_id}: {message_text}")

    return jsonify({'success': True})


@app.route('/hello', methods=['GET'])
def hello():
    print('Hello world!')
    return jsonify({'success': True})


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001, debug=True)
