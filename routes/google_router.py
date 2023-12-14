import json
import os
import logging
import pandas as pd

import flask
from flask import Blueprint, request

import google.oauth2.credentials
import google_auth_oauthlib.flow
import googleapiclient.discovery

from env_vars import GOOGLE_OAUTH_REDIRECT_URI, GOOGLE_CLIENT_SECRETS_FILE, PUSH_TO_SLACK, PUSH_TO_S3, \
    RAW_DATA_S3_BUCKET_NAME
from utils.publishsing_client import publish_message_to_slack, publish_object_file_to_s3
from utils.time_utils import get_current_datetime

google_blueprint = Blueprint('google_router', __name__)

logger = logging.getLogger(__name__)

# This variable specifies the name of a file that contains the OAuth 2.0
# information for this application, including its client_id and client_secret.

# This OAuth 2.0 access scope allows for full read/write access to the
# authenticated user's account and requires requests to use an SSL connection.
SCOPES = ['https://www.googleapis.com/auth/chat.messages.readonly',
          'https://www.googleapis.com/auth/chat.spaces.readonly']
API_SERVICE_NAME = 'chat'
API_VERSION = 'v1'

secrets_file_path = os.path.join(os.getcwd() + '/secrets', GOOGLE_CLIENT_SECRETS_FILE)


def credentials_to_dict(credentials):
    return {'token': credentials.token,
            'refresh_token': credentials.refresh_token,
            'token_uri': credentials.token_uri,
            'client_id': credentials.client_id,
            'client_secret': credentials.client_secret,
            'scopes': credentials.scopes}


@google_blueprint.route('/get_chats')
def get_chats_request():
    space_name = request.args.get('space_name')
    credentials_str = request.args.get('credentials')
    if not space_name:
        return 'Missing space_name', 400
    all_messages = []
    cred_dict = json.loads(credentials_str)

    # Load credentials from the session.
    credentials = google.oauth2.credentials.Credentials(**cred_dict)

    chat_service = googleapiclient.discovery.build(API_SERVICE_NAME, API_VERSION, credentials=credentials)

    should_continue = True
    page_token = ''
    while should_continue:
        response = chat_service.spaces().messages().list(parent=space_name, pageSize=1000,
                                                         pageToken=page_token).execute()
        all_messages.extend(response['messages'])
        if 'nextPageToken' in response and response['nextPageToken']:
            page_token = response['nextPageToken']
        else:
            should_continue = False
            break

    df = pd.DataFrame(all_messages)
    current_time = get_current_datetime()
    csv_file_name = f"{space_name.split('/')[1]}-{current_time}-raw_data.csv"
    downloads_dir = os.path.join(os.getcwd(), 'downloads')
    downloads_data_dir = os.path.join(downloads_dir, 'data')
    csv_file_path = os.path.join(downloads_data_dir, csv_file_name)
    df.to_csv(csv_file_path, index=False)
    if PUSH_TO_S3:
        publish_object_file_to_s3(csv_file_path, RAW_DATA_S3_BUCKET_NAME, csv_file_name)
    # Save credentials back to session in case access token was refreshed.
    # ACTION ITEM: In a production app, you likely want to save these
    #              credentials in a persistent database instead.
    # flask.session['credentials'] = credentials_to_dict(credentials)

    return {}


@google_blueprint.route('/get_spaces')
def get_spaces_request():
    if 'credentials' not in flask.session:
        return flask.redirect('/google/authorize')

    # Load credentials from the session.
    credentials = google.oauth2.credentials.Credentials(
        **flask.session['credentials'])

    chat_service = googleapiclient.discovery.build(
        API_SERVICE_NAME, API_VERSION, credentials=credentials)

    response = chat_service.spaces().list().execute()

    # Save credentials back to session in case access token was refreshed.
    # ACTION ITEM: In a production app, you likely want to save these
    #              credentials in a persistent database instead.
    flask.session['credentials'] = credentials_to_dict(credentials)
    print(response)
    return flask.jsonify(**response)


@google_blueprint.route('/authorize')
def authorize():
    # Create flow instance to manage the OAuth 2.0 Authorization Grant Flow steps.
    flow = google_auth_oauthlib.flow.Flow.from_client_secrets_file(secrets_file_path, scopes=SCOPES)

    # The URI created here must exactly match one of the authorized redirect URIs
    # for the OAuth 2.0 client, which you configured in the API Console. If this
    # value doesn't match an authorized URI, you will get a 'redirect_uri_mismatch'
    # error.
    flow.redirect_uri = GOOGLE_OAUTH_REDIRECT_URI

    authorization_url, state = flow.authorization_url(
        # Enable offline access so that you can refresh an access token without
        # re-prompting the user for permission. Recommended for web server apps.
        access_type='offline',
        # Enable incremental authorization. Recommended as a best practice.
        include_granted_scopes='true')

    # Store the state so the callback can verify the auth server response.
    flask.session['state'] = state

    return flask.redirect(authorization_url)


@google_blueprint.route('/oauth2callback')
def oauth2callback():
    # Specify the state when creating the flow in the callback so that it can
    # verified in the authorization server response.
    state = flask.session['state']

    flow = google_auth_oauthlib.flow.Flow.from_client_secrets_file(secrets_file_path, scopes=SCOPES, state=state)
    flow.redirect_uri = GOOGLE_OAUTH_REDIRECT_URI

    # Use the authorization server's response to fetch the OAuth 2.0 tokens.
    if not flask.request.url.startswith("https"):
        authorization_response = flask.request.url.replace('http', 'https')
    else:
        authorization_response = flask.request.url
    flow.fetch_token(authorization_response=authorization_response)

    # Store credentials in the session.
    # ACTION ITEM: In a production app, you likely want to save these
    #              credentials in a persistent database instead.
    credentials = flow.credentials
    credentials_dict = credentials_to_dict(credentials)
    credentials_json_str = json.dumps(credentials_dict)
    if PUSH_TO_SLACK:
        message_text = "Registered g-chat workspace: " + "*" + credentials_json_str + "*"
        publish_message_to_slack(message_text)
    print('credentials_dict', credentials_dict)
    df = pd.DataFrame([credentials_dict])
    current_time = get_current_datetime()
    csv_file_name = f"{current_time}-credentials.csv"
    downloads_dir = os.path.join(os.getcwd(), 'downloads')
    downloads_credentials_dir = os.path.join(downloads_dir, 'credentials')
    csv_file_path = os.path.join(downloads_credentials_dir, csv_file_name)
    df.to_csv(csv_file_path, index=False)
    flask.session['credentials'] = credentials_dict

    return flask.redirect(flask.url_for('google_router.get_spaces_request'))
