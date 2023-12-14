# Flask APP Configurations
FLAKS_APP_SECRET_KEY = 'secret'

# Replace these with your app's information
SLACK_APP_ID = 'your_app_id'
SLACK_CLIENT_ID = 'your_client_id'
SLACK_CLIENT_SECRET = 'your_client_secret'
SLACK_REDIRECT_URI = 'your_redirect_uri'

# AWS S3 Credentials
PUSH_TO_S3 = True
AWS_ACCESS_KEY = 'your_aws_access_key'
AWS_SECRET_KEY = 'your_aws_secret_key'
METADATA_S3_BUCKET_NAME = 'your_metadata_s3_bucket_name'
RAW_DATA_S3_BUCKET_NAME = 'your_raw_data_s3_bucket_name'

PUSH_TO_SLACK = True
SLACK_URL = 'your_slack_webhook_url'

# Postgres DB Credentials
PG_DB_HOSTNAME = 'localhost'
PG_DB_USERNAME = ''
PG_DB_PASSWORD = ''
PG_DB_NAME = 'data_scrapper_db'

# G-chat App Configurations
GOOGLE_OAUTH_REDIRECT_URI = 'your_redirect_uri'
GOOGLE_CLIENT_SECRETS_FILE = "google_chat_app_secret.json"
