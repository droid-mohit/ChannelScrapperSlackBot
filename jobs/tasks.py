from celery_app import celery
from processors.slack_api import SlackApiProcessor


@celery.task
def data_fetch_job(bot_auth_token: str, channel_id: str):
    print(f"Initiating Data Fetch Job for channel_id: {channel_id}")
    slack_api_processor = SlackApiProcessor(bot_auth_token)
    slack_api_processor.fetch_conversation_history(bot_auth_token, channel_id)
