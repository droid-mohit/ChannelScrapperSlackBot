from celery_app import celery
from processors.slack_api import SlackApiProcessor
from utils.time_utils import get_current_time


@celery.task
def data_fetch_job(bot_auth_token: str, channel_id: str, latest_timestamp=None, oldest_timestamp=None, ):
    current_time = str(get_current_time())
    if not latest_timestamp:
        latest_timestamp = current_time
    print(f"Initiating Data Fetch Job for channel_id: {channel_id} at epoch: {current_time}")
    slack_api_processor = SlackApiProcessor(bot_auth_token)
    slack_api_processor.fetch_conversation_history(channel_id, latest_timestamp, oldest_timestamp)
