from celery_app import celery, app


@celery.task
def periodic_data_fetch_job():
    with app.app_context():
        from persistance.db_utils import get_slack_bot_configs_by, get_last_slack_channel_scrap_schedule_for, \
            create_slack_channel_scrap_schedule
        import time
        from datetime import datetime
        slack_bot_configs = get_slack_bot_configs_by(is_active=True)
        if not slack_bot_configs:
            print(f"No active slack bot configs found")
            return
        current_time = str(time.time())
        for slack_bot_config in slack_bot_configs:
            latest_timestamp = current_time
            oldest_timestamp = None
            slack_channel_config_id = slack_bot_config.id
            channel_id = slack_bot_config.channel_id
            latest_schedule = get_last_slack_channel_scrap_schedule_for(slack_channel_config_id)
            if latest_schedule:
                oldest_timestamp = str(latest_schedule.data_extraction_to.timestamp())
            bot_auth_token = slack_bot_config.slack_workspace.bot_auth_token
            print(f"Scheduling Data Fetch Job for channel_id: {channel_id} at epoch: {current_time}")
            data_fetch_job.delay(bot_auth_token, channel_id, latest_timestamp, oldest_timestamp)
            data_extraction_to = datetime.fromtimestamp(float(latest_timestamp))
            data_extraction_from = None
            if oldest_timestamp:
                data_extraction_from = datetime.fromtimestamp(float(oldest_timestamp))
            create_slack_channel_scrap_schedule(slack_channel_config_id, data_extraction_from, data_extraction_to)


@celery.task
def data_fetch_job(bot_auth_token: str, channel_id: str, latest_timestamp=None, oldest_timestamp=None):
    from processors.slack_webclient_apis import SlackApiProcessor
    import time
    current_time = str(time.time())
    if not latest_timestamp:
        latest_timestamp = current_time
    print(f"Initiating Data Fetch Job for channel_id: {channel_id} at epoch: {current_time}")
    slack_api_processor = SlackApiProcessor(bot_auth_token)
    slack_api_processor.fetch_conversation_history(channel_id, latest_timestamp, oldest_timestamp)
