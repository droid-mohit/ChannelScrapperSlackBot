# ChannelScrapperSlackBot
Slack bot server to scrap channel messages and dump in different destinations for analysis

It is a Flask + Celery Project
Run Flask service for receiving webhooks from alert source
python main.py

Run Celery worker for processing data fetch requests from slack channels 
celery -A tasks.celery worker -l INFO