# DataScrapperBot

Data Scrapper bot server to scrap messages from different sources and dump in different destinations for analysis.

It is a Flask + Celery Project

Flask Migrate Commands:

```
python -m flask db init
python -m flask db migrate
python -m  flask db upgrade
```

Run Flask service:

```
python app.py
```

Run Celery Worker:

```
celery -A celery_app.celery worker --loglevel=info
```

Run Celery Beat:

```
celery -A celery_beat_schedule.app beat --loglevel=info
```

Current Supported Sources:

1. Slack Channels
2. Google Chat Spaces