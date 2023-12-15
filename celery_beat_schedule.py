from celery import Celery
from celery.schedules import crontab

app = Celery('beat_schedule', broker='redis://localhost:6379/0')

app.conf.beat_schedule = {
    'fetch-every-1-day': {
        'task': 'jobs.tasks.periodic_data_fetch_job',
        'schedule': crontab(minute='0', hour='0'),
    },
}
