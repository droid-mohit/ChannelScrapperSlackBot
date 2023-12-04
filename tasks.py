import os

import boto3
from celery import Celery
from flask import Flask
from flask_sqlalchemy import SQLAlchemy

from env_vars import AWS_ACCESS_KEY, AWS_SECRET_KEY
from processors.slack_api import fetch_conversation_history

basedir = os.path.abspath(os.path.dirname(__file__))

DATABASE_FILE_PATH = os.path.join(basedir, 'database.db')

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///' + os.path.join(basedir, 'database.db')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

# Celery configuration
app.config['CELERY_BROKER_URL'] = 'redis://localhost:6379/0'
app.config['CELERY_RESULT_BACKEND'] = 'redis://localhost:6379/0'

celery = Celery(app.name, broker=app.config['CELERY_BROKER_URL'])
celery.conf.update(app.config)

db = SQLAlchemy(app)
s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY)


@celery.task(name='add')
def data_fetch_job(bot_auth_token: str, channel_id: str):
    print(f"Initiating Data Fetch Job for channel_id: {channel_id}")
    fetch_conversation_history(bot_auth_token, channel_id)
