import sys

from celery import Celery
from flask import Flask

from env_vars import PG_DB_USERNAME, PG_DB_PASSWORD, PG_DB_NAME, PG_DB_HOSTNAME
from persistance.models import db
from pathlib import Path

sys.path.append(str(Path(__file__).parent.absolute()))

app = Flask(__name__)

# Configure postgres db
app.config['SQLALCHEMY_DATABASE_URI'] = f'postgresql://{PG_DB_USERNAME}:{PG_DB_PASSWORD}@{PG_DB_HOSTNAME}/{PG_DB_NAME}'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db.init_app(app)

# Celery configuration
app.config['CELERY_BROKER_URL'] = 'redis://localhost:6379/0'
celery = Celery(
    app.name,  # Replace with your Flask app name
    broker=app.config['CELERY_BROKER_URL'],  # Use Redis as the message broker
    include=['jobs.tasks']
)
celery.conf.update(app.config)
