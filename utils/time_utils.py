import time
from datetime import datetime


def get_current_datetime_utc():
    return datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')


def get_current_datetime():
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def get_current_time():
    return time.time()


def get_current_epoch():
    return int(datetime.now().timestamp())
