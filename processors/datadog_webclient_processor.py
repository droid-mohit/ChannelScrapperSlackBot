import logging
from typing import Dict

from datadog import api, initialize as dd_api_initialize

logger = logging.getLogger(__name__)


class DatadogRestApiProcessor:

    def __init__(self, dd_api_key, dd_app_key):
        options = {
            'api_key': dd_api_key,
            'app_key': dd_app_key,
        }
        dd_api_initialize(**options)

    def fetch_monitors(self, monitor_id: int = None, params: Dict = dict({})):
        try:
            if monitor_id is not None:
                monitors = [api.Monitor.get(id=monitor_id, **params)]
            else:
                monitors = api.Monitor.get_all(**params)
            if not monitors:
                logger.error(f"No monitors found")
            print("Found total monitors: ", len(monitors))
            return monitors
        except Exception as e:
            logger.error(f"Exception occurred while fetching monitors with error: {e}")
            return None

    def fetch_events(self, start_time_epoch, end_time_epoch, params: Dict = dict({})):
        try:
            response = api.Event.query(start=start_time_epoch, end=end_time_epoch, **params)
            if not response:
                logger.error(f"No events found")
            events = response['events']
            print("Found total events: ", len(events))
            return events
        except Exception as e:
            logger.error(f"Exception occurred while fetching monitors with error: {e}")
            return None

    def fetch_metric(self, start_time_epoch, end_time_epoch, params: Dict = dict({})):
        # api.Metric.query(start=start_time_epoch, end=end_time_epoch, **params)
        return None
