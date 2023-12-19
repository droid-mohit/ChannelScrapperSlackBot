from flask import jsonify

from processors.datadog_webclient_processor import DatadogRestApiProcessor


def get_monitors(dd_api_key, dd_app_key, dd_monitor_id=None):
    datadog_webclient_processor = DatadogRestApiProcessor(dd_api_key, dd_app_key)
    monitors = datadog_webclient_processor.fetch_monitors(dd_monitor_id)
    if monitors:
        return jsonify(monitors)
    return jsonify({'success': False})


def get_alert_events(dd_api_key, dd_app_key, start_time_epoch, end_time_epoch, params=None):
    if not params:
        params = {}
    params['sources'] = 'alert'
    datadog_webclient_processor = DatadogRestApiProcessor(dd_api_key, dd_app_key)
    events = datadog_webclient_processor.fetch_events(start_time_epoch, end_time_epoch, params)
    if events:
        return jsonify(events)
    return jsonify({'success': False})


def get_metric_timeseries(dd_api_key, dd_app_key, start_time_epoch, end_time_epoch, query_string):
    return
