import re
import pandas as pd
from phase_2_report_processor import title_identifier, text_identifier, extract_tags_grafana, extract_tags_cloudwatch, extract_tags_newrelic

def channel_extractor(s):
    match_var = re.search(r'-([A-Z0-9]+)-', s)
    extracted_sequence = match_var.group(1) if match_var else None
    return extracted_sequence


def timestamp_identifier(full_message_cell):
    timestamp = pd.to_datetime(eval(str(full_message_cell)).get('ts'), unit='s')
    return timestamp


def source_identifier(full_message_cell):
  message = eval(str(full_message_cell))
  sentry_keywords = ['Sentry']
  newrelic_keywords = ['Newrelic', 'New relic']
  honeybadger_keywords = ['Honeybadger']
  datadog_keywords = ['Datadog']
  drdroid_keywords = ['drdroid','doctordroid','dr droid','dr. droid','dr.droid']
  cloudwatch_keywords = ['Cloudwatch', 'Cloud watch','AWS Cloudwatch','marbot','AWS Chatbot','Amazon CloudWatch']
#   container_keywords = ['Prometheus','ComponentOutOfMemory','HostHighCpuLoad','KubeDeploymentReplicasMismatch','KubePodNotReady','HostOomKillDetected','ContainerCPUUsage','HostCpuHighIowait','HostUnusualDiskWriteRate','HostUnusualDiskReadRate','ThanosQueryGrpcClientErrorRate', 'KubePersistentVolumeFillingUp','KubePodCrashLooping', 'BlackboxProbeFailed' ,'BlackboxProbeHttpFailure', 'BlackboxSlowProbe', 'NodeUnschedulable' ,'KubeDaemonSetMisScheduled', 'KubeNodeNotReady', 'KubeNodeUnreachable','TargetDown', 'KubeContainerWaiting' ,'KubeDaemonSetRolloutStuck','KubeHpaMaxedOut', 'KubeControllerManagerDown' ,'KubeNodeReadinessFlapping', 'ElasticsearchNotHealthy', 'CriticalContainerCPUUsage', 'ThanosCompactIsDown', 'AlertmanagerClusterDown', 'ThanosStoreIsDown']
#   apm_keywords = ['latency']
#   infra_keywords_extension = ['Aurora', 'Replica','CPUUtilization','Redshift', 'replication', 'rabbitmq', 'amazonMQ','CacheClusterId', 'DBInstanceIdentifier', 'QueueName', 'DBClusterIdentifier', 'Metric readIOPS', 'Metric writeIOPS','RDS']
  unrelated_bots_keywords = ['giphy','polly']
  grafana_keywords = ['grafana']
  source = ""
  attachments = message.get('attachments')
  files = message.get('files')
  if ('client_msg_id' in message):
    source = "Not an alert"
  elif 'bot_profile' in message:
    source = message['bot_profile'].get('name',"")
    if any(key_word.lower() in str(source).lower() for key_word in unrelated_bots_keywords):
      source = 'Not an alert'
  elif 'subtype' in message:
    if (message.get('subtype')!='bot_message'):
      source = "Not an alert"
    elif 'username' in message:
      source = message.get('username',"")
    if source=='':
      source = 'custom_bot'
  elif attachments:
    for attachment in attachments:
      if 'author_subname' in attachment:
        source = "Not an alert"
        break
  elif files:
    for file in files:
      if 'display_as_bot' in file:
        dis = file.get('display_as_bot')
        if dis==False:
          source = "Not an alert"
  else:
    source = 'custom'
  if (source!='Not an alert'):
    if any(key_word.lower() in str(message).lower() for key_word in cloudwatch_keywords):
      source = 'Cloudwatch'
    elif any(key_word.lower() in str(message).lower() for key_word in honeybadger_keywords):
      source = 'Honeybadger'
    elif any(key_word.lower() in str(message).lower() for key_word in newrelic_keywords):
      source = 'New Relic'
    elif any(key_word.lower() in str(message).lower() for key_word in datadog_keywords):
      source = 'Datadog'
    elif any(key_word.lower() in str(message).lower() for key_word in drdroid_keywords):
      source = 'DrDroid'
    elif any(key_word.lower() in str(message).lower() for key_word in sentry_keywords):
      source = 'Sentry'
    elif any(key_word.lower() in str(message).lower() for key_word in grafana_keywords):
      source = 'Grafana'
    # elif any(key_word.lower() in str(message).lower() for key_word in container_keywords):
    #   source = 'Container'
    # elif any(key_word.lower() in str(message).lower() for key_word in infra_keywords_extension):
    #   source = 'Cloudwatch'
  return source


def phase_1_cleanup(all_alerts):
    all_alerts['timestamp'] = all_alerts['full_message'].apply(lambda x: timestamp_identifier(x))
    all_alerts['source'] = all_alerts['full_message'].apply(lambda x: source_identifier(x))
    all_alerts['alert_type'] = all_alerts['source']
    return all_alerts


def phase_1_filters(df, months=0):
    df = df[df['source'] != 'Not an alert']
    if months > 0:
        time_filter = df['timestamp'].max() - pd.DateOffset(months=months)
        df = df[df['timestamp'] >= time_filter]
    df = df.drop_duplicates(subset=['uuid'], keep='first')
    return df.reset_index(drop=True)


def full_function(all_alerts, workspace_id, channel_id):
    all_alerts['channel_id'] = channel_id
    all_alerts['workspace_id'] = workspace_id
    all_alerts_phase_1 = phase_1_cleanup(all_alerts)
    phase_1_dataset = phase_1_filters(all_alerts_phase_1).groupby(
        [pd.Grouper(key='timestamp', freq='1D'), 'channel_id', 'alert_type']).size().reset_index().rename(
        columns={0: 'count'})

    all_alerts_phase_1['title'] = all_alerts_phase_1['full_message'].apply(lambda x: title_identifier(eval(str(x))))
    all_alerts_phase_1['text'] = all_alerts_phase_1['full_message'].apply(lambda x: text_identifier(eval(str(x))))
    all_alerts_phase_1 = extract_tags_grafana(all_alerts_phase_1)
    all_alerts_phase_1 = extract_tags_newrelic(all_alerts_phase_1)
    all_alerts_phase_1 = extract_tags_cloudwatch(all_alerts_phase_1)
    phase_2_dataset = all_alerts_phase_1
    return phase_1_dataset
