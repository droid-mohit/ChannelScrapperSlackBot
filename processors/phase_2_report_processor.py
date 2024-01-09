import re
import pandas as pd

def title_identifier(message):
    title = ''
    titles = []
    if 'title' in message:
      titles.append(message.get('title'))
    attachments = message.get('attachments')
    if attachments:
      for attachment in attachments:
        if 'title' in attachment:
          titles.append(attachment.get('title'))
    if titles:
      title = max(titles,key=len)
    return title



def text_identifier(message):
  text = ''
  texts = []
  for key in ['text', 'fallback']:
    value = message.get(key)
    if value:
      texts.append(value)
  attachments = message.get('attachments')
  if attachments:
    for attachment in attachments:
      for key in ['text', 'fallback']:
        value = attachment.get(key)
        if value:
          texts.append(value)
  if texts:
    text = max(texts, key=len)
  else:
    text = ''
  return text


def extract_tags_grafana(df):
  def title_update(title):
    if title.startswith("[Alerting]"):
      remaining_title = title[len("[Alerting]"):].strip()
    elif title.startswith("[OK]"):
      remaining_title = title[len("[OK]"):].strip()
    else:
      return title
    title = remaining_title
    return title

  def status_extraction(title):
    if title.startswith("[Alerting]"):
      ids_dict = {"status" : "Alerting"}
    elif title.startswith("[OK]"):
      ids_dict = {"status" : "OK"}
    else:
      ids_dict = {"status" : "Alerting"}
    return str(ids_dict)

  grafana_rows = df['alert_type']=='Grafana'
  df.loc[grafana_rows,'tags'] = df.loc[grafana_rows,'title'].apply(lambda x: status_extraction(x))
  df.loc[grafana_rows,'title'] = df.loc[grafana_rows,'title'].apply(lambda x: title_update(x))
  return df


def extract_tags_newrelic(df):

  def newrelic_extract_ids(full_message):
    # Define regular expressions for each ID type
    condition_id_pattern = r'conditions/(\d+)/edit'
    policy_id_pattern = r'policies/(\d+)'
    incident_id_pattern = r'incidents/(\d+)'
    issue_id_pattern = r'issues/([0-9a-fA-F\-]+)'
    # Function to find the first match of a pattern

    def find_first(pattern, full_message):
        match = re.search(pattern, full_message)
        return match.group(1) if match else None

    # Extract IDs using the defined patterns
    condition_id = find_first(condition_id_pattern, full_message)
    policy_id = find_first(policy_id_pattern, full_message)
    incident_id = find_first(incident_id_pattern, full_message)
    issue_id = find_first(issue_id_pattern, full_message)

    # Create a dictionary of found IDs
    ids_dict = {}
    if condition_id:
        ids_dict['condition_id'] = condition_id
    if policy_id:
        ids_dict['policy_id'] = policy_id
    if incident_id:
        ids_dict['incident_id'] = incident_id
    if issue_id:
        ids_dict['issue_id'] = issue_id
    return str(ids_dict)

  def update_title(row):
      ids_dict = eval(row['tags'])

      # Prioritize Condition ID, Issue ID, Incident ID, Policy ID, and then text
      if ids_dict.get('condition_id'):
          return f"Condition ID: {ids_dict['condition_id']}"
      elif ids_dict.get('issue_id'):
          return f"Issue ID: {ids_dict['issue_id']}"
      elif ids_dict.get('incident_id'):
          return f"Incident ID: {ids_dict['incident_id']}"
      elif ids_dict.get('policy_id'):
          return f"Policy ID: {ids_dict['policy_id']}"
      else:
          return row['text']

  new_relic_rows = df['alert_type'] == 'New Relic'
  df.loc[new_relic_rows, 'tags'] = df.loc[new_relic_rows, 'full_message'].apply(lambda x: newrelic_extract_ids(x))
  df.loc[new_relic_rows, 'title'] = df.loc[new_relic_rows].apply(lambda x: update_title(x), axis=1)
  return df




def extract_tags_cloudwatch(df):

  def tags_extractor(full_message):
    message = eval(full_message)
    tags_available= {}
    if 'attachments' in message:
      attachments = message.get('attachments')
      tags_available_temp = {}
      if attachments:
        for attachment in attachments:
          if 'fields' in attachment:
            fields = attachment.get('fields')
            tags_available_temp = {item['title']: item['value'] for item in fields}
          tags_available.update(tags_available_temp)
    return tags_available

  def regex_specific(text):
    # Regular expression pattern for specific key-value pairs
    patterns = {
        'Name': r'-\s*Name:\s*(.*)',
        'MetricName': r'-\s*MetricName:\s*(.*)',
        'Name': r'-\s*Pulse:\s*(.*)',
        'State Change': r'-\s*State Change:\s*(.*)',
        'Reason for State Change': r'-\s*Reason for State Change:\s*(.*)',
        'Timestamp': r'-\s*Timestamp:\s*(.*)',
        'AWS Account': r'-\s*AWS Account:\s*(.*)',
        'Alarm Arn': r'-\s*Alarm Arn:\s*(.*)',
        'Alarm': r'Alarm:\s*([^\s]+)'  # Updated pattern for 'Alarm'
    }
    result = {}
    for key, pattern in patterns.items():
        match = re.search(pattern, text)
        if match:
            result[key] = match.group(1).strip()
    return str(result)

  def text_extractor(message):
    message = eval(message)
    text = ''
    texts = []
    for key in ['text', 'fallback']:
      value = message.get(key)
      if value:
        texts.append(value)
    files = message.get('files')
    if files:
      for file in files:
        for key in ['plain_text', 'preview_plain_text']:
          value = file.get(key)
          if value:
            texts.append(value)
    if texts:
      text = max(texts, key=len)
    else:
      text = ''
    return text


  def title_extractor(text):
    input_dict = eval(str(text))
    # Check if 'Alarm Arn' key exists in the dictionary
    if (('Alarm Arn' in input_dict) and (input_dict['Alarm Arn'])):
        return input_dict['Alarm Arn']
    # Check if 'Name' key exists in the dictionary
    elif (('Name' in input_dict) and (input_dict['Name'])):
        return input_dict['Name']
    elif (('Alarm' in input_dict) and (input_dict['Alarm'])):
        return input_dict['Alarm']
    # Return a default message if neither key is found
    else:
        return "No Alarm Name found"

  cloudwatch_rows = df['alert_type']=='Cloudwatch'
  df.loc[cloudwatch_rows,'text'] = df.loc[cloudwatch_rows,'full_message'].apply(lambda x: text_extractor(x))
  df.loc[cloudwatch_rows,'tags_temp'] = df.loc[cloudwatch_rows,'full_message'].apply(lambda x: tags_extractor(x))
  df.loc[cloudwatch_rows,'tags'] = df.loc[cloudwatch_rows,'text'].apply(lambda x: regex_specific(x))
  def safe_eval(dict_string):
      try:
          # Safely evaluate the string representation of a dictionary
          return eval(dict_string)
      except:
          # Return an empty dictionary in case of an error
          return {}

  # Merge the dictionaries in 'tags' and 'tags_temp' into a new column 'merged_tags'
  df.loc[cloudwatch_rows,'tags'] = df.loc[cloudwatch_rows].apply(lambda x: {**safe_eval(x['tags']),**safe_eval(x['tags_temp'])}, axis=1)
  df.loc[cloudwatch_rows,'title'] = df.loc[cloudwatch_rows,'tags'].apply(lambda x: title_extractor(x))
  df = df.drop(columns='tags_temp')
  return df
