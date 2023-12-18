import logging
import os

import pandas as pd
from datetime import datetime

import requests

from env_vars import RAW_DATA_S3_BUCKET_NAME, PUSH_TO_S3
from utils.publishsing_client import publish_object_file_to_s3

logger = logging.getLogger(__name__)


class NewRelicRestApiProcessor:
    client = None

    def __init__(self, new_relic_api_key, account_id, new_relic_query_key=None):
        self.__new_relic_key = new_relic_api_key
        self.__account_id = account_id
        self.__new_relic_query_key = new_relic_query_key
        self.base_url = f'https://api.newrelic.com/v2'

    def fetch_services(self, account_id):
        services_url = f'{self.base_url}/applications.json'

        # Set up the headers with the API key
        headers = {
            'Api-Key': self.__new_relic_key,
            'Content-Type': 'application/json'
        }

        # Set up the parameters with the account ID
        params = {
            'filter[account_id]': self.__account_id
        }

        try:
            # Make the API request to get the list of services
            response = requests.get(services_url, headers=headers, params=params)

            # Check if the request was successful (status code 200)
            if response.status_code == 200:
                # Parse the JSON response
                services_data = response.json()

                # Extract and print the list of services
                services = services_data['applications']
                for service in services:
                    print(f"Service Name: {service['name']}, Service ID: {service['id']}")
                return services
            else:
                print(f"Error: {response.status_code}, {response.text}")
        except Exception as e:
            print(f"An error occurred: {e}")
        return None

    def fetch_alert_violations(self, start_date: str = None, end_date: str = None):
        alerts_violations_url = f'{self.base_url}/alerts_violations.json'

        # Set up the headers with the API key
        headers = {
            'Api-Key': self.__new_relic_key,
            'Content-Type': 'application/json'
        }

        if end_date is None or end_date == '':
            end_date = datetime.now().strftime('%Y-%m-%d')

        if start_date is None or start_date == '':
            start_date = (datetime.now() - pd.DateOffset(years=1)).strftime('%Y-%m-%d')

        all_violations = []
        try:
            # Make the API request to get the list of services
            print(f"Fetching violations from {start_date} to {end_date} for account_id: {self.__account_id}")
            for i in range(0, 250):
                params = {
                    'page': i,
                    'start_date': start_date,
                    'end_date': end_date
                }
                response = requests.get(alerts_violations_url, headers=headers, params=params)

                # Check if the request was successful (status code 200)
                if response.status_code == 200:
                    # Parse the JSON response
                    alerts_data = response.json()

                    # Extract and print the list of services
                    violations = alerts_data['violations']
                    all_violations.extend(violations)
                    print(f"Found {len(violations)} violations on page {i}")
                    if len(violations) <= 0:
                        break
                else:
                    print(f"Error: {response.status_code}, {response.text}")
                    continue
        except Exception as e:
            print(f"An error occurred: {e}")

        try:
            raw_data = pd.DataFrame(all_violations)
            if raw_data.shape[0] > 0:
                raw_data = raw_data.reset_index(drop=True)
                base_dir = os.path.dirname(os.path.abspath(__file__))
                csv_file_name = f"{self.__account_id}-{end_date}-all_violations_data.csv"
                file_path = os.path.join(base_dir, csv_file_name)
                raw_data.to_csv(file_path, index=False)
                if PUSH_TO_S3:
                    publish_object_file_to_s3(file_path, RAW_DATA_S3_BUCKET_NAME, csv_file_name)
                    logger.info(f"Successfully extracted {len(all_violations)} alerts for account: {self.__account_id}")
                    try:
                        os.remove(file_path)
                        logger.error(f"File '{file_path}' deleted successfully.")
                    except FileNotFoundError:
                        logger.error(f"File '{file_path}' not found.")
                    except PermissionError:
                        logger.error(
                            f"Permission error. You may not have the necessary permissions to delete the file.")
                    except Exception as e:
                        logger.error(f"An error occurred: {e}")
            else:
                logger.error(f"No alert violations found for account: {self.__account_id}")
                return False
        except Exception as e:
            logger.error(f"An error occurred while fetching alert violations: {e}")
            return False
        return True

    def fetch_alert_policies(self):
        alert_policies_url = f'{self.base_url}/alerts_policies.json'

        # Set up the headers with the API key
        headers = {
            'Api-Key': self.__new_relic_key,
            'Content-Type': 'application/json'
        }

        all_policies = []
        try:
            # Make the API request to get the list of services
            print(f"Fetching alert policies for account_id: {self.__account_id}")
            for i in range(0, 250):
                params = {
                    'page': i,
                }
                response = requests.get(alert_policies_url, headers=headers, params=params)

                # Check if the request was successful (status code 200)
                if response.status_code == 200:
                    # Parse the JSON response
                    alert_policy_data = response.json()

                    # Extract and print the list of services
                    policies = alert_policy_data['policies']
                    all_policies.extend(policies)
                    print(f"Found {len(policies)} policies on page {i}")
                    if len(policies) <= 0:
                        break
                else:
                    print(f"Error: {response.status_code}, {response.text}")
                    continue
        except Exception as e:
            print(f"An error occurred: {e}")

        try:
            raw_data = pd.DataFrame(all_policies)
            if raw_data.shape[0] > 0:
                raw_data = raw_data.reset_index(drop=True)
                base_dir = os.path.dirname(os.path.abspath(__file__))
                csv_file_name = f"{self.__account_id}-all_policies_data.csv"
                file_path = os.path.join(base_dir, csv_file_name)
                raw_data.to_csv(file_path, index=False)
                if PUSH_TO_S3:
                    publish_object_file_to_s3(file_path, RAW_DATA_S3_BUCKET_NAME, csv_file_name)
                    logger.info(
                        f"Successfully extracted {len(all_policies)} alert policies for account: {self.__account_id}")
                    try:
                        os.remove(file_path)
                        logger.error(f"File '{file_path}' deleted successfully.")
                    except FileNotFoundError:
                        logger.error(f"File '{file_path}' not found.")
                    except PermissionError:
                        logger.error(
                            f"Permission error. You may not have the necessary permissions to delete the file.")
                    except Exception as e:
                        logger.error(f"An error occurred: {e}")
            else:
                logger.error(f"No alert policies found for account: {self.__account_id}")
                return None
        except Exception as e:
            logger.error(f"An error occurred while fetching alert policies: {e}")
            return None
        return all_policies

    def fetch_alert_policies_nrql_conditions(self, policy_ids: [] = None):
        alert_policies_nrql_url = f'{self.base_url}/alerts_nrql_conditions.json'

        # Set up the headers with the API key
        headers = {
            'Api-Key': self.__new_relic_key,
            'Content-Type': 'application/json'
        }

        all_policies_nrql_conditions = []
        if policy_ids is None or len(policy_ids) <= 0:
            policy_ids = self.fetch_alert_policies()

        for policy in policy_ids:
            try:
                # Make the API request to get the list of services
                policy_id = policy.get('id', None)
                if not policy_id:
                    print(f"Skipping fetching nrql condition: policy_id not found")
                    continue
                print(f"Fetching alert policies nrql condition for policy_id: {policy_id}")
                for i in range(0, 250):
                    params = {
                        'page': i,
                        'policy_id': policy_id
                    }
                    response = requests.get(alert_policies_nrql_url, headers=headers, params=params)

                    # Check if the request was successful (status code 200)
                    if response.status_code == 200:
                        # Parse the JSON response
                        alert_policy_data = response.json()

                        # Extract and print the list of services
                        nrql_conditions = alert_policy_data['nrql_conditions']
                        for nrql_condition in nrql_conditions:
                            nrql_condition['policy_id'] = policy_id
                        all_policies_nrql_conditions.extend(nrql_conditions)
                        print(f"Found {len(nrql_conditions)} nrql conditions for policy {policy_id} on page {i}")
                        if len(nrql_conditions) <= 0:
                            break
                    else:
                        print(f"Error: {response.status_code}, {response.text}")
                        continue
            except Exception as e:
                print(f"An error occurred: {e}")

            try:
                raw_data = pd.DataFrame(all_policies_nrql_conditions)
                if raw_data.shape[0] > 0:
                    raw_data = raw_data.reset_index(drop=True)
                    base_dir = os.path.dirname(os.path.abspath(__file__))
                    csv_file_name = f"{self.__account_id}-all_policies_nrql_conditions_data.csv"
                    file_path = os.path.join(base_dir, csv_file_name)
                    raw_data.to_csv(file_path, index=False)
                    if PUSH_TO_S3:
                        publish_object_file_to_s3(file_path, RAW_DATA_S3_BUCKET_NAME, csv_file_name)
                        logger.info(f"Successfully extracted {len(all_policies_nrql_conditions)} "
                                    f"policies nrql conditions for account: {self.__account_id}")
                        try:
                            os.remove(file_path)
                            logger.error(f"File '{file_path}' deleted successfully.")
                        except FileNotFoundError:
                            logger.error(f"File '{file_path}' not found.")
                        except PermissionError:
                            logger.error(
                                f"Permission error. You may not have the necessary permissions to delete the file.")
                        except Exception as e:
                            logger.error(f"An error occurred: {e}")
                else:
                    logger.error(f"No alert policy nrql conditions found for account: {self.__account_id}")
                    return None
            except Exception as e:
                logger.error(f"An error occurred while fetching alert policy nrql conditions: {e}")
                return None
            return all_policies_nrql_conditions
