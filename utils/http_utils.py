import requests

from env_vars import REPORT_SERVICE_URL


def send_report_intimation(account_id):
    url = f'{REPORT_SERVICE_URL}/connectors/report_intimation'
    headers = {
        'Content-Type': 'application/json'
    }

    data = {
        "account_id": account_id,
        "report_type": "INITIAL"
    }

    print(f"Sending report intimation for account: {account_id}")
    try:
        response = requests.post(url, headers=headers, json=data)
        response.raise_for_status()
        print(f"Status code: {response.status_code}")
        print("Response body:", response.json())
    except requests.exceptions.RequestException as e:
        print(f"Error making POST request: {e}")
