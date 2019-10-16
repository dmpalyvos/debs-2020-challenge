import requests
import json
import os
import sys

if __name__ == "__main__":
    host = os.getenv('BENCHMARK_SYSTEM_URL')
    if host is None or '':
        print('Error reading Server address!')

    # Get the JSON data and print them
    while(True):
        response = requests.get("http://" + host +'/data/')
        if response.status_code == 404:
            print(response.json())
            sys.stdout.flush()
            break

        data = response.json()
        print("JSON data: " + data)
