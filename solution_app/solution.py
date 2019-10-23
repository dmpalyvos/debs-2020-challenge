import requests
import json
import os
import sys
import time

def host_url(host, path):
    return "http://" + host + path

def get_batch(host):
    return requests.get(host_url(host, '/data/'))

def post_result(host, payload):
    headers = {'Content-type': 'application/json'}
    response = requests.post(host_url(host, '/data/'), json = payload, headers=headers)

def random_results():
    my_dict = {}
    my_dict[0] = 1
    my_dict[1] = True
    my_dict[2] = 3

    return my_dict

if __name__ == "__main__":
    host = os.getenv('BENCHMARK_SYSTEM_URL')
    if not host:
        host = 'localhost'
        print('Warning: Benchmark system url undefined. Using localhost!')
    if host is None or '':
        print('Error reading Server address!')

    # Get the JSON data and print them
    while(True):
        response = get_batch(host)
        if response.status_code == 404:
            print(response.json())
            sys.stdout.flush()
            break

        data = response.json()
        print("JSON data: " , data)

        post_result(host, random_results())
        time.sleep(1)
