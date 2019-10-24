import requests
import json
import os
import sys
import time
import datetime
import random

def host_url(host):
    return "http://" + host + "/data/"

def get_batch(host):
    return requests.get(host_url(host))

def post_result(host, payload):
    headers = {'Content-type': 'application/json'}
    response = requests.post(host_url(host), json = payload, headers=headers)

def random_results():
    my_result = {}
    my_result['ts'] = random.randint(1000, 1000000)
    my_result['detected'] = bool(random.getrandbits(1))
    if my_result['detected']:
        my_result['event_ts'] = random.randint(1000, 1000000)
    time.sleep(1)
    return my_result

if __name__ == "__main__":
    host = os.getenv('BENCHMARK_SYSTEM_URL')
    if not host:
        host = 'localhost'
        print('Warning: Benchmark system url undefined. Using localhost!')
    if host is None or '':
        print('Error reading Server address!')

    print('Getting data in batches...') 

    # Here is an script to get the data in batches and give back the results   
    # Recieved data is in the format of JSON, with attributes {'idx':,'voltage':,'current':}
    # For each batch, you produce a result with format {'ts':,'detected':,'event_ts':}
    while(True):

        # Making GET request
        # Each request will fetch new batch
        response = get_batch(host)
        if response.status_code == 404:
            print(response.json())
            break

        data = response.json()

        # RUN YOUR DETECTION ALGORITHM HERE
        # Randomly generated results
        example_result = random_results()
        # Substitute the randomly generated results with your implementation.        
        # Produce one result tuple per batch, in JSON format {'ts':,'detected':,'event_ts':}
        # After computing the result and making sure it is in the correct format,
        # submit it via POST request:
        post_result(host, example_result)

    print('Results are submitted for all the batches successfuly.')
