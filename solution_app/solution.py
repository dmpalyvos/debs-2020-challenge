import requests
import json
import os
import sys
import time
import datetime
import random
import csv

# Timeout for GET requests to the grader. 
# Especially important for the first request, when the containers are still starting
GET_TIMEOUT = 60

def host_url(host):
    return "http://" + host + "/data/"

def get_batch(host):
    return requests.get(host_url(host), timeout=GET_TIMEOUT)

def post_result(host, payload):
    headers = {'Content-type': 'application/json'}
    response = requests.post(host_url(host), json = payload, headers=headers)

def random_results(batchCounter, out_file):
    my_result = {}
    my_result['ts'] = batchCounter
    my_result['detected'] = random.choice([True, False])
    if my_result['detected']:
        my_result['event_ts'] = random.randint(0, batchCounter)
        out_file.write(str(my_result['ts'])+','+str(my_result['detected'])+','+str(my_result['event_ts'])+'\n')
    else:
        out_file.write(str(my_result['ts'])+','+str(my_result['detected'])+',\n')

    return my_result

if __name__ == "__main__":
    random.seed(0)
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
    batchCounter = 0

    with open('out.csv', mode='a') as out_file:
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
            example_result = random_results(batchCounter, out_file)
            # Substitute the randomly generated results with your implementation.        
            # Produce one result tuple per batch, in JSON format {'ts':,'detected':,'event_ts':}
            # After computing the result and making sure it is in the correct format,
            # submit it via POST request:
            post_result(host, example_result)

            batchCounter += 1

    print('Results are submitted for all the batches successfuly.')
    requests.get("http://" + host + '/grade/', timeout=GET_TIMEOUT)
