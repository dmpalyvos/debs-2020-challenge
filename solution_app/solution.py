import requests
import json
import os
import sys
import time
import datetime
import random
import csv

import ipdb
import glob
import pandas as pd
import Event_Detector as ed
import Test_Utility as tu
import numpy as np

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

    NETWORK_FREQUENCY = 50 # Base electrical network frequency of the region where the dataset was recorded
    VALUES_PER_SECOND = 50 # We compute 50 features (data points) per second, once every 1000 samples
    SAMPLERATE= 50000 # Sampling Rate the raw Dataset

    # Hyperparameters Dictionary for the Event Detector
    init_dict = {"dbscan_eps": 0.03, #epsilon radius parameter for the dbscan algorithm
                           "dbscan_min_pts": 2,  # minimum points parameter for the dbscan algorithm 
                           "window_size_n": 50,  # datapoints the algorithm takes one at a time, i..e here 1 second
                           "values_per_second": VALUES_PER_SECOND,  # datapoints it needs from the future
                           "loss_thresh": 40, # threshold for model loss 
                           "temp_eps": 0.8, # temporal epsilon parameter of the algorithm
                           "debugging_mode": False, # debugging, yes or no - if yes detailed information is printed to console
                           "network_frequency": 50} #base frequency

    # Compute some relevant window sizes etc. for the "streaming"
    window_size_seconds = init_dict["window_size_n"] / VALUES_PER_SECOND

    # Compute how big the window is regarding the raw samples --> this is used for the "streaming"
    SAMPLES_RAW_PER_WINDOW = SAMPLERATE * window_size_seconds

    # Compute the period size of the dataset: i.e. number of raw data points per period
    period = int(SAMPLERATE / NETWORK_FREQUENCY) 

    EventDet_Barsim = ed.STREAMING_EventDet_Barsim_Sequential(**init_dict) #i.e. values are unpacked into the parameters
    EventDet_Barsim.fit() # Call the fit() method to further initialize the algorithm (required by the sklearn API)


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

            feature_index +=1 
        
            # Compute the feature for the 1000 samples we have buffered
            X_i = EventDet_Barsim.compute_input_signal(voltage=np.array(data['voltage']), current=np.array(data['current']), period_length=period,
                                                      single_sample_mode=True) 
            
            features_streamed.append(X_i) #append the newly computed feature point to the features streamed

            if X is None:
                X = X_i #if it is the first point in our window
                window_start_index = feature_index #the index the window is starting with
                
            else:
                X = np.concatenate([X, X_i],axis=0) #add new feature point to window

            # Step 3: Run the prediciton on the features
            event_interval_indices = EventDet_Barsim.predict(X) #(start_index, end_index) of event if existent is returned
            
            if event_interval_indices is not None: # if an event is returned
                
                print("Event Detected at ",current_window_start+event_interval_indices[0],',',current_window_start+event_interval_indices[1])

                # Instead of an event interval, we might be interested in an exact event point
                # Hence, we just take the mean of the interval boundaries
                mean_event_index = np.mean([event_interval_indices[0], event_interval_indices[1]])
                
                # Now we create a new data window X
                # We take all datapoints that we have already receveived, beginning form the index of the event (the end index of the event interval in this case)
                end_event_index = event_interval_indices[1] #the end_index of the event
                         
                X = X[end_event_index:] # set a new X
                window_start_index = window_start_index + end_event_index # the index the window is starting with
                
            else: #no event was detected

                # We start at the end of the previous window
                # Hence, at first, we do nothing, except the maximum window size is exceeded
                if len(X) > MAXIMUM_WINDOW_SIZE: #if the maximum window size is exceeded
                    X = None # Reset X

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
