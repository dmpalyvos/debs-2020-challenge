import requests
import json
import os
import sys
import time
import datetime
import random
import csv

import glob
import pandas as pd
import Event_Detector as ed
import Test_Utility as tu
import numpy as np

# Timeout for GET requests to the grader.
# Especially important for the first request, when the containers are still starting
GET_TIMEOUT = 60


def host_url(host):
    return "http://" + host + "/data/2/"


def get_batch(host):
    return requests.get(host_url(host), timeout=GET_TIMEOUT)


def post_result(host, payload):
    headers = {'Content-type': 'application/json'}
    response = requests.post(host_url(host), json=payload, headers=headers)

# This baseline for query 2 aims at optimal

if __name__ == "__main__":
    random.seed(0)
    host = os.getenv('BENCHMARK_SYSTEM_URL')
    if not host:
        host = 'localhost'
        print('Warning: Benchmark system url undefined. Using localhost!')
    if host is None or '':
        print('Error reading Server address!')

    # Base electrical network frequency of the region where the dataset was recorded
    NETWORK_FREQUENCY = 50
    # We compute 50 features (data points) per second, once every 1000 samples
    VALUES_PER_SECOND = 50
    SAMPLERATE = 50000  # Sampling Rate the raw Dataset

    # Hyperparameters Dictionary for the Event Detector
    init_dict = {"dbscan_eps": 0.03,  # epsilon radius parameter for the dbscan algorithm
                 "dbscan_min_pts": 2,  # minimum points parameter for the dbscan algorithm
                 "window_size_n": 50,  # datapoints the algorithm takes one at a time, i..e here 1 second
                 "values_per_second": VALUES_PER_SECOND,  # datapoints it needs from the future
                 "loss_thresh": 40,  # threshold for model loss
                 "temp_eps": 0.8,  # temporal epsilon parameter of the algorithm
                 # debugging, yes or no - if yes detailed information is printed to console
                 "debugging_mode": False,
                 "network_frequency": 50}  # base frequency

    # Compute some relevant window sizes etc. for the "streaming"
    window_size_seconds = init_dict["window_size_n"] / VALUES_PER_SECOND

    # Compute how big the window is regarding the raw samples --> this is used for the "streaming"
    SAMPLES_RAW_PER_WINDOW = SAMPLERATE * window_size_seconds

    # Compute the period size of the dataset: i.e. number of raw data points per period
    period = int(SAMPLERATE / NETWORK_FREQUENCY)

    EventDet_Barsim = ed.STREAMING_EventDet_Barsim_Sequential(
        **init_dict)  # i.e. values are unpacked into the parameters
    # Call the fit() method to further initialize the algorithm (required by the sklearn API)
    EventDet_Barsim.fit()

    current_window_start = 0
    current_window_end = 0

    MAXIMUM_WINDOW_SIZE = 100  # 2 seconds

    # the to the feature domain converted data that we have already receievd from the stream
    features_streamed = []

    # the data (feature domain) that is used for the prediction, i.e. our current window
    X = None

    print('Getting data in batches...')

    # Here is a script to get the data in batches and give back the results
    # Recieved data is in JSON format, with attributes {'idx':,'voltage':,'current':}
    # For each batch, you produce a result with format {'ts':,'detected':,'event_ts':}
    batchCounter = 0
    feature_index = 0
    while(True):

        # Making GET request
        # Each request will fetch a new batch
        response = get_batch(host)
        if response.status_code == 404 or response.status_code == 500:
            print(response.json())
            break

        jsonRecords = response.json()['records']
        data = pd.DataFrame.from_dict(jsonRecords)
        
        batch_left_boundary=batchCounter*1000
        batch_right_boundary=(batchCounter+1)*1000
        voltage_arr=np.empty([1000,1])
        current_arr=np.empty([1000,1])
        found=0
        arr_index=0
        for i in range(batch_left_boundary,batch_right_boundary):
            idx = data.index[data['idx']==i]
            if (len(idx)==1):
                voltage_arr[arr_index,0] = data['voltage'].iloc[idx[0]]
                current_arr[arr_index,0] = data['current'].iloc[idx[0]]
                found+=1
            else:
                voltage_arr[arr_index,0] = 2.0
                current_arr[arr_index,0] = 2.0
            arr_index+=1
        if found<1000:
            print('Expected timestamps [',batchCounter*1000,',',(batchCounter+1)*1000,'[')
            print('Found ',found,' out of 1000')

        feature_index += 1

        # Compute the feature for the 1000 samples we have buffered
        X_i = EventDet_Barsim.compute_input_signal(voltage=voltage_arr, current=current_arr, period_length=period,
                                                   single_sample_mode=True)

        # append the newly computed feature point to the features streamed
        features_streamed.append(X_i)

        if X is None:
            X = X_i  # if it is the first point in our window
            window_start_index = feature_index  # the index the window is starting with

            current_window_start = feature_index
            current_window_end = feature_index

        else:
            # add new feature point to window
            X = np.concatenate([X, X_i], axis=0)

            current_window_end = current_window_end+1

        # Step 3: Run the prediciton on the features
        # (start_index, end_index) of event if existent is returned
        event_interval_indices = EventDet_Barsim.predict(X)

        my_result = {}
        my_result['ts'] = batchCounter

        if event_interval_indices is not None:  # if an event is returned

            print("Event Detected at ", current_window_start +
                  event_interval_indices[0], ',', current_window_start+event_interval_indices[1])

            # Instead of an event interval, we might be interested in an exact event point
            # Hence, we just take the mean of the interval boundaries
            mean_event_index = np.mean(
                [event_interval_indices[0], event_interval_indices[1]])

            # Now we create a new data window X
            # We take all datapoints that we have already receveived, beginning form the index of the event (the end index of the event interval in this case)
            # the end_index of the event
            end_event_index = event_interval_indices[1]

            X = X[end_event_index:]  # set a new X
            # the index the window is starting with
            window_start_index = window_start_index + end_event_index

            my_result['detected'] = True
            my_result['event_ts'] = current_window_start+mean_event_index

            current_window_start = window_start_index

        else:  # no event was detected

            # We start at the end of the previous window
            # Hence, at first, we do nothing, except the maximum window size is exceeded
            if len(X) > MAXIMUM_WINDOW_SIZE:  # if the maximum window size is exceeded
                X = None  # Reset X

            my_result['detected'] = False
            my_result['event_ts'] = -1

        post_result(host, my_result)

        batchCounter += 1

    print('Solution done!')
