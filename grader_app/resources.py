from flask import request
import datetime
from flask_restful import Resource
import os
import subprocess
import sqlite3
import state
import pandas as pd
import constants

class Benchmark(Resource):

    def get(self):
        print(f'Requested tuples {state.nextSendIndex} - {state.nextSendIndex + constants.INPUT_BATCH_SIZE - 1}')
        try:
            tupleBatch = state.inputDf.get_chunk(constants.INPUT_BATCH_SIZE)
        except StopIteration:
            return {'message': 'Input finished.'}, 404

        print(tupleBatch.head(2))
        print('...')
        print(tupleBatch.tail(2))

        self.recordBatchEmitted(state.nextSendIndex)
        state.nextSendIndex += constants.INPUT_BATCH_SIZE
        return {'data': tupleBatch.to_json(orient='records')}


    def recordBatchEmitted(self, batchIndex):
        with sqlite3.connect(constants.DATABASE_NAME) as con:
            cursor = con.cursor()
            query = "INSERT INTO sent (batch) VALUES(?)"
            cursor.execute(query, (batchIndex,))


    def post(self):
        submissionTime = datetime.datetime.now()
        event = request.get_json()
        print(f'Received POST event: {event}')
        inputTimestamp = event['ts']
        detected = int(event['detected'])
        eventTimestamp = event['event_ts'] if 'event_ts' in event else None
        self.recordResult(submissionTime, inputTimestamp, detected, eventTimestamp)


    def recordResult(self, submissionTime, inputTimestamp, detected, eventTimestamp):
        with sqlite3.connect(constants.DATABASE_NAME) as con:
            cursor = con.cursor()
            query = 'INSERT INTO results (batch, receivedTimestamp, inputTimestamp, detected, eventTimestamp) VALUES(?, ?, ?, ?, ?)'
            cursor.execute(query, (state.nextReceiveIndex, submissionTime, inputTimestamp, detected, eventTimestamp))
            state.nextReceiveIndex += constants.INPUT_BATCH_SIZE




class Grader(Resource):
    

    def get(self):
        self.loadResults()
        self.verifyResults()
        self.computeScore()

    def loadResults(self):
        resultDf = pd.read_csv(state.RESULT_FILE, chunksize=10000)
        with sqlite3.connect(constants.DATABASE_NAME) as con:
            resultDf.to_sql('expected', con, if_exists='replace')

    
    def verifyResults(self):
        with sqlite3.connect(constants.DATABASE_NAME) as con:
            cursor = con.cursor()
            cursor.execute('''SELECT inputTimestamp, detected, eventTimestamp FROM expected
                            EXCEPT
                            SELECT inputTimestamp, detected, eventTimestamp FROM results''')
            rows = cursor.fetchall()
            # if len > 0, report missing result
            for row in rows:
                print(row)
            # TODO: Do reverse EXCEPT and report additional (wrong) results 