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
            # TODO: Handle case where solution tries to add a duplicate result (for the same batch)
            query = 'INSERT INTO results (batch, receivedTimestamp, inputTimestamp, detected, eventTimestamp) VALUES(?, ?, ?, ?, ?)'
            cursor.execute(query, (state.nextReceiveIndex, submissionTime, inputTimestamp, detected, eventTimestamp))
            state.nextReceiveIndex += constants.INPUT_BATCH_SIZE




class Grader(Resource):
    

    def get(self):
        self.loadResults()
        self.verifyResults()
        self.computeScore()

    def loadResults(self):
        # TODO: Append because of replace
        resultDf = pd.read_csv(constants.OUTPUT_FILE, names=['inputTimestamp', 'detected', 'eventTimestamp'])
        with sqlite3.connect(constants.DATABASE_NAME) as con:
            resultDf.to_sql('expected', con, if_exists='replace')

    
    def verifyResults(self):
        print("Verifying results...")
        with sqlite3.connect(constants.DATABASE_NAME) as con:
            cursor = con.cursor()
            # Check if results are missing (or wrong)
            # TODO: Order tables by inputTimestamp
            cursor.execute('''SELECT inputTimestamp, detected, eventTimestamp FROM expected
                            EXCEPT
                            SELECT inputTimestamp, detected, eventTimestamp FROM results''')
            firstWrong = cursor.fetchone()
            if firstWrong:
                print('ERROR: Missing results!')
                print(firstWrong)
                return
            # Check if there are extra results that should not be there
            # TODO: Order tables by inputTimestamp
            cursor.execute('''SELECT inputTimestamp, detected, eventTimestamp FROM results
                            EXCEPT
                            SELECT inputTimestamp, detected, eventTimestamp FROM expected''')
            firstExtra = cursor.fetchone()
            if firstExtra:
                print('ERROR: Extra results!')
                print(firstExtra)
                return
        print('SUCCESS: Results verified!')

    def computeScore(self):
        latencyQuery = '''SELECT R.batch, (julianday(R.receivedTimestamp) - julianday(S.timestamp))*86400000  FROM
           sent as S 
           INNER JOIN results AS R ON S.batch = R.batch
           INNER JOIN expected AS E ON R.inputTimestamp = E.inputTimestamp;
           '''
