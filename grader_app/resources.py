from flask import request
import datetime
from flask_restful import Resource
import os
import subprocess
import sqlite3
import state
import pandas as pd
import constants
import numpy as np


class Benchmark(Resource):

    def get(self):
        print(
            f'Requested tuples {state.nextSendIndex * constants.INPUT_BATCH_SIZE} - {(state.nextSendIndex * constants.INPUT_BATCH_SIZE) + constants.INPUT_BATCH_SIZE - 1}')
        try:
            tupleBatch = state.inputDf.get_chunk(constants.INPUT_BATCH_SIZE)
        except StopIteration:
            return {'message': 'Input finished.', 'score': Grader.getScore()}, 404

        print(tupleBatch.head(2))
        print('...')
        print(tupleBatch.tail(2))

        submissionTime = datetime.datetime.now()
        self.recordBatchEmitted(state.nextSendIndex, submissionTime)
        state.nextSendIndex += 1
        return {'records': tupleBatch.to_json(orient='records')}

    def recordBatchEmitted(self, batchIndex, submissionTime):
        with sqlite3.connect(constants.DATABASE_NAME) as con:
            cursor = con.cursor()
            query = "INSERT INTO sent (batch,timestamp) VALUES(?,?)"
            cursor.execute(query, (batchIndex, submissionTime))

    def post(self):
        receivedTime = datetime.datetime.now()
        event = request.get_json()
        print(f'Received POST event: {event}')
        batchID = event['ts']
        detected = int(event['detected'])
        eventTimestamp = event['event_ts'] if 'event_ts' in event else None
        self.recordResult(receivedTime, batchID, detected, eventTimestamp)

    def recordResult(self, receivedTime, batchID, detected, eventTimestamp):
        with sqlite3.connect(constants.DATABASE_NAME) as con:
            cursor = con.cursor()
            # TODO: Handle case where solution tries to add a duplicate result (for the same batch)
            query = 'INSERT INTO results (batch, receivedTimestamp, detected, eventTimestamp) VALUES(?, ?, ?, ?)'
            try:
                cursor.execute(query, (batchID, receivedTime,
                                       detected, eventTimestamp))
            except sqlite3.IntegrityError:
                return {'Error': 'Only one result is allowed for each batch!'}, 404


class Grader(Resource):

    def get(self):
        return self.getScore()

    @classmethod
    def getScore(cls):
        Grader.loadResults()
        if cls.verifyResults():
            return cls.computeScore()
        else:
            return cls.jsonScore(np.nan, np.nan)

    @classmethod
    def loadResults(cls):
        resultDf = pd.read_csv(constants.OUTPUT_FILE, names=[
                               'batch', 'detected', 'eventTimestamp'])
        with sqlite3.connect(constants.DATABASE_NAME) as con:
            resultDf.to_sql('expected', con, if_exists='replace')

    @classmethod
    def verifyResults(cls):
        print("Verifying results...")
        with sqlite3.connect(constants.DATABASE_NAME) as con:
            cursor = con.cursor()
            # Check if results are missing (or wrong)
            cursor.execute('''SELECT * FROM (SELECT batch, detected, eventTimestamp FROM expected ORDER BY batch) A
                            EXCEPT
                            SELECT * FROM (SELECT batch, detected, eventTimestamp FROM results ORDER BY batch) B''')
            firstWrong = cursor.fetchone()
            if firstWrong:
                print('ERROR: Missing results!')
                print(firstWrong)
                return False
            # Check if there are extra results that should not be there
            cursor.execute('''SELECT * FROM (SELECT batch, detected, eventTimestamp FROM results ORDER BY batch) A
                            EXCEPT
                            SELECT * FROM (SELECT batch, detected, eventTimestamp FROM expected ORDER BY batch) B''')
            firstExtra = cursor.fetchone()
            if firstExtra:
                print('ERROR: Extra results!')
                print(firstExtra)
                return False
        print('SUCCESS: Results verified!')
        return True

    @classmethod
    def computeScore(cls):
        # rank_0: the total time span between sending the first batch and recieving the last result
        # Returns latency in days so we need to multiply by ms per day
        # 24*60*60*1000 = 8_640_0000
        totalTimeQuery = '''SELECT (julianday(MAX(r.receivedTimestamp)) - julianday(MIN(s.timestamp)))*86400000  FROM 
            results AS R, sent AS s'''

        # rank_1: average latency per batch
        batchLatencyQuery = '''SELECT AVG((julianday(R.receivedTimestamp) - julianday(S.timestamp))*86400000)  FROM
           sent as S 
           INNER JOIN results AS R ON S.batch = R.batch
           '''
        totalRuntime = np.nan
        latency = np.nan
        with sqlite3.connect(constants.DATABASE_NAME) as con:
            cursor = con.cursor()
            cursor.execute(totalTimeQuery)
            totalRuntime = float(cursor.fetchone()[0])
            cursor.execute(batchLatencyQuery)
            latency = float(cursor.fetchone()[0])
        return cls.jsonScore(totalRuntime, latency)

    @classmethod
    def jsonScore(cls, totalRuntime, latency):
        return {'total_runtime': totalRuntime, 'latency': latency}


class ResultsCsvExporter(Resource):

    def get(self):
        import csv
        with open(constants.OUTPUT_FILE, 'w') as outputFile,\
        sqlite3.connect(constants.DATABASE_NAME) as con:
            cursor = con.cursor()
            cursor.execute('SELECT batch, detected, eventTimestamp FROM results')
            rows = cursor.fetchall()
            writer = csv.writer(outputFile)
            writer.writerows(rows)
        print(f'Wrote {len(rows)} lines to {constants.OUTPUT_FILE}')
        return {'Message': 'OK'}
