from flask import request
import datetime
from flask_restful import Resource
import os
import subprocess
import sqlite3
import pandas as pd
import constants
import numpy as np
import state


def floatOrNan(value):
    if value and not np.isnan(value):
        return float(value)
    return 1e10


class Benchmark(Resource):

    def __init__(self, benchmarkInput, benchmarkGrader):
        super(Benchmark, self).__init__()
        self.__benchmarkInput = benchmarkInput
        self.__benchmarkGrader = benchmarkGrader

    def get(self):
        (firstTupleIndex, lastTupleIndex) = self.__benchmarkInput.nextSendTupleIndexes()
        print(f'Requested tuples {firstTupleIndex} - {lastTupleIndex}')
        try:
            tupleBatch = self.__benchmarkInput.getChunk()
        except StopIteration:
            return {'message': 'Input finished.', 'score': self.__benchmarkGrader.getScore()}, 404

        print(tupleBatch.head(2))
        print('...')
        print(tupleBatch.tail(2))

        submissionTime = datetime.datetime.now()
        self.recordBatchEmitted(self.__benchmarkInput.currentBatchIndex(), submissionTime)
        self.__benchmarkInput.batchSent()
        return {'records': tupleBatch.to_dict(orient='records')}

    def recordBatchEmitted(self, batchIndex, submissionTime):
        with sqlite3.connect(constants.DATABASE_NAME) as con:
            cursor = con.cursor()
            query = "INSERT INTO sent (batch,timestamp) VALUES(?,?)"
            try:
                cursor.execute(query, (batchIndex, submissionTime))
            except sqlite3.IntegrityError:
                return {'Error': 'All data already sent. Restart the grader if you want to retrieve the data again.'}, 404

    def post(self):
        receivedTime = datetime.datetime.now()
        result = request.get_json()
        print(f'Received POST event: {result}')
        batchID = result['ts']
        detected = int(result['detected'])
        event = result['event_ts'] if 'event_ts' in result else None
        self.recordResult(receivedTime, batchID, detected, event)

    def recordResult(self, timestamp, batch, detected, event):
        with sqlite3.connect(constants.DATABASE_NAME) as con:
            cursor = con.cursor()
            query = 'INSERT INTO results (batch, timestamp, detected, event, lastSentBatch) VALUES(?, ?, ?, ?, ?)'
            try:
                lastSentBatch = self.__benchmarkInput.currentBatchIndex()-1
                cursor.execute(query, (batch, timestamp,
                                       detected, event, lastSentBatch))
            except sqlite3.IntegrityError:
                return {'Error': 'Only one result is allowed for each batch!'}, 404


class BenchmarkOne(Benchmark):

    def __init__(self):
        super(BenchmarkOne, self).__init__(state.TASK_ONE, GraderOne)


class BenchmarkTwo(Benchmark):

       def __init__(self):
        super(BenchmarkTwo, self).__init__(state.TASK_TWO, GraderTwo)


class GraderOne(Resource):

    def get(self):
        return self.getScore()

    @classmethod
    def getScore(cls):
        GraderOne.loadResults()
        if cls.verifyResults():
            return cls.computeScore()
        else:
            return cls.jsonScore(np.nan, np.nan)

    @classmethod
    def loadResults(cls):
        resultDf = pd.read_csv(constants.OUTPUT_FILE, names=['batch', 'detected', 'event'])
        with sqlite3.connect(constants.DATABASE_NAME) as con:
            resultDf.to_sql('expected', con, if_exists='replace')

    @classmethod
    def verifyResults(cls):
        print("Verifying results...")
        with sqlite3.connect(constants.DATABASE_NAME) as con:
            cursor = con.cursor()
            # Check if results are missing (or wrong)
            cursor.execute('''SELECT * FROM (SELECT batch, detected, event FROM expected ORDER BY batch) A
                            EXCEPT
                            SELECT * FROM (SELECT batch, detected, event FROM results ORDER BY batch) B''')
            firstWrong = cursor.fetchone()
            if firstWrong:
                print('ERROR: Missing results!')
                print(firstWrong)
                return False
            # Check if there are extra results that should not be there
            cursor.execute('''SELECT * FROM (SELECT batch, detected, event FROM results ORDER BY batch) A
                            EXCEPT
                            SELECT * FROM (SELECT batch, detected, event FROM expected ORDER BY batch) B''')
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
        totalTimeQuery = '''SELECT (julianday(MAX(R.timestamp)) - julianday(MIN(S.timestamp)))*86400000  FROM 
            results AS R, sent AS s'''

        # rank_1: average latency per batch
        batchLatencyQuery = '''SELECT AVG((julianday(R.timestamp) - julianday(S.timestamp))*86400000)  FROM
           sent as S 
           INNER JOIN results AS R ON S.batch = R.batch
           '''
        totalRuntime = np.nan
        latency = np.nan
        with sqlite3.connect(constants.DATABASE_NAME) as con:
            cursor = con.cursor()
            cursor.execute(totalTimeQuery)
            totalRuntime = cursor.fetchone()[0]
            cursor.execute(batchLatencyQuery)
            latency = cursor.fetchone()[0]
        return cls.jsonScore(totalRuntime, latency)

    @classmethod
    def jsonScore(cls, totalRuntime, latency):
        return {'total_runtime': floatOrNan(totalRuntime), 'latency': floatOrNan(latency)}


class GraderTwo(Resource):

    def get(self):
        return self.getScore()

    @classmethod
    def getScore(cls):
        GraderOne.loadResults()
        print(cls.computeScore())
        return cls.computeScore()


    @classmethod
    def computeScore(cls):
        # rank_0: timeliness
        timelinessQuery = '''SELECT SUM(MAX(0, 1 - (lastSentBatch - batch)/10.0))
                            FROM results'''

        # rank_1: accuracy
        #TODO: Implement
        timeliness = np.nan
        accuracy = np.nan
        with sqlite3.connect(constants.DATABASE_NAME) as con:
            cursor = con.cursor()
            cursor.execute(timelinessQuery)
            timeliness = cursor.fetchone()[0]
        return cls.jsonScore(timeliness, accuracy)

    @classmethod
    def jsonScore(cls, timeliness, accuracy):
        return {'timeliness': floatOrNan(timeliness), 'accuracy': floatOrNan(accuracy)}


class ResultsCsvExporter(Resource):

    def get(self):
        import csv
        with open(constants.OUTPUT_FILE, 'w') as outputFile,\
        sqlite3.connect(constants.DATABASE_NAME) as con:
            cursor = con.cursor()
            cursor.execute('SELECT batch, detected, event FROM results')
            rows = cursor.fetchall()
            writer = csv.writer(outputFile)
            writer.writerows(rows)
        print(f'Wrote {len(rows)} lines to {constants.OUTPUT_FILE}')
        return {'Message': 'OK'}
