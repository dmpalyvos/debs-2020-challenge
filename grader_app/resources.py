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
import json

def floatOrOther(value, other):
    if value and not np.isnan(value):
        return float(value)
    return other


class Benchmark(Resource):

    def __init__(self, taskId, benchmarkInput, benchmarkGrader):
        super(Benchmark, self).__init__()
        self.taskId = taskId
        self.__benchmarkInput = benchmarkInput
        self.__benchmarkGrader = benchmarkGrader

    def get(self):
        try:
            records = self.__benchmarkInput.getNextRecords()
        except StopIteration:
            results = self.__benchmarkGrader.getScore()
            response = {'message': 'Input finished.', 'score': results}
            print(response)
            self.writeResultsToFile(results)
            return response, 404
        submissionTime = datetime.datetime.now()
        self.recordBatchEmitted(self.__benchmarkInput.currentBatchIndex(), submissionTime)
        self.__benchmarkInput.batchSent()
        return records

    def recordBatchEmitted(self, batchIndex, submissionTime):
        with sqlite3.connect(constants.DATABASE_NAME) as con:
            cursor = con.cursor()
            query = "INSERT INTO sent (task, batch, timestamp) VALUES(?, ?, ?)"
            try:
                cursor.execute(query, (self.taskId, batchIndex, submissionTime))
            except sqlite3.IntegrityError:
                return {'Error': 'All data already sent. Restart the grader if you want to retrieve the data again.'}, 404


    def writeResultsToFile(self, results):
        try:
            with open(constants.RESULT_FILE, 'r') as resultFile:
                currentResults = json.load(resultFile)
        except:
            currentResults = {}

        # Enrich results with new values
        for key, value in results.items():
            if key in currentResults:
                raise ValueError(f'Duplicate result found in {constants.RESULT_FILE} for key "{key}"')
            currentResults[key] = value

        try:
            with open(constants.RESULT_FILE, 'w+') as resultFile:
                json.dump(currentResults, resultFile, indent=4)
                print(f'Wrote results to {constants.RESULT_FILE}')
        except Exception as e:
            print(f'Failed to write results to {constants.RESULT_FILE}: {e}')

    def post(self):
        receivedTime = datetime.datetime.now()
        result = request.get_json()
        batchID = result['s']
        detected = int(result['d'])
        event = result['event_s'] if 'event_s' in result else None
        self.recordResult(receivedTime, batchID, detected, event)

    def recordResult(self, timestamp, batch, detected, event):
        with sqlite3.connect(constants.DATABASE_NAME) as con:
            cursor = con.cursor()
            query = 'INSERT INTO results (task, batch, timestamp, detected, event, lastSentBatch) VALUES(?, ?, ?, ?, ?, ?)'
            try:
                lastSentBatch = self.__benchmarkInput.currentBatchIndex()-1
                cursor.execute(query, (self.taskId, batch, timestamp,
                                       detected, event, lastSentBatch))
            except sqlite3.IntegrityError:
                return {'Error': 'Only one result is allowed for each batch!'}, 404


class BenchmarkOne(Benchmark):

    def __init__(self):
        super(BenchmarkOne, self).__init__(constants.TASK_ONE_ID, state.TASK_ONE, GraderOne)


class BenchmarkTwo(Benchmark):

    def __init__(self):
        super(BenchmarkTwo, self).__init__(constants.TASK_TWO_ID, state.TASK_TWO, GraderTwo)


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
                            SELECT * FROM (SELECT batch, detected, event FROM results WHERE task = ? ORDER BY batch) B''',
                            (constants.TASK_ONE_ID,))
            firstWrong = cursor.fetchone()
            if firstWrong:
                print('ERROR: Missing results!')
                print(firstWrong)
                return False
            # Check if there are extra results that should not be there
            cursor.execute('''SELECT * FROM (SELECT batch, detected, event FROM results WHERE task = ? ORDER BY batch) A
                            EXCEPT
                            SELECT * FROM (SELECT batch, detected, event FROM expected ORDER BY batch) B''',
                            (constants.TASK_ONE_ID, ))
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
        totalTimeQuery = '''SELECT (julianday(MAX(R.timestamp)) - julianday(MIN(S.timestamp)))*86400000 FROM 
            results AS R, sent AS S 
            WHERE R.task = S.task and R.task = ?'''

        # rank_1: average latency per batch
        batchLatencyQuery = '''SELECT AVG((julianday(R.timestamp) - julianday(S.timestamp)))*86400000 FROM
           sent as S 
           INNER JOIN results AS R ON S.batch = R.batch AND R.task = S.task AND R.task = ?'''
        totalRuntime = np.nan
        latency = np.nan
        with sqlite3.connect(constants.DATABASE_NAME) as con:
            cursor = con.cursor()
            cursor.execute(totalTimeQuery, (constants.TASK_ONE_ID,))
            totalRuntime = cursor.fetchone()[0]
            cursor.execute(batchLatencyQuery, (constants.TASK_ONE_ID,))
            latency = cursor.fetchone()[0]
        return cls.jsonScore(totalRuntime, latency)

    @classmethod
    def jsonScore(cls, totalRuntime, latency):
        return {'total_runtime': floatOrOther(totalRuntime, "inf"), 'latency': floatOrOther(latency, "inf")}


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
                            FROM results
                            WHERE task = ?'''
        # rank_1: accuracy
        accuracyQuery = '''SELECT SUM(MAX(0, 1 - ABS(R.event - E.event)/10.0))
                            FROM expected E
                            INNER JOIN results R
                            ON E.batch = R.batch AND E.detected = R.detected AND E.detected = 1 AND R.task = ?'''
        timeliness = np.nan
        accuracy = np.nan
        with sqlite3.connect(constants.DATABASE_NAME) as con:
            cursor = con.cursor()
            cursor.execute(timelinessQuery, (constants.TASK_TWO_ID,))
            timeliness = cursor.fetchone()[0]
            cursor.execute(accuracyQuery, (constants.TASK_TWO_ID,))
            accuracy = cursor.fetchone()[0]
        return cls.jsonScore(timeliness, accuracy)

    @classmethod
    def jsonScore(cls, timeliness, accuracy):
        return {'timeliness': floatOrOther(timeliness, 0), 
                'accuracy': floatOrOther(accuracy, 0)}


class GraderFinal(Resource):

    def get(self):
        score1 = GraderOne.getScore()
        score2 = GraderTwo.getScore()
        return {**score1, **score2}


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
