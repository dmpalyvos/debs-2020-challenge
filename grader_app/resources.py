from flask import request
import datetime
from flask_restful import Resource
import os
import subprocess
import sqlite3
import state

class Benchmark(Resource):

    def get(self):
        print(f'Requested tuples {state.nextSendIndex} - {state.nextSendIndex + state.BATCH_SIZE - 1}')
        tupleBatch = state.inputDf.get_chunk(state.BATCH_SIZE)
        print(tupleBatch.head(2))
        print('...')
        print(tupleBatch.tail(2))
        self.recordBatchEmitted(state.nextSendIndex)
        state.nextSendIndex += state.BATCH_SIZE
        return {'data': tupleBatch.to_json(orient='records')}


    def recordBatchEmitted(self, batchIndex):
        con = sqlite3.connect(state.DB_NAME)
        with con:
            cursor = con.cursor()
            query = "INSERT INTO sent (batch) VALUES(?)"
            cursor.execute(query, (batchIndex,))


    def post(self):
        submissionTime = datetime.datetime.now()
        event = request.get_json()
        print(f'Received POST for event: {event}')
        self.recordResult(submissionTime, event)


    def recordResult(self, submissionTime, event):
        con = sqlite3.connect(state.DB_NAME)
        with con:
            cursor = con.cursor()
            query = 'INSERT INTO received (batch, timestamp, result) VALUES(?, ?, ?)'
            cursor.execute(query, (state.nextReceiveIndex, submissionTime, str(event.values())))
            state.nextReceiveIndex += state.BATCH_SIZE



