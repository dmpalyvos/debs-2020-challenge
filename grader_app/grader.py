
from flask import Flask
from flask_restful import Api
import sqlite3
import state
import constants
import os
import logging
import resources
import signal
from contextlib import contextmanager

def initDatabase():
    # Remove database in case it exists
    try:
        os.remove(constants.DATABASE_NAME)
    except OSError:
        pass
    # (Re)create database
    con = sqlite3.connect(constants.DATABASE_NAME)
    with con:
        cursor = con.cursor()

        cursor.execute('''CREATE TABLE sent (
                        task INTEGER NOT NULL,
                        batch INTEGER NOT NULL,
                        timestamp DATETIME NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now', 'localtime')),
                        PRIMARY KEY (task, batch)
                        )''')

        cursor.execute('''CREATE TABLE results (
                        task INTEGER NOT NULL,
                        batch INTEGER  NOT NULL,
                        detected INTEGER,
                        event REAL,
                        timestamp DATETIME,
                        lastSentBatch INTEGER,
                        PRIMARY KEY (task, batch)
                        )''')

        cursor.execute('''CREATE TABLE expected (
                        batch INTEGER  PRIMARY KEY NOT NULL,
                        detected INTEGER,
                        event REAL
                        )''')


app = Flask(__name__)
api = Api(app)

api.add_resource(resources.BenchmarkOne, constants.DATA_TASK_ONE_ENDPOINT)
api.add_resource(resources.BenchmarkTwo, constants.DATA_TASK_TWO_ENDPOINT)
api.add_resource(resources.GraderOne, constants.GRADER_ENDPOINT_TASK_ONE)
api.add_resource(resources.GraderTwo, constants.GRADER_ENDPOINT_TASK_TWO)
api.add_resource(resources.GraderFinal, constants.GRADER_ENDPOINT_FINAL)
api.add_resource(resources.ResultsCsvExporter, constants.RESULTS_EXPORTER_ENDPOINT)

@contextmanager
def timeout(time):
    signal.signal(signal.SIGALRM, raise_timeout)
    signal.alarm(time)

    try:
        yield
    except TimeoutError:
        pass
    finally:
        signal.signal(signal.SIGALRM, signal.SIG_IGN)

def raise_timeout(signum, frame):
    raise TimeoutError

if __name__ == '__main__':
    initDatabase()
    from waitress import serve
    with timeout(constants.timeout_wait_seconds):
        serve(app, host=constants.SERVER_HOST, port=constants.SERVER_PORT)
