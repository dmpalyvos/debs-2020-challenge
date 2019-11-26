
from flask import Flask
from flask_restful import Api
import sqlite3
import state
import constants
import os

from resources import Benchmark, Grader, ResultsCsvExporter, BenchmarkOne, BenchmarkTwo


# TODO: Rename columns to more meaningful names
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
                        batch INTEGER  PRIMARY KEY NOT NULL,
                        timestamp DATETIME NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now', 'localtime'))
                        )''')

        cursor.execute('''CREATE TABLE results (
                        batch INTEGER  PRIMARY KEY NOT NULL,
                        detected INTEGER,
                        event INTEGER,
                        timestamp DATETIME
                        )''')

        cursor.execute('''CREATE TABLE expected (
                        batch INTEGER  PRIMARY KEY NOT NULL,
                        detected INTEGER,
                        event INTEGER
                        )''')


app = Flask(__name__)
api = Api(app)

api.add_resource(BenchmarkOne, constants.DATA_TASK_ONE_ENDPOINT)
api.add_resource(BenchmarkTwo, constants.DATA_TASK_TWO_ENDPOINT)
api.add_resource(Grader, constants.GRADER_ENDPOINT)
api.add_resource(ResultsCsvExporter, constants.RESULTS_EXPORTER_ENDPOINT)

if __name__ == '__main__':
    initDatabase()
    app.run(host=constants.SERVER_HOST, port=constants.SERVER_PORT)
