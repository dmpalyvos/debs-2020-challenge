
from flask import Flask
from flask_restful import Api
import sqlite3
import state
import constants

from resources import Benchmark, Grader


# TODO: Rename columns to more meaningful names
# TODO: Remove redundant timestamp fields
def createTables():
    con = sqlite3.connect(constants.DATABASE_NAME)
    with con:
        cursor = con.cursor()

        cursor.execute('''CREATE TABLE sent (
                        batch INTEGER  PRIMARY KEY NOT NULL,
                        timestamp DATETIME NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now', 'localtime'))
                        )''')

        cursor.execute('''CREATE TABLE results (
                        batch INTEGER  PRIMARY KEY NOT NULL,
                        receivedTimestamp DATETIME,
                        inputTimestamp INTEGER,
                        detected INTEGER,
                        eventTimestamp INTEGER
                        )''')

        cursor.execute('''CREATE TABLE expected (
                        inputTimestamp INTEGER,
                        detected INTEGER,
                        eventTimestamp INTEGER
                        )''')


app = Flask(__name__)
api = Api(app)

api.add_resource(Benchmark, constants.BENCHMARK_ENDPOINT)
api.add_resource(Grader, constants.GRADER_ENDPOINT)

if __name__ == '__main__':
    createTables()
    app.run(host=constants.SERVER_HOST, port=constants.SERVER_PORT)
