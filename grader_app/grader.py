
from flask import Flask
from flask_restful import Api
import sqlite3
import state
import constants

from resources import Benchmark, Grader

app = Flask(__name__)
api = Api(app)

api.add_resource(Benchmark, constants.BENCHMARK_ENDPOINT)
api.add_resource(Grader, constants.GRADER_ENDPOINT)

if __name__ == '__main__':
    with sqlite3.connect(constants.DATABASE_NAME) as con:     
        cursor = con.cursor()
        cursor.execute('DELETE FROM sent')
        cursor.execute('DELETE FROM results')
    app.run(host=constants.SERVER_HOST, port=constants.SERVER_PORT)