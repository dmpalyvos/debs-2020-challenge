
from flask import Flask
from flask_restful import Api
import sqlite3
import state

from resources import Benchmark


app = Flask(__name__)
api = Api(app)

api.add_resource(Benchmark, '/data/')

if __name__ == '__main__':
    con = sqlite3.connect(state.DB_NAME)
    with con:
        cursor = con.cursor()
        cursor.execute('DELETE FROM sent')
        cursor.execute('DELETE FROM results')
    app.run(host='0.0.0.0', port=80)