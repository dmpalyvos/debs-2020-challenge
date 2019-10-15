
from flask import Flask
from flask_restful import Api

from resources import Benchmark


app = Flask(__name__)
api = Api(app)

api.add_resource(Benchmark, '/data/')

if __name__ == '__main__':
    app.secret_key = 'A0Zr98j/3yX R~XHH!jmN]LWX/,?RT'
    app.run(host='0.0.0.0', port=80)