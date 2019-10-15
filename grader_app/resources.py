from flask import session
from flask_restful import Resource
import os
import subprocess
import pandas as pd
import state

class Benchmark(Resource):

    def get(self):
        print(f'Requested tuples {state.tupleIndex} - {state.tupleIndex + state.BATCH_SIZE - 1}')
        sc = state.inputDf.get_chunk(state.BATCH_SIZE)
        state.tupleIndex += state.BATCH_SIZE
        print(sc.head(2))
        print('...')
        print(sc.tail(2))
        return {'data': sc.to_json(orient='records')}

