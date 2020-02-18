import os

DATASET_PATH = os.getenv('DATASET_PATH', '../dataset')
DATABASE_NAME = 'gc.db'
DATA_TASK_ONE_ENDPOINT = '/data/1/'
DATA_TASK_TWO_ENDPOINT = '/data/2/'
GRADER_ENDPOINT_TASK_ONE = '/score/1/'
GRADER_ENDPOINT_TASK_TWO = '/score/2/'
GRADER_ENDPOINT_FINAL = '/score/all/'
RESULTS_EXPORTER_ENDPOINT = '/export/'
SERVER_PORT = 80
SERVER_HOST = '0.0.0.0'
INPUT_BATCH_SIZE = 1000
INPUT_FILE_TASK_ONE = f'{DATASET_PATH}/in1.csv'
INPUT_FILE_TASK_TWO = f'{DATASET_PATH}/in2.csv'
OUTPUT_FILE = f'{DATASET_PATH}/out.csv'
RESULT_FILE = os.getenv('RESULTS_PATH', '.') + '/' + 'results.json'
TASK_ONE_ID = 1
TASK_TWO_ID = 2
timeout_wait_seconds = int(os.getenv("HARD_TIMEOUT_SECONDS", default=6000))