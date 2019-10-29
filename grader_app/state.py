import os
import subprocess
import pandas as pd
import sqlite3
import constants

def readInput(inputFile):
    if not os.path.isfile(inputFile):
        print(f"{inputFile} not found. Please put datafiles in the /dataset folder")
        #raise FileNotFoundError()
        exit(1)
    return pd.read_csv(inputFile, sep=',', names=['idx', 'voltage', 'current'], iterator=True)

inputDf = readInput(constants.INPUT_FILE)
nextSendIndex = 0
nextReceiveIndex = 0
