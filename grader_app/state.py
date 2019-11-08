import os
import subprocess
import pandas as pd
import sqlite3
import constants
import csv

def readInput(inputFile):
    if not os.path.isfile(inputFile):
        print(f"{inputFile} not found. Please put datafiles in the /dataset folder")
        #raise FileNotFoundError()
        exit(1)
    verifyInputHasNoHeader(inputFile)
    return pd.read_csv(inputFile, sep=',', names=['idx', 'voltage', 'current'], header=None, iterator=True)

def verifyInputHasNoHeader(inputFile):
    with open(inputFile) as csvfile:
        if csv.Sniffer().has_header(csvfile.read(2048)):
            raise ValueError('The input file should not have a header!')

inputDf = readInput(constants.INPUT_FILE)
nextSendIndex = 0
nextReceiveIndex = 0
