import os
import subprocess
import pandas as pd
import sqlite3
import constants
import csv


class BenchmarkInputTaskOne:

    def __init__(self, inputFile, chunkSize):
        self.__inputDf = self.readInput(inputFile)
        self.chunkSize = chunkSize
        self.__nextBatchIndex = 0

    def getChunk(self, minus=0):
        return self.__inputDf.get_chunk(self.chunkSize - minus)

    def nextSendTupleIndexes(self):
        firstIndex = self.__nextBatchIndex * self.chunkSize
        lastIndex = (self.__nextBatchIndex * self.chunkSize) + \
            self.chunkSize - 1
        return (firstIndex, lastIndex)

    def currentBatchIndex(self):
        return self.__nextBatchIndex

    def batchSent(self):
        self.__nextBatchIndex += 1

    def readInput(self, inputFile):
        if not os.path.isfile(inputFile):
            print(
                f"{inputFile} not found. Please put datafiles in the /dataset folder")
            exit(1)
        self.verifyInputHasNoHeader(inputFile)
        return pd.read_csv(inputFile, sep=',', names=['idx', 'voltage', 'current'], header=None, iterator=True)

    def verifyInputHasNoHeader(self, inputFile):
        with open(inputFile) as csvfile:
            if csv.Sniffer().has_header(csvfile.read(2048)):
                raise ValueError('The input file should not have a header!')


class BenchmarkInputTaskTwo(BenchmarkInputTaskOne):

    def __init__(self, inputFile, chunkSize):
        super(BenchmarkInputTaskTwo, self).__init__(inputFile, chunkSize)
        self.delayed = pd.DataFrame()


    def getChunk(self):
        (_, lastTupleIndex) = super().nextSendTupleIndexes()
        chunk = super().getChunk(len(self.delayed))
        availableData = pd.concat([self.delayed, chunk], sort=False, ignore_index=True)
        sendPortion = availableData
        keepPortion = pd.DataFrame()
        for index, row in availableData.iterrows():
            if row['idx'] > lastTupleIndex:
                print(f'Found delayed tuples after tuple #{index}')
                sendPortion = availableData[:index]
                keepPortion = availableData[index:]
                break
        self.delayed = keepPortion 
        print(f'Sending {len(sendPortion)} tuples')
        return sendPortion


TASK_ONE = BenchmarkInputTaskOne(constants.INPUT_FILE_TASK_ONE, constants.INPUT_BATCH_SIZE)
TASK_TWO = BenchmarkInputTaskTwo(constants.INPUT_FILE_TASK_TWO, constants.INPUT_BATCH_SIZE)
