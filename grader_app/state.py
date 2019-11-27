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
                raise ValueError(f'The input file should not have a header: {inputFile}')


class BenchmarkInputTaskTwo(BenchmarkInputTaskOne):

    def __init__(self, inputFile, chunkSize):
        super(BenchmarkInputTaskTwo, self).__init__(inputFile, chunkSize)
        self.futureTuples = pd.DataFrame()
        self.maxAvailableIndex = 0


    def getChunk(self):
        (_, lastTupleIndex) = super().nextSendTupleIndexes()
        chunk = super().getChunk(len(self.futureTuples))
        self.updateMaxAvailableIndex(chunk)
        availableData = pd.concat([self.futureTuples, chunk], sort=False, ignore_index=True)
        # In case the batch read contains previously delayed tuples, we might need to read more than a chunk
        # to fill the whole batch with data
        while self.maxAvailableIndex < lastTupleIndex:
            extraChunk = super().getChunk()
            self.updateMaxAvailableIndex(extraChunk)
            availableData = pd.concat([availableData, extraChunk], sort=False, ignore_index=True)
        # The input is potentially split into two pieces
        # The batch, i.e., the tuples belonging to the current batch round 
        # The kept, i.e., the tuples that need to be sent later because their index is too high
        # Check if there is a portion of the dataframe 
        firstRowOutsideBatch = (availableData.idx > lastTupleIndex).idxmax()
        if firstRowOutsideBatch:
            # If there are tuples outside this batch, split the input
            batchTuples = availableData[:firstRowOutsideBatch]
            self.futureTuples = availableData[firstRowOutsideBatch:]
        else:
            # If no tuples outside this batch, send all available data
            batchTuples = availableData
            self.futureTuples = pd.DataFrame()
        print(f'Sending {len(batchTuples)} tuples')
        return batchTuples
    
    def updateMaxAvailableIndex(self, df):
        self.maxAvailableIndex = max(self.maxAvailableIndex, df.idx.max()) 
        


TASK_ONE = BenchmarkInputTaskOne(constants.INPUT_FILE_TASK_ONE, constants.INPUT_BATCH_SIZE)
TASK_TWO = BenchmarkInputTaskTwo(constants.INPUT_FILE_TASK_TWO, constants.INPUT_BATCH_SIZE)
