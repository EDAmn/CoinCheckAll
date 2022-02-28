import numpy as np
import pymongo
from pymongo import MongoClient, UpdateOne
import pandas as pd 
import sys
from keras.models import Sequential
from keras.layers import Dense

import dbFunc
import calc
import json


def split_sequence(sequence, n_steps):
	X, y = list(), list()
	for i in range(len(sequence)):
		# find the end of this pattern
		end_ix = i + n_steps
		# check if we are beyond the sequence
		if end_ix > len(sequence)-1:
			break
		# gather input and output parts of the pattern
		seq_x, seq_y = sequence[i:end_ix], sequence[end_ix]
		X.append(seq_x)
		y.append(seq_y)
	return np.array(X), np.array(y)


# client = MongoClient("mongodb://localhost:27017")
client = MongoClient("mongodb+srv://analytics:analytics-password@mflix.i8krg.mongodb.net")
sourceLst = ["CoinMonitor"]
timeFrame = int(5)
# investorTypeLst = ["BIG", "RETAIL"]
investorTypeLst = ["RETAIL"]

for source in sourceLst:

    db = client[source]
    for investorType in investorTypeLst:
        querryForData =  {
            "timeFrame": timeFrame,
            "investorType": investorType
            }
        
        DataDF = pd.DataFrame(list(db.result.find(querryForData)))
        # DataDFLim = DataDF[["timeFrameStart", "closePrice"]]

        # define univariate time series
        DataArr = DataDF["closePrice"].to_numpy()
        print(DataArr.shape)
        # transform to a supervised learning problem
        n_steps = 3
        X, y = split_sequence(DataArr, n_steps)
        print(X.shape, y.shape)
        # # transform input from [samples, features] to [samples, timesteps, features]
        # X = X.reshape((X.shape[0], X.shape[1], 1))
        # print(X.shape)
        # show each sample
        for i in range(len(X)):
            print(X[i], y[i])

        # samples = list()
        # length = 200
        # n=len(DataArr)
        # # step over the 5,000 in jumps of 200
        # for i in range(0,n,length):
        #     # grab from i to i + 200
        #     sample = DataArr[i:i+length]
        #     samples.append(sample)
        # print(len(samples))

        #-------------------MLP model
        model = Sequential()
        model.add(Dense(100, activation='relu', input_dim=n_steps))
        model.add(Dense(1))
        model.compile(optimizer='adam', loss='mse')
        # fit model
        model.fit(X, y, epochs=2000, verbose=0)
        # demonstrate prediction
        x_input = np.array([57950.01, 58011.63, 57968.31])
        x_input = x_input.reshape((1, n_steps))
        yhat = model.predict(x_input, verbose=0)
        print(yhat)