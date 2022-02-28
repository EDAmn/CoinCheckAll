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


# split a multivariate sequence into samples
def split_sequences(sequences, n_steps):
	X, y = list(), list()
	for i in range(len(sequences)):
		# find the end of this pattern
		end_ix = i + n_steps
		# check if we are beyond the dataset
		if end_ix > len(sequences):
			break
		# gather input and output parts of the pattern
		seq_x, seq_y = sequences[i:end_ix, :-1], sequences[end_ix-1, -1]
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

        # define input sequence
        DataArrCount = DataDF["countQty"].to_numpy()
        DataArrDelta = DataDF["deltaPerTF"].to_numpy()
        DataArrPrice = DataDF["closePrice"].to_numpy()

        print(DataArrCount[:10])
        print(DataArrDelta[:10])
        print(DataArrPrice[:10])
        # convert to [rows, columns] structure
        DataArrCount = DataArrCount.reshape((len(DataArrCount), 1))
        DataArrDelta = DataArrDelta.reshape((len(DataArrDelta), 1))
        DataArrPrice = DataArrPrice.reshape((len(DataArrPrice), 1))

        # horizontally stack columns
        dataset = np.hstack((DataArrCount, DataArrDelta, DataArrPrice))

        # choose a number of time steps
        n_steps = 3
        # convert into input/output
        X, y = split_sequences(dataset, n_steps)

        #  summarize the data
        for i in range(len(X)):
            print(X[i], y[i])

        # separate input data
        X1 = X[:, :, 0]
        X2 = X[:, :, 1]

        print(X1)
        print(X2)

        # # first input model
        # visible1 = Input(shape=(n_steps,))
        # dense1 = Dense(100, activation='relu')(visible1)
        # # second input model
        # visible2 = Input(shape=(n_steps,))
        # dense2 = Dense(100, activation='relu')(visible2)
        # # merge input models
        # merge = concatenate([dense1, dense2])
        # output = Dense(1)(merge)
        # model = Model(inputs=[visible1, visible2], outputs=output)
        # model.compile(optimizer='adam', loss='mse')
        # # fit model
        # model.fit([X1, X2], y, epochs=2000, verbose=0)
        # # demonstrate prediction

        # x_input = array([[80, 85], [90, 95], [100, 105]])
        # x1 = x_input[:, 0].reshape((1, n_steps))
        # x2 = x_input[:, 1].reshape((1, n_steps))
        # yhat = model.predict([x1, x2], verbose=0)
        # print(yhat)

        # x_input = np.array([[3084, 477717], [2660, 379878], [2683, 379997]])
        # x_input = x_input.reshape((1, n_input))
        # yhat = model.predict(x_input, verbose=0)
        # print(yhat)
