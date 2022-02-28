import pymongo
from pymongo import MongoClient, UpdateOne
import pandas as pd 
import sys
import json

import dbFunc

client = MongoClient("mongodb+srv://analytics:analytics-password@mflix.i8krg.mongodb.net")
db = client["CoinMonitor"]


# What will be coming

# *1) pair:*
# - btcusd

# *2) timeframe:*
# - 1440, 240, 60, 30, 5

# *3) type:*
# - candle, total, small, medium, large, huge

# for line in sys.stdin:
#     timeFrame = int(line)

timeFrame = int(1440)
pair = "btcusd"
numberOfRecords = 100
investorType = "small"
dataType = ["candle", "total", "small", "medium", "large", "huge"]

# for line in sys.stdin:
#     print(line)

dataForPlotDF = dbFunc.serve_data_for_plot(db, "result", timeFrame,investorType, numberOfRecords)

timeFrameList = []
timeFrameList.append("time")
timeFrameList.extend(dataForPlotDF["timeFrameStart"].tolist())

priceCloseList = []
priceCloseList.append("close")
priceCloseList.extend(dataForPlotDF["closePrice"].tolist())

priceOpenList = []
priceOpenList.append("open")
priceOpenList.extend(dataForPlotDF["openPrice"].tolist())

# deltaList = []
# deltaList.append("Delta")
# deltaList.extend(dataForPlotDF["deltaPerTF"].tolist())

finalListToPlot = []
finalListToPlot.append(timeFrameList)
finalListToPlot.append(priceCloseList)
finalListToPlot.append(priceOpenList)
# finalListToPlot.append(deltaList)

finalListToPlotJson = json.dumps(finalListToPlot)

sys.stdout.write(finalListToPlotJson)
sys.stdout.flush()