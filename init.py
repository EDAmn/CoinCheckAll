import pymongo
from pymongo import MongoClient, UpdateOne
import pandas as pd 
import sys
import numpy as np
from bson.objectid import ObjectId

import dbFunc
import calc
import json

sourceLst = ["CoinMonitor"]

timeFrameList = [int(5), int(30), int(60), int(240), int(1440)]
# timeFrameList = [int(240)]

# investorTypesList = ['RETAIL','BIG','INSTITUTION']
# investorTypesList = ['RETAIL','BIG']
# investorTypesList = ['RETAIL']
investorTypesList = ["small", "medium", "large", "huge"]
binsForInvestorType = [0, 100, 1000, 10000, 100000000000000]

buySellDummyList = ['BUY','SELL']

movingAvgWindowsList = [6,12,24,48]

limitNo = 10000


# client = MongoClient("mongodb://localhost:27017")
client = MongoClient("mongodb+srv://analytics:analytics-password@mflix.i8krg.mongodb.net")


for source in sourceLst:

    db = client[source]

    querryEmpty = {"status": {"$exists": False}}
    # querryEmpty = {"status": "InProgress"}
    statusLoaded = "Loaded"

    dbFunc.tas_lable(db.tas.find(querryEmpty).limit(limitNo),statusLoaded,db.tas)
    # dbFunc.tas_lable(db.tas.find(querryEmpty),statusLoaded,db.tas)
    

    querryLoaded = {"status": statusLoaded}
    statusInProgress = "InProgress"
    
    # print(list(db.tas.find(querryLoaded))[:10])
    DataDF = dbFunc.read_mongo(db.tas.find(querryLoaded))
    
    if DataDF.empty:

         sys.stdout.write("No new data to process")

    else:
        #--------ODSUD PREPSAT DO FUNKCE--------
        dbFunc.tas_lable(db.tas.find(querryLoaded),statusInProgress,db.tas)

        querryProgress = {"status": statusInProgress}

        #Provede zálohu do tabule tas_backup2 (potřeba přepsat do proměnné)
        statusBackedUp = "BackedUp"

        db.tas.aggregate([
            { "$match": querryProgress},
            { "$merge": {
                "into": "tas_backup",
                "on": "_id" }}
        ])

        #označí zálohované statusem = "BackedUp"
        dbFunc.tas_lable(db.tas.find(querryProgress),statusBackedUp,db.tas)

        #smaže všechny zálohované
        querryBU = {"status": statusBackedUp}
        db.tas.delete_many({ "status" : statusBackedUp})
        #------ POSEM PREPSAT-------

        # # DEPRECATED
        # # Assign Investor Type based on qty
        # DataDF['investorType'] = DataDF['v'].apply(lambda x: calc.assign_investor_type(x))
        

        # Assign Investor Type based on nominal as product of volume and price
        # TODO - last bin value should be np.inf() - vyhazuje err na float not calleble - doresit

        DataDF['nominalValue'] = DataDF.apply(lambda row: row["v"]*row["p"], axis=1)
        DataDF['investorType'] = pd.cut(
            DataDF['nominalValue'],
            bins = binsForInvestorType,
            labels = investorTypesList
        )
        
        # Kontrola DataFrame, uložení na disk pro manuální přepočet - TODO delete před Mergem
        print(DataDF)
        storeCSVPath = (r"C:\Users\Administrator\Desktop\PythonScripts\BTC_Analysis\DataOut\data.csv")
        DataDF.to_csv(storeCSVPath, encoding='utf-8', index=False)

        # Assign TimeFrame (step of frame specified in TimeFrame PARAM in minutes)
        for timeFrame in timeFrameList:
            
            binsForTimeSeries, labelsForTimeSeries = calc.time_series_labels(DataDF, timeFrame)
            
            DataDF['timeFrameStart'] = pd.cut(
                DataDF[['time']].time,
                bins = binsForTimeSeries,
                labels = labelsForTimeSeries
            )
          
            # Creates new DF with calculated pd.DataFrame(columns=["timeFrameStart", "meanQty","countQty","openPrice","closePrice","volumeTraded","deltaPerTF"])

            statisticsDF, exStatDFNested, invStatDFNested = calc.statistics_per_specified_group(labelsForTimeSeries, DataDF,investorTypesList, timeFrame)

            print("ZAKLAD:")    
            print(statisticsDF)
            print("BURZY:")
            print(exStatDFNested)
            print("INVESTOR:")
            print(invStatDFNested)
                # querryCalculationsMissing = {"calculations": {"$exists": False}}
                # missingCalculationsObject = db.result.find(querryCalculationsMissing).distinct("_id")
                
                # calculationsDF = calc.calculations_per_specified_group(db.result.find(querryCalculationsMissing))
                # print(calculationsDF)



                # print("Displaying results for investor type: " + investorType + " and TimeFrame: " + str(timeFrame))
                # print(InvestorSpecificDF)
               
            dbFunc.insert_results(db,"result",statisticsDF, exStatDFNested, invStatDFNested, timeFrame, investorTypesList)
        
        ### ----------- ODSUD KONTROLA!!!!!!!!!!!! ------- 

        # querryCalculationsMissing = {"calculations": {"$exists": False}}
        # projectionCalculationsMissing = {"_id": 1}

        # missingCalculationsObject = db.result.find(querryCalculationsMissing, projectionCalculationsMissing).limit(10)
        # # calculationsList = list(missingCalculationsObject)
        # # calculationsList = calc.calculations_per_specified_group(db.result.find(querryCalculationsMissing).distinct("_id"))
        # calculationsList = list(db.result.find(querryCalculationsMissing).distinct("_id"))
        # calculationsList = calculationsList[-10:]

        # if calculationsList:

        #     for item in calculationsList:
                
        #         print("item " + str(item))
        #         querryTF = {
        #             "_id": ObjectId(str(item))
        #         }
        #         projectionTF = {
        #             "_id": 0,
        #             "timeFrameStart": 1,
        #             "timeFrame": 1,
        #             "investorType": 1,
        #             "rationQtySumTF": 1,
        #             "ratioQtyCountTF": 1
        #         }
        #         objectFind = db.result.find(querryTF, projectionTF)
        #         findList = list(objectFind)

        #         movingAvgTF = dbFunc.moving_average_per_TF(findList, db, movingAvgWindowsList)
        #         for row in movingAvgTF:

        #             size = movingAvgTF["windowBack"].iloc[0]
        #             newData = movingAvgTF[["meanCount","meanSum"]]

        #             for item2 in newData.apply(lambda x: x.to_dict(), axis=1).to_list():
        #                 print(item2)
        #                 db.result.update(
        #                     {
        #                         "_id": ObjectId(str(item))
        #                     },
        #                     {
        #                         "$set": {
        #                             "calculations": {
        #                                 str(size): item2
        #                             }
        #                             # "hovno": {"pokus":1}
        #                         }
        #                     }
        #                 )
        #         print(movingAvgTF)      


sys.stdout.write("Python done. New transactions processed: " + str(len(DataDF)) + ". Zdar a silu soudruzi!")
sys.stdout.flush()
