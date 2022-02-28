import pymongo
from pymongo import MongoClient, UpdateOne
import pandas as pd
import sys
import calc
import numpy as np
from bson.objectid import ObjectId


def read_mongo(tradeListToLoad, no_id=True):
    """ Read from Mongo and Store into DataFrame """

    # Load to pd DF, drop mongo ObjectId "_id" and Status, new value "quoteQty" - product of price and quantity
    
    tradeListToLoadLst = list(tradeListToLoad)
    
    if not tradeListToLoadLst:

        df = pd.DataFrame()
        return df

    else:
        df = pd.DataFrame(tradeListToLoadLst)
        df = df.drop(['_id','status'],axis = 1)
        df = df.rename(columns={"T": "time"})
        df['quoteQty'] = df.apply(lambda row: row.v * row.p, axis = 1)

    return df


def tas_lable(tradeListToLable, statusNew, collection):
    
    batch_size = 100
    updates = []
    count = 0
    tradeListToLableLst = []

    tradeListToLableLst = list(tradeListToLable)

    if not tradeListToLableLst:
        
        sys.stdout.write("No new data to lable as: " + str(statusNew))

    else:
        
        for trade in tradeListToLableLst:
            
            fields_to_set = {}
            fields_to_set["status"] = str(statusNew)

            update_doc = {}
            
            if fields_to_set:
                update_doc["$set"] = fields_to_set

            updates.append(UpdateOne({"_id":trade["_id"]}, update_doc))
            
            count += 1
            
            if count == batch_size:
                collection.bulk_write(updates)
                updates = []
                count = 0

        if updates:
            collection.bulk_write(updates)
            updates = []
            count = 0

def insert_results (db,collName, dataFrameToInsert, exStatDFNested, invStatDFNested, timeFrame, investorTypesList):
    
    collForInsert = db[collName]

    if not dataFrameToInsert.empty:

        firstDateInDF = dataFrameToInsert["timeFrameStart"].iloc[0]

        filter = {
                "timeFrameStart" : firstDateInDF,
                "timeFrame": timeFrame
            }

        existingRecord = list(collForInsert.find(filter))
        
        if not existingRecord:
            
            collForInsert.insert_many(dataFrameToInsert.apply(lambda x: x.to_dict(), axis=1).to_list())
            
            for item in exStatDFNested.apply(lambda x: x.to_dict(), axis=1).to_list():
                
                firstDateExAvailability = item["timeFrameStart"]
                del item["timeFrameStart"]

                collForInsert.update(
                    {
                        "timeFrameStart": firstDateExAvailability,
                        "timeFrame":timeFrame
                    },
                    {
                        "$set": {
                            "exchangeAvailability": item
                        }
                    }
                )
            
            for investorType in investorTypesList:
                
                invDF = invStatDFNested.loc[(invStatDFNested['investorType'] == investorType)]

                for item in invDF.apply(lambda x: x.to_dict(), axis=1).to_list():

                    dateAvailability = item["timeFrameStart"]
                    del item["timeFrameStart"]
                    del item["investorType"]
                    
                    collForInsert.update(
                        {
                            "timeFrameStart": dateAvailability,
                            "timeFrame": timeFrame
                        },
                        {
                            "$set": {
                                investorType: item
                            }
                        }
                    )

        
        elif len(existingRecord) == 1:

            originalDF = pd.json_normalize(existingRecord)

            print("ORIGINAL:")
            print(originalDF)

            collForInsert.find_one_and_update(
                                    filter,
                                    calc.update_last_time_frame(investorTypesList, originalDF, dataFrameToInsert, exStatDFNested, invStatDFNested),
                                    upsert = False
                                    )

            # print(originalDF)
            # print(dataFrameToInsert)
            # print(firstDateInDF)
            # print(dataFrameToInsert.loc[[0]])
            # print(calc.update_last_time_frame(originalDF,dataFrameToInsert.loc[[0]]))
            # print(originalDfOId)

            dataFrameToInsert = dataFrameToInsert.iloc[1:]
            if len(dataFrameToInsert) >= 1:

                collForInsert.insert_many(dataFrameToInsert.apply(lambda x: x.to_dict(), axis=1).to_list())

        else:
            sys.stdout.write("Something went terribly wrong :(")

def serve_data_for_plot(db, collectionName, timeFrame, investorType, recordsCount):

    filters = {
        "investorType": investorType,
        "timeFrame": timeFrame
        }
    
    collection = db[collectionName]

    dataToPlotObject = collection.find(filters).sort("timeFrameStart", -1).limit(recordsCount)
    
    dataToPlotLst = list(dataToPlotObject)
    dataToPlotDF = pd.DataFrame(dataToPlotLst)
    
    return(dataToPlotDF)

def moving_average_per_TF(listTF, db, windowsList):
    
    # print("id " + str(idMongo))
    
    # filters = {
    #     "_id": ObjectId(str(idMongo))
    # }
    # timeFrameStartObject = db.collection.find(filters).distinct("timeFrameStart")
    # timeFrameStartList = list(timeFrameStartObject)

    DFToProcess = pd.DataFrame(listTF)
    timeFrameStart = DFToProcess["timeFrameStart"].iloc[0]
    timeFrame = DFToProcess["timeFrame"].iloc[0]
    investorType = DFToProcess["investorType"].iloc[0]

    print(timeFrame)
    querryTF = {
        "timeFrameStart": {"$lte":timeFrameStart},
        "timeFrame": timeFrame,
        "investorType": investorType
        # "timeFrame": DFToProcess["timeFrame"].iloc[0],
        # "investorType": DFToProcess["investorType"].iloc[0]
    }
    
    maxNumberOfWindows = max(windowsList)
    mongoObj = db.result.find(querryTF).sort("timeFrameStart", -1).limit(maxNumberOfWindows)
    historicalLowerDF = pd.DataFrame(list(mongoObj))
    
    meanValueDF = pd.DataFrame()

    print(historicalLowerDF)
    for size in windowsList:
        print(size)
        
        historicalDF = historicalLowerDF[["ratioQtySumTF", "ratioQtyCountTF"]].head(size)
        print(historicalDF)

        meanSumValue = historicalDF["ratioQtySumTF"].mean()
        meanCountValue = historicalDF["ratioQtyCountTF"].mean()

        meanValueDF = meanValueDF.append({
            "windowBack" : size,
            "meanSum": meanSumValue,
            "meanCount": meanCountValue
        }, ignore_index=True)
        
        print("VALUE X = " + str(size))
        print(meanValueDF)

    return meanValueDF