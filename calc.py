import json
import pandas as pd
import datetime
import numpy as np

def time_series_labels(df, timeFrame):
    
    FirstTSEpoch = float((df['time'].iloc[0])/1000)
    LastTSEpoch = float((df['time'].iloc[-1])/1000)

    FirstTSDT = datetime.datetime.fromtimestamp(FirstTSEpoch)

    if timeFrame < int(60):
        lastFrameDateTime = FirstTSDT.replace(microsecond=0, second=0, minute=0)
    else:
        lastFrameDateTime = FirstTSDT.replace(microsecond=0, second=0, minute=0, hour=0)
    
    lastFrameDateTimeEpoch = float(datetime.datetime.timestamp(lastFrameDateTime))

    TimeFrameArr = np.arange(lastFrameDateTimeEpoch*1000, LastTSEpoch*1000, timeFrame*60000)
    
    array_length = len(TimeFrameArr)
    last_element = TimeFrameArr[array_length - 1]
    TimeFrameArrPlus = np.append(TimeFrameArr, last_element + timeFrame*60000)

    LableArr = []
    for i in TimeFrameArr:
        Lable = (datetime.datetime.fromtimestamp(float(i/1000)).strftime('%Y-%m-%d %H:%M:%S'))
        LableArr.append (Lable)
    
    # print(timeFrame)
    # print(TimeFrameArrPlus)
    # print(LableArr)
    
    return TimeFrameArrPlus, LableArr

def buy_sell_assign(isBuyerMaker):
    if isBuyerMaker:
        return 'SELL'
    else:
        return 'BUY'
def calculations_per_specified_group(missingCalculationsObject):

    missingCalculationsList = list(missingCalculationsObject)

   

        # missingCalculationsDF = pd.DataFrame(missingCalculationsList)
        # print(missingCalculationsDF)

    return missingCalculationsList

def statistics_per_specified_group(DatesList,DF,investorTypesList,timeFrame):
    
    # FilteredDF = DF.loc[(DF['investorType'] == investorType)] - DEPRECATED - was used, when the TF was constructed per InvestorGroup
    FilteredDF = DF #Not needed - for the possibility to revert back to InvestorGroup

    statDF = pd.DataFrame()
    statDFNested = pd.DataFrame()
    investorStatDF = pd.DataFrame()
    investorStatDFNested = pd.DataFrame()
    
    for dateRow in DatesList:
        DateFilteredDF = FilteredDF.loc[FilteredDF['timeFrameStart'] == dateRow]

        if len(DateFilteredDF) == 0:
            avgQty = 0
            cntQty = 0
            beginPrice = 0
            endPrice = 0
            totalVolume = 0
            ratioSumTF = 0
            ratioCountTF = 0
            highPrice = 0
            lowPrice = 0

            investorCntQty = 0
            investorAvgQty = 0

            countPerExchange = pd.Series()
            totalCountExchange = 0
            # countPerExchange = pd.Series(np.array([0,0,0,0]),index = ["binance","kraken","huobi","coinbase"])
        else:
            avgQty = DateFilteredDF['v'].mean()
            cntQty = DateFilteredDF['v'].count()
            totalVolume = DateFilteredDF['quoteQty'].sum()
            sumQtyBuy = DateFilteredDF.loc[DateFilteredDF["o"] == "b"]["quoteQty"].sum()
            sumQtySell = DateFilteredDF.loc[DateFilteredDF["o"] == "s"]["quoteQty"].sum()
            countQtyBuy = DateFilteredDF.loc[DateFilteredDF["o"] == "b"]["quoteQty"].count()
            countQtySell = DateFilteredDF.loc[DateFilteredDF["o"] == "s"]["quoteQty"].count()

            sortedDateFilteredDF = DateFilteredDF.sort_values('time')
            
            beginPrice = sortedDateFilteredDF['p'].iloc[0]
            endPrice = sortedDateFilteredDF['p'].iloc[-1]
            highPrice = sortedDateFilteredDF['p'].max()
            lowPrice = sortedDateFilteredDF['p'].min()
            
            ratioSumTF = sumQtyBuy/(sumQtySell + sumQtyBuy)
            ratioCountTF = countQtyBuy/(countQtySell + countQtyBuy)


            countPerExchange = DateFilteredDF.e.value_counts(normalize=True)
            totalCountExchange = DateFilteredDF.e.value_counts().sum()

            for investorType in investorTypesList:

                DateFilteredDFInvestor = FilteredDF.loc[FilteredDF['investorType'] == investorType]

                investorCntQty = DateFilteredDFInvestor['v'].count()
                investorAvgQty = DateFilteredDFInvestor['v'].mean()

                investorStatDF = investorStatDF.append({
                    "timeFrameStart": dateRow,
                    "investorCountQty": investorCntQty,
                    "investorMeanQty": investorAvgQty,
                    "investorType": investorType
                    }, ignore_index=True)
                
                print(investorStatDF)
            # #DOPSAT TEST - sum dole se rovná countQty
            # DateFilteredDF.e.value_counts().sum()

        # TODO Dát do dbFunc - připravit na různé vkládání /nested, /několik najednou, /update??? 
        statDF = statDF.append({
            "timeFrameStart": dateRow,
            "meanQty": avgQty,
            "countQty":  cntQty,
            "openPrice": beginPrice,
            "closePrice": endPrice,
            "highPrice": highPrice,
            "lowPrice": lowPrice,
            "volumeTraded": totalVolume,
            "ratioQtySumTF":  ratioSumTF,
            "ratioQtyCountTF": ratioCountTF,
            "timeFrame": timeFrame
            }, ignore_index=True)

        statDFNested = statDFNested.append({
            "timeFrameStart" : dateRow,
            "binancePct": return_exchange_pct(countPerExchange, "binance"),
            "coinbasePct": return_exchange_pct(countPerExchange, "coinbase"),
            "krakenPct": return_exchange_pct(countPerExchange, "kraken"),
            "huobiPct":return_exchange_pct(countPerExchange, "huobi"),
            "totalCount": totalCountExchange
        }, ignore_index=True)

    # Chodí v DatesList o jeden méně pro jistotu, pokud je prázdný řádek pro tenhle TimeFrame - smaže se tady, funkce vrací jen nenulové timeframy
    statDF = statDF[statDF.countQty != 0]
    statDFNested = statDFNested[statDFNested.totalCount != 0]

    return statDF, statDFNested, investorStatDF

def return_exchange_pct(valueCount, exchange):
    try:
        pctValue = valueCount[exchange]
    except:
        pctValue = 0

    return pctValue

def ratio_addition (ratio1, ratio2, mean1, mean2):

    numberOfPositive1 = ratio1 * mean1

    numberOfPositive2 = ratio2 * mean2

    totalCount = sum([mean1, mean2])

    resultingRatio = sum([numberOfPositive1, numberOfPositive2])/totalCount

    return resultingRatio

def update_last_time_frame (investorTypesList, originalDF, newDF, exchangeDF, investorDF):

    # První uroveň dokumentu

    originalMean = originalDF["meanQty"].iloc[0]
    originalCount = originalDF["countQty"].iloc[0]
    
    originalRatioSum = originalDF["ratioQtySumTF"].iloc[0]
    originalRatioCount = originalDF["ratioQtyCountTF"].iloc[0]

    newMean = newDF["meanQty"].iloc[0]
    newCount = newDF["countQty"].iloc[0]
    
    newRatioSum = newDF["ratioQtySumTF"].iloc[0]
    newRatioCount = newDF["ratioQtyCountTF"].iloc[0]

    ratioSumNew = ratio_addition(originalRatioSum, newRatioSum, originalMean, newMean)

    ratioCountNew = ratio_addition(originalRatioCount, newRatioCount, originalCount, newCount)

    meanQtyNew = (
        (originalMean * originalCount)
        + (newMean * newCount)) / (originalCount + newCount)

    countQtyNew = originalCount + newCount

    closePriceNew = newDF["closePrice"].iloc[0]
    volumeTradedNew = originalDF["volumeTraded"].iloc[0] + newDF["volumeTraded"].iloc[0]
    
    highPriceNew = max(newDF["highPrice"].iloc[0], originalDF["highPrice"].iloc[0])
    lowPriceNew = min(newDF["lowPrice"].iloc[0], originalDF["lowPrice"].iloc[0])

    # přepočet statistik pro investor groupy
    fields_to_set_inv = {}

    for investorType in investorTypesList:
        invOriginalCount = originalDF[investorType+".investorCountQty"].iloc[0]
        invOriginalMean = originalDF[investorType+".investorMeanQty"].iloc[0]

        invNewCount = investorDF.loc[investorDF["investorType"] == investorType]["investorCountQty"].iloc[0]
        invNewMean = investorDF.loc[investorDF["investorType"] == investorType]["investorMeanQty"].iloc[0]
        
        investorCountQtyNew = invOriginalCount + invNewCount
        investorMeanQtyNew = ratio_addition(invOriginalMean, invNewMean, invOriginalCount, invNewCount)

        fields_to_set_inv[investorType+".investorCountQty"] = float(investorCountQtyNew)
        fields_to_set_inv[investorType+".investorMeanQty"] = float(investorMeanQtyNew)

        # print("OriginalCount:" + str(invOriginalCount))
        # print("NewCount:" + str(invNewCount))
        # print("FinalCount:" + str(investorCountQtyNew))
        # print("FinalMean:" + str(investorMeanQtyNew))

    # příprava dictionary pro update 
    #   fields_to_set -> hlavní objekt,
    #   fields_to_set_inv -> statistiky per investor,
    #   fields_to_set_ex -> statistiky dostupnosti burz

    update_doc = {}
    fields_to_set = {}

    fields_to_set["meanQty"] = float(meanQtyNew)
    fields_to_set["countQty"] = float(countQtyNew)
    fields_to_set["closePrice"] = float(closePriceNew)
    fields_to_set["volumeTraded"] = float(volumeTradedNew)
    fields_to_set["ratioQtySumTF"] = float(ratioSumNew)
    fields_to_set["ratioQtyCountTF"] = float(ratioCountNew)
    fields_to_set["highPrice"] = float(highPriceNew)
    fields_to_set["lowPrice"] = float(lowPriceNew)

    final_fields_to_set = {**fields_to_set, **fields_to_set_inv} # merge dvou dict - s updatem na py 3.9 je nový zápis z = x|y

    update_doc["$set"] = final_fields_to_set

    # print("Original Mean" + str(originalMean))
    # print("New Mean" + str(newMean))
    # print("calculated Mean" + str(meanQtyNew))

    # print(fields_to_set)
    # print(fields_to_set_inv)
    # print(final_fields_to_set)

    return(update_doc)