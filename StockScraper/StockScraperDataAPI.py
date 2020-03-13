from flask import Flask, jsonify
import pymongo
import json
import datetime
import os
from datetime import datetime
from bson import json_util
from bson.json_util import dumps
import matplotlib.pyplot as plt
from matplotlib import dates

app = Flask(__name__)

#create mondo db connection
# mongodb://localhost:27017/?readPreference=primary&appname=MongoDB%20Compass%20Community&ssl=false
with app.app_context():
    dbClient = pymongo.MongoClient("mongodb://localhost:27017/?readPreference=primary&appname=MongoDB%20Compass%20Community&ssl=false")
    configDB = dbClient["Configurations"]
    configCol = configDB["StockConfig"]

#get the Stocks Database name
def getStockDbName():
    dbName = configCol.find_one()["StockDbName"]
    print("Stock Database is set to: " +dbName)
    return dbName

#get the Stocks Column name
def getStockColName():
    colName = configCol.find_one()["StockColName"]    
    print("Stock column is set to: " +colName)
    return colName

#Initialize the stockDB connections
with app.app_context():
    stockDB = dbClient[getStockDbName()]
    stockCol = stockDB[getStockColName()]

#Insert a single file to the database
@app.route('/insertStocksData/<fileName>')
def insertStocksData(fileName):
    #open the json file at C:\Data\TickersData.json 
    with open("C:\Data\\" + fileName +".json") as json_file:
        data = json.load(json_file)
        status = stockCol.insert_many(data)
        #replace this with status codes
        return "Attempted to insert records"
#Loop through a directory and load all files to the database.
@app.route('/insertStocksDataWithDirectory/<directory>')
def insertStocksDataWithDirectory(directory):
    for subdir, dirs, files in os.walk(directory):
        for file in files:
            with open(directory +"\\"+ file) as json_file:
                data = json.load(json_file)
                status = stockCol.insert_many(data)
    return "it might have worked"

        
#CRUD Methods.
#getStockByDate
@app.route("/getStockByDate")
def getStockByDate(ticker, date):
    stockCol.find({"Ticker": ticker,"Date": date})

#getStockByDateRange
@app.route("/getStockJsonByDateRange/<ticker>/<start>/<end>")
def getStockJsonByDateRange(ticker ,start ,end ):
    startDate = datetime.strptime(start,"%Y-%m-%d")
    endDate = datetime.strptime(end,"%Y-%m-%d")
    stock = stockCol.find({"Ticker":ticker})
    #create a blank list
    stockList = []
    #for each stock add its information to the list
    for x in stock:
        #if the stocks date falls between the start and end search date
        if datetime.strptime(x["Date"],"%Y-%m-%d") >= startDate and datetime.strptime(x["Date"],"%Y-%m-%d") <= endDate:            
            stockList.append(x)
    #use the mongo json deserializer since the built in one does not property serialize the UUIDs
    stockJson = json.loads(json_util.dumps(stockList))
    return jsonify(stockJson)

def getStockByDateRange(ticker ,start ,end ):
    startDate = datetime.strptime(start,"%Y-%m-%d")
    endDate = datetime.strptime(end,"%Y-%m-%d")
    stock = stockCol.find({"Ticker":ticker})
    #create a blank list
    stockList = []
    #for each stock add its information to the list
    for x in stock:
        #if the stocks date falls between the start and end search date
        if datetime.strptime(x["Date"],"%Y-%m-%d") >= startDate and datetime.strptime(x["Date"],"%Y-%m-%d") <= endDate:            
            stockList.append(x)
    return stockList

#getDayPopulated
@app.route("/getDayPopulated/<date>")
def getDayPopulated(date):  
    #Grab the first stock for a day and check if the result is null
    result = stockCol.find_one({"Date": date})
    if result is None:
        return "False"
    else:
        return "True"

def getStockOneYear(ticker, year):
    pass

#Analytics Sections

#getAverages
def getAveragePrice(dataList):
    totalPrice = 0
    count = 0
    for x in dataList:
        count +=1
        totalPrice += float(x["Close"])
    return totalPrice/count

def getAverageVolume(dataList):
    totalVolume = 0
    count = 0
    for x in dataList:
        count +=1
        totalVolume += int(x["Volume"].replace(',',''))
    return totalVolume/count

#find days with volume difference by %
def getDaysWithVolumePercentage(dataList, difference):
    days = []
    averageVolume = getAverageVolume(dataList)
    print("calculated average volume: " + str(averageVolume))
    for x in dataList:
        fVol = float(x["Volume"].replace(',',''))
        volDiff =  fVol - averageVolume
        avgVolDif = abs(volDiff)/averageVolume * 100
        if  avgVolDif > difference:
            days.append(x)
    return days

#find days with price distance by %
def getDaysWithPricePercentage(dataList, difference):
    days = []
    averagePrice = getAveragePrice(dataList)
    print("calcuated average price: "+ str(averagePrice))
    for x in dataList:
        fPrice = float(float(x["Close"]))
        volDiff =  fPrice- averagePrice
        avgPriceDif = abs(volDiff)/averagePrice *100        
        if avgPriceDif > difference:
            days.append(x)
    return days

#determine the volume trend in specified days against the days passed.
def getVolumeTrend(days, start, end):
    startDate = datetime.strptime(start,"%Y-%m-%d")
    endDate = datetime.strptime(end,"%Y-%m-%d")

    pass

#determine price the trend in specified days against the days passed.
def getPriceTrend(days, start, end):
    startDate = datetime.strptime(start,"%Y-%m-%d")
    endDate = datetime.strptime(end,"%Y-%m-%d")
    pass

#figure out how to determine steps in stocks and volume
#find all series of days where the volume is increasing and the percentage they increase each day
def findTrendsClose(days, step):
    #compare the day against the previous day if the difference is bigger than the step percentage add it to the list
    #this should find positive and negative trends
    positiveTrends = []
    negativeTrends = []
    inPositiveTrend = False
    inNegativeTrend = False
    #only true for the first day
    start = True
    for day in days:
        if day["Date"] == "2019-09-10":
            print(day["Date"])
        if day is not None:
            #break the results out into their own seperate dicts or lists
            if start == False:
                avgDif = float(day["Close"])/float(previousDay["Close"])
                if(avgDif > 1):
                    #test for positive trend
                    d = (avgDif - 1) * 100
                    if d >= step:
                        positiveTrends.append(previousDay)
                        if inPositiveTrend == False:
                            positiveTrends.append(day)
                else:
                    #test for negative trend
                    d = (1 - avgDif) * 100 
                    if d >= step:                        
                        negativeTrends.append(previousDay)
                        if inNegativeTrend == False:
                            negativeTrends.append(day)
        start = False
        previousDay = day
        #create a dictionary to return both values
    retDict = dict()
    retDict['positiveTrends'] = positiveTrends
    retDict['negativeTrends'] = negativeTrends
    return retDict

#find the highClose in a list of stocks
def findHighClose(days):
    high = 0
    for day in days:
        if float(day["Close"]) > high:
            high = float(day["Close"])
    return high

#find the lowClose in a list of stocks
def findLowClose(days):
    low = float(days[0]["Close"])
    for day in days:
        if float(day["Close"]) < low:
            low = float(day["Close"])
    return low

#find the lowVolume in a list of stocks
def findLowVolume(days):
    low = float(days[0]["Volume"])
    for day in days:
        if float(day["Volume"]) < low:
            low = float(day["Volume"])
    return low


#find the highVolume in a list of stocks
def findHighVolume(days):
    high = float(days[0]["Volume"])
    for day in days:
        if float(day["Volume"]) < high:
            high = float(day["Volume"])
    return high

#graphing utilities

#plot stock prices
def plotStockPrices(stocks):
    days = []
    prices = []
    formatter = dates.DateFormatter('%Y-%m-%d')
    for stock in stocks:
        #Create a list of stock prices
        prices.append(float(stock["Close"]))
        #Create a list of days
        days.append(stock["Date"])
    plt.style.use("ggplot")
    plt.xlabel("Dates")
    plt.ylabel("Close")
    plt.title("Stock Analysis")
    plt.plot(prices)
    plt.show()
    return prices


#Test API call
@app.route("/Test")
def Test():
    stocks = getStockByDateRange("T","2020-01-01","2020-12-31")
    #The trends are inserting duplicate records into the list. Not sure why.
    trendTest = findTrendsClose(stocks, 2)
    highVolumeTest = findHighVolume(stocks)
    lowVolumeTest = findLowVolume(stocks)
    highCloseTest = findHighClose(stocks)
    lowCloseTest = findLowClose(stocks)
    plotStockPricesTest = plotStockPrices(stocks)
    #test1 = getDaysWithPricePercentage(stocks,2)   
    #test2 = getDaysWithVolumePercentage(stocks,1)
    return "We did it"


Test()


