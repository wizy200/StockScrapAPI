from bs4 import BeautifulSoup
import pymongo
import json
import datetime
from dateutil.parser import parser
from datetime import datetime
from flask import Flask, jsonify
from bson import json_util, ObjectId
from bson.json_util import dumps

app = Flask(__name__)

#create mondo db connection
# mongodb://localhost:27017/?readPreference=primary&appname=MongoDB%20Compass%20Community&ssl=false
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
stockDB = dbClient[getStockDbName()]
stockCol = stockDB[getStockColName()]

@app.route('/insertStocksData/<fileName>')
def insertStocksData(fileName):
    #open the json file at C:\Data\TickersData.json 
    with open("C:\Data\\" + fileName +".json") as json_file:
            data = json.load(json_file)
            status = stockCol.insert_many(data)
            #replace this with status codes
            return "Attempted to insert records"

#CRUD Methods.
#getStockByDate
@app.route("/getStockByDate")
def getStockByDate(ticker, date):
    stockCol.find({"Ticker": ticker,"Date": date})

#getStockByDateRange
@app.route("/getStockByDateRange/<ticker>/<start>/<end>")
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
    #use the mongo json deserializer since the built in one does not property serialize the UUIDs
    stockJson = json.loads(json_util.dumps(stockList))
    return jsonify(stockJson)

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

#Test API call
@app.route("/Test")
def Test():
    return "We did it"

