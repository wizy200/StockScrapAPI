from bs4 import BeautifulSoup
import pymongo
import json
import datetime
from dateutil.parser import parser
from flask import Flask

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
@app.route('/insertStocksData')
def insertStocksData():
    #open the json file at C:\Data\TickersData.json 
    with open("C:\Data\Tickers_2020-03-06.json") as json_file:
            data = json.load(json_file)
            print(stockCol.insert_many(data))


#CRUD Methods.
#getStockByDate
@app.route("/getStockByDate")
def getStockByDate(ticker, date):
    stockCol.find({"Ticker": ticker,"Date": date})

#getStockByDateRange
@app.route("/getStockByDateRange")
def getStockByDateRange(ticker,start,end):
    stock = stockCol.find({"Ticker":ticker})
    return(stock)
#NOTE: Have bill break out the data information into day month year fields.
def getStockOneYear(ticker, year):
    pass


#Test API call
@app.route("/Test")
def Test():
    return "We did it"


getStockByDateRange("CSCO",2019,2020)
