import streamlit as st

import pymongo
import dns.resolver
import time

from Functions import *

st.set_page_config(layout="wide")

st.header('Stock/PSX analysis with ShortScalp strategy!!!')

#-------------------Variables------------------------------
userName = st.secrets["db_username"]
passName = st.secrets["db_password"]

dns.resolver.default_resolver=dns.resolver.Resolver(configure=False)
dns.resolver.default_resolver.nameservers=['8.8.8.8']
myclient = pymongo.MongoClient("mongodb+srv://"+userName+":"+passName+"@cluster0.ss8kmkn.mongodb.net/?retryWrites=true&w=majority")
db = myclient["StockData"]
mo = MongoObject(db)
userCol = db['userData']
gotCol = db['gotData']
watchCol = db['watchData']
dailyCol = db['dailyData']

st.success('MongoDB connected')
kInfo = KSE100Info('data/0_kseall.csv')
today = date.today()
todayDate = today.strftime("%Y-%m-%d")
stocks = db.dailyData.distinct("symbol")

st.success("Deleting today's entry (if any)")
dailyCol.delete_many({'Date': todayDate})

st.info('Data scrapper getting data! (please wait)')
ds = DataScrapper(infoFile='data/0_kseall.csv',mutualFunds=True)
st.success('Data scrapper ready!')
#-------------------Variables------------------------------

allPosts = []
with st.spinner('Scrape+Updating... Wait for it...'):
    for percent_complete in range(len(stocks)):
        actualStockSymbol = stocks[percent_complete]
        stockSymbol = kInfo.getInvestingSymbol(actualStockSymbol)
        stockType = kInfo.getStockType(stockSymbol)
        stockName = kInfo.getActualName(actualStockSymbol)
        
        if(stockType=='Index'):
            # print(stockName+":"+stockType) 
            
            val = mo.getDailyData(actualStockSymbol)
            lastClose = val.iloc[-1].Close
            td = ds.getData(actualStockSymbol,'Index',isDebug=False)
            row2save = todayDate+","+str(lastClose)+","+str(td[1])+","+str(td[2])+","+str(td[3])+"\n"
            td[0] = lastClose

        elif(stockType=='PSX'):

            td = ds.getData(actualStockSymbol,'PSX',isDebug=False)        
            row2save = todayDate+","+str(td[0])+","+str(td[1])+","+str(td[2])+","+str(td[3])+"\n"
            
            
        elif(stockType=='MF'):
            td = ds.getData(actualStockSymbol,'MF',isDebug=False)
            row2save = todayDate+","+str(td[0])+","+str(td[1])+","+str(td[2])+","+str(td[3])+"\n"
        
        # st.success(actualStockSymbol+":"+row2save)

        post = {    "Date":todayDate,
                    "symbol":actualStockSymbol,
                    "Open":td[0],
                    "Low":td[1],
                    "High":td[2],
                    "Close":td[3]
                    }
        allPosts.append(post)

st.success('Scraping Done!')

st.text('Now updating to mongo!!!')
db.dailyData.insert_many(allPosts)


st.success('Updating to mongo Done!')

files = ['MIF.csv','MEF.csv','MAAF.csv','KMIF.csv','MGF.csv',]
bought = [57.83,31.05,0,0,103]

for i in range(len(files)):

    ## Not used due to Mongo
    print('Processing...'+files[i])
    thisFileName = files[i].replace(".csv", "")

    # df = pd.read_csv(dirPath+files[i])
    # df = df.reset_index()
    # df['Date'] = pd.to_datetime(df['Date'], format='%Y-%m-%d')
    # df.sort_values(by='Date')
    # df = df.set_index('Date')

    df = mo.getDailyData(thisFileName)
    df = sortWRTDates(df)

    # print(df.tail(30))
    
    dfClose = df.iloc[-1].Close
    dfLClose = df.iloc[-2].Close
    dfChange = ((dfClose - dfLClose)/dfClose)*100

    df=df.set_index('Date')

    fig = savePlot_Multi(     df,
                        params=[9,21,100,12,26,14],
                        dirName="mif/",
                        fileName=str(i)+'_'+thisFileName,
                        isClipping=True,
                        isShading=True,
                        candlestick=False,
                        horLine=bought[i],
                        stringToPrint=thisFileName+" : "+str(dfClose)+" : "+str(dfChange)+"%"
                        )
    st.pyplot(fig)


dailyCol.delete_many({'Date': todayDate})
st.success('Cleaned up afterwards')