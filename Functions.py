import pandas as pd
from datetime import date
from datetime import datetime
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.ticker import (MultipleLocator, AutoMinorLocator)
import pandas_ta

def sortWRTDates(indf):
    indf['DataFrame Column'] = pd.to_datetime(indf['Date'], format="%Y-%m-%d")
    indf = indf.sort_values(by='DataFrame Column',ignore_index=True)
    
    indf = indf.drop('DataFrame Column', axis=1)
    return indf

def getTodayData(copydf):
    df = copydf.iloc[:]
    df.reset_index(inplace=True)
    # print(df.tail())
    today = date.today()   
    todayDate = today.strftime("%d/%m/%Y")
    todayData = df[df['Date'].map(lambda s: s.startswith(todayDate))]
    if(todayData['Close'].shape[0]==1):
        return [date.today().strftime("%Y-%m-%d"),np.nan,np.nan,np.nan,np.nan]
    else:
        return [date.today().strftime("%Y-%m-%d"),todayData['Close'].iloc[0],todayData['Close'].min(),todayData['Close'].max(),todayData['Close'].iloc[len(todayData['Close'])-1]]

def convert_float(s):
    try:
        f = float(s)
    except ValueError:
        f = np.nan
    return f

def getmacd(df,a,b,c):
    close = df['Close']
    exp1 = close.ewm(span=a, adjust=False).mean()
    exp2 = close.ewm(span=b, adjust=False).mean()
    macd = exp1 - exp2
    signal = macd.ewm(span=c, adjust=False).mean()
    return macd,signal

def savePlot_Multi( df,
                dirName,
                fileName,
                params=[9,21,200,12,26,14],
                stringToPrint="",
                horLine=0,
                candlestick=False,
                isClipping=True,
                isShading=True):
    # df = df.reset_index()
    # df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')
    # df=df.set_index('Date')

    df['emaShort']=df['Close'].ewm(span=params[0], adjust=True).mean()
    df['emaMid']=df['Close'].ewm(span=params[1], adjust=True).mean()
    df['emaLong']=df['Close'].ewm(span=params[2], adjust=True).mean()
    df['RSI'] = pandas_ta.rsi(df['Close'], length = 14)
    df['RSI-EMA'] = df['RSI'].ewm(span=14, adjust=True).mean()

    macd,signal = getmacd(df,params[3],params[4],9)
    df['MACD']=macd
    df['Signal']=signal

    df['RSI'] = pandas_ta.rsi(df['Close'], length = params[5])       

    if(isClipping):
        df = df.iloc[30:] #Clipping
        df = df.iloc[-100:] #Clipping


    plt.rcParams['figure.figsize'] = (15, 10)

    fig, axs = plt.subplots(3, sharex=True, gridspec_kw={'height_ratios':[3,1,1]})
    plt.subplots_adjust(hspace=.0)
    plt.xticks(rotation=40)
    plt.locator_params(axis='x', nbins=10)

    #Info Plot
    
    if(candlestick):
        ##------------------------CandleStick Plot------------------------
        prices = df

        #define width of candlestick elements
        width = 0.9
        width2 = .1

        #define up and down prices
        up = prices[prices.Close>=prices.Open]
        down = prices[prices.Close<prices.Open]

        #define colors to use
        col1 = 'green'
        col2 = 'red'

        #plot up prices
        axs[0].bar(up.index,up.Close-up.Open,width,bottom=up.Open,color=col1)
        axs[0].bar(up.index,up.High-up.Close,width2,bottom=up.Close,color=col1)
        axs[0].bar(up.index,up.Low-up.Open,width2,bottom=up.Open,color=col1)

        #plot down prices
        axs[0].bar(down.index,down.Close-down.Open,width,bottom=down.Open,color=col2)
        axs[0].bar(down.index,down.High-down.Open,width2,bottom=down.Open,color=col2)
        axs[0].bar(down.index,down.Low-down.Close,width2,bottom=down.Close,color=col2)
        ##------------------------CandleStick Plot------------------------
    else:
        axs[0].plot(df['Close'], color='blue',marker=".",linewidth='0.5', label="Close")

    if(isShading):
        axs[0].fill_between(df.index, max(df.Close), min(df.Close),  
                    where=(df.MACD>df.Signal) , color = 'green', alpha = 0.1)

    axs[0].plot(df['emaShort'],linestyle='--',linewidth='0.75',label=str(params[1])+"-ema")
    axs[0].plot(df['emaMid'],linestyle='--',linewidth='0.75',label=str(params[2])+"-ema")
    axs[0].plot(df['emaLong'],linewidth='2',linestyle='--',label=str(params[0])+"-ema")
    
    ax = plt.gca()
    props = dict(boxstyle='round', facecolor='wheat', alpha=0.5)
    ax.text(0.1, 0.1, stringToPrint, transform=ax.transAxes, fontsize=14,verticalalignment='bottom', bbox=props)
    if(horLine>0):
        axs[0].axhline(y=horLine, color='r', linestyle='--')

    axs[1].plot(df['Signal'], color='orange',linewidth='1', label="Signal")
    axs[1].plot(df['MACD'], color='green',linewidth='1', label="MACD")
    axs[1].axhline(y=0, color='r', linestyle='-',linewidth='1')

    #RSI
    axs[2].plot(df['RSI'], color='green',linewidth='1', label="RSI")
    axs[2].plot(df['RSI-EMA'], color='blue',linewidth='1', label="EMA")
    axs[2].axhline(y=70, color='r', linestyle='--',linewidth='1')
    axs[2].axhline(y=30, color='g', linestyle='--',linewidth='1')
    axs[2].axis(ymin=0,ymax=100)

    for ax in axs:
        ax.label_outer()
        ax.grid()
        ax.legend(loc="upper left")
        
        # ax.grid(which='minor', linestyle='--')
        # ax.margins(0, 0)
        ax.xaxis.set_major_locator(MultipleLocator(5))
        ax.xaxis.set_minor_locator(MultipleLocator(1))

    return fig

class MongoObject:
    def __init__(self,db):
        self.db = db

    def getUsers(self):
        return self.db.userData.distinct("userID")

    def getDailyData(self,symbol):
        # print(symbol)
        results = self.db.dailyData.find(    { "symbol": symbol },
                                {"_id": 0,'symbol':1,'Date':1,'Open':1,'High':1,'Low':1,'Close':1}
                            )
        output = []
        for result in results:
            # print(result)
            thisOutput = [result['Date'],
                            float(result['Open']),
                            float(result['Low']),
                            float(result['High']),
                            float(result['Close'])
                            ]
            output.append(thisOutput)

        df = pd.DataFrame(output,columns=['Date','Open','Low','High','Close'])
        
        return df

    def getUpdatedDailyData(self,symbol='Karachi 100'):
        dateData = self.getDailyData(symbol)
        if(datetime.today().weekday()<5):
            quickData = self.getQuickData(symbol)
            y=getTodayData(quickData)
            a_series = pd.Series(y, index = dateData.columns)
            dateData=dateData.append(a_series,ignore_index=True)
            return dateData
        else:
            return dateData

    def getQuickData(self,symbol):
        results = self.db.quickData.find(   { "symbol": symbol },
                                            {"_id": 0,'Date':1,'symbol':1,'Close':1}
                                        )
        output = []
        for result in results:
            # print(result)
            thisOutput = [result['Date'],
                            float(result['Close'])
                            ]
            output.append(thisOutput)   
        df = pd.DataFrame(output,columns=['Date','Close'])
        df.set_index('Date',inplace=True)
        
        return df

    def getGotStocks(self,userID):
        results = pd.DataFrame(list(self.db.gotData.find( {'userID':userID },
                        {"_id": 0,
                        'userID':1,
                        'shares':1,
                        'actualSym':1,
                        'bought_price':1,
                        'bought_on':1,
                        'trail_fixed':1,
                        'slVal':1})
                      ))
        return results
    
    def getWatchStocks(self,userID):
        results = pd.DataFrame(list(self.db.watchData.find( {'userID':userID },
                        {"_id": 0,
                        'actualSym':1
                        })
        ))
        # print(results)
        return results

    def getUserInfo(self,userID):
        results = pd.DataFrame(list(self.db.userData.find( {'userID':userID},
                                {"_id": 0,
                                "userID":1,
                                "email":1,
                                "allName":1
                                })
        ))
        return results.iloc[0]

class KSE100Info:
    def __init__(self,path='data/0_kseall.csv'):
        self.kse100Stocks = pd.read_csv(path)
    
    def getActualSymbol(self,symbol):
        kseInfo = self.kse100Stocks[self.kse100Stocks.symbol==symbol]#Fetching only first record
        if(kseInfo.shape[0]>0):
            return kseInfo.iloc[0].actualSym
        else:
            return symbol
        
    def getActualName(self,actualSymbol):
        kseInfo = self.kse100Stocks[self.kse100Stocks.actualSym==actualSymbol]#Fetching only first record
        if(kseInfo.shape[0]>0):
            return kseInfo.iloc[0].NAME
        else:
            return actualSymbol
        
    def getInvestingSymbol(self,actualSymbol):
        kseInfo = self.kse100Stocks[self.kse100Stocks.actualSym==actualSymbol]#Fetching only first record
        if(kseInfo.shape[0]>0):
            # print(kseInfo.iloc[0])
            return kseInfo.iloc[0].symbol
        else:
            return actualSymbol
        
    def getActualSymbolBool(self,symbol):
        kseInfo = self.kse100Stocks[self.kse100Stocks.symbol==symbol]#Fetching only first record
        if(kseInfo.shape[0]>0):
            return kseInfo.iloc[0].actualSym,True
        else:
            return symbol,False
        
    def getInvestingSymbolBool(self,actualSymbol):
        kseInfo = self.kse100Stocks[self.kse100Stocks.actualSym==actualSymbol]#Fetching only first record
        if(kseInfo.shape[0]>0):
            return kseInfo.iloc[0].symbol,True
        else:
            return actualSymbol,False
        
    def getStockType(self,symbol,isActualSym=False):
        
        if(isActualSym):
            symbol = self.getInvestingSymbol(symbol)        
        
        kseInfo = self.kse100Stocks[self.kse100Stocks.symbol==symbol]#Fetching only first record
        if(kseInfo.shape[0]>0):
            return kseInfo.iloc[0].type 
        else:
            return "PSX"

class DataScrapper:
    def __init__(self,
                    infoFile='data/0_kseall.csv',
                    mutualFunds=False,
                    justMutualFunds=False):
        #===============================================UGLY CODE======================================================
        if(justMutualFunds or mutualFunds): 
            print('Gathering MFs')
            url="https://www.almeezangroup.com/fund-prices/"
            table_list=pd.io.html.read_html(url)

            mfData=table_list[0]
            # print(mfData)
            self.mfData          =  mfData.iloc[:, mfData.columns.get_level_values(1)=='MTD'] #Dummy
            self.mfData['names'] =  mfData.iloc[:, mfData.columns.get_level_values(1)=='Funds Category']
            self.mfData['nav']   =  mfData.iloc[:, mfData.columns.get_level_values(1)=='Repurchase (Rs.)']
            self.mfData['dates'] =  mfData.iloc[:, mfData.columns.get_level_values(1)=='Validity Date']

            self.mf_dict = {    'MEF': 'Meezan Energy Fund',
                                'MIF': 'Meezan Islamic Fund',
                                'MGF': 'Meezan Gold Fund',
                                'MAAF':'Meezan Asset Allocation Fund',
                                'KMIF':'KSE Meezan Index Fund',
            }

            self.rawMFdata=table_list[0]
            if(justMutualFunds):
                return
        #===============================================UGLY CODE======================================================

        self.kInfo = KSE100Info(infoFile)

        url="https://dps.psx.com.pk/indices"
        print('Gathering Indexes')
        table_list=pd.io.html.read_html(url)
        self.indexData=table_list[0]

        indexDict = {   'Karachi Meezan 30': 'KMI30',
                        'Karachi 100': 'KSE100',
                        'OGTi': 'OGTi',
                        'BKTi':'BKTi'
                        }

        url="https://www.psx.com.pk/market-summary/"
        print('Gathering Stocks')
        table_list=pd.io.html.read_html(url)
        allRows = []

        for i in range(2,len(table_list)):
            thisRow = table_list[i].values.tolist()
            for j in range(1,len(thisRow)):
                allRows.append(thisRow[j])
            
        # allRows[0:5]
        self.stockData = pd.DataFrame(allRows, columns = ['Name', 'LDCP','Open','High','Low','Close','Change','Volume'])


    #===============================================UGLY CODE======================================================
    def isNewData(self,today):
        thisDate = self.mfData.loc[self.mfData['names'] == 'Meezan Islamic Fund'].dates.iloc[0]
        navDate = datetime.datetime.strptime(thisDate, '%d %b %Y').date()

        print('Nav Date:   '+navDate.strftime("%Y-%m-%d"))
        print('Checking date: '+today.strftime("%Y-%m-%d"))
        return (navDate>today)
    #===============================================UGLY CODE======================================================
        
    
    def getData(self,actualName,objecType,isDebug=True):
        if(objecType=='Index'):

            objectName = self.kInfo.getActualName(actualName)

            # print(self.indexData.tail())
            thisHigh = self.indexData.loc[self.indexData['Index'] == objectName].iloc[0].High
            thisLow = self.indexData.loc[self.indexData['Index'] == objectName].iloc[0].Low
            thisClose = self.indexData.loc[self.indexData['Index'] == objectName].iloc[0].Current

            output = [np.nan,thisLow,thisHigh,thisClose]
            
        
        elif(objecType=='PSX'):
            # print(self.stockData.tail())
            # objectName = self.kInfo.getActualName(objectName)

            objectName = self.kInfo.getActualName(actualName)

            if(len(self.stockData[self.stockData['Name'].map(lambda s: s.startswith(objectName))])>0):
                thisOpen = convert_float(self.stockData[self.stockData['Name'].map(lambda s: s.startswith(objectName))].iloc[0].Open)
                thisHigh = convert_float(self.stockData[self.stockData['Name'].map(lambda s: s.startswith(objectName))].iloc[0].High)
                thisLow = convert_float(self.stockData[self.stockData['Name'].map(lambda s: s.startswith(objectName))].iloc[0].Low)
                thisClose = convert_float(self.stockData[self.stockData['Name'].map(lambda s: s.startswith(objectName))].iloc[0].Close)

                output = [thisOpen,thisLow,thisHigh,thisClose]
            else:
                if(isDebug):
                    print('----------------------------------'+objectName+'----------------------------------')
                output = [np.nan,np.nan,np.nan,np.nan]

        elif(objecType=='MF'):
            # print(self.mfData.tail())
            thisMF = self.kInfo.getActualName(actualName) #self.mf_dict[actualName]
            thisNAV = self.mfData.loc[self.mfData['names'] == thisMF].nav.iloc[0]

            output = [np.nan,np.nan,np.nan,thisNAV]#[thisNAV]       
        if(isDebug):
            print(output)
        return output