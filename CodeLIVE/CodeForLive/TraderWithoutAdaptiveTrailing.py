from __future__ import division

__author__ = 'pawasgupta'

# ----------Modules Required---------
import MySQLdb as mariadb
import numpy as np
import math
import time
import ConfigParser
import logging
import csv

#-------------Class Defination Start---------


class Trader:

    def __init__(self,l_strConfFile):  # constructor to initialise the members
        self.m_strConfFile=l_strConfFile

        self.m_strPriceDbName = ''  # Name of Database
        self.m_strPriceDbUserName = ''  # username
        self.m_strPriceDbPassword = ''  # Password
        self.m_strPriceTableName = ''  # name of Tick Data Table
        self.m_strPriceDbHost = '' # ip of the pricedb host

        self.m_strTradeDbName = ''  # Name of Database
        self.m_strTradeDbUserName = ''  # username
        self.m_strTradeDbPassword = ''  # Password
        self.m_strResultTableName = ''  # name of Results Table (Contains Date,  Time,  Position)
        self.m_strSignalTableName = ''  # name of Signal Table
        self.m_strOHLCTableName = ''  # name of Signal Table
        self.m_strRinaFileName = ""  # Rina File name
        
        self.m_iBarTimeInterval = 15  # Bar Size
        self.m_strSessionCloseTime = ''  # Session Close Time (Should have same format as Tick_Time in DataTable)
        self.m_strSessionBeginTime = ''  # Session Begin Time (Should have same format as Tick_Time in DataTable)
        self.m_strRunStartDate = ''
        self.m_strRunStopDate = ''

        self.m_iLiveMode=''

        self.m_LoggerHandle=''
        self.m_strLogFileName=''
        self.m_iRestartFlag = 0	    # 1 if u r restarting after the crash
                                    # 0 if u r starting normally
        self.m_iInternalRestartFlag = 0   # a flag that initialize self.m_2dafFeatureMatrix differently in case u restart
                                        # it will be set and reset automatically
        self.m_iVirtualExitAfterRestart=0


        self.m_iTotalTicks = 0  # Counts the Total Ticks
        self.m_iBarNumber = 0  # Time Instant or  Bar Count
        self.m_2dlfOHLCMatrix = []  # Contains Date, Time, O, H, L, C for Each bar
        self.m_2dlfNonRoundedClose = []

        #-----  Parameters of my Algo ------
        self.m_iTradingWindowSize = 80
        self.m_fA = 1000
        self.m_fAlpha = 100
        self.m_fBetaa = 60
        self.m_fGamma = 5
        self.m_fDelta = 0.5
        self.m_iBarsBack = 25
        self.m_fThreshold = 0.3

        self.m_liPosition = []  # vector storing Positions for each 't'
        self.m_2dafWeights = []  # Matrix storing Weights for each 't'
        self.m_afTempPosition = []  # vector for TempPosition for each 't'
        self.m_afProfit = []  # Vector for Profit for each 't'
        self.m_afCumulativeProfit = []  # Vector for Cumulative Position for each 't'
        self.m_2dafFeatureMatrix = []  # Matrix for Features
        self.m_afReturns = []  # Vector Storing Returns for each 't'
        self.m_fTrailPrice = 0.0  # For Trailing
        self.m_iTrailFlag = 0  # for Trailing

        self.m_iTradeType = 0  # variable for remarks for trades in Trade sheet
        self.m_iPositionInMarket = 0  # variable that stores current position in market
        self.m_fMarketEnterPrice = 0.0  # trade Entry Price (used in Trailing and Stoploss)
        self.m_iNumberofLots=1
        self.m_iLotSize=10000
        self.m_iShareQuantity = 10000  # Share Quantity

        self.m_iRinaInternalFlag = 1  # Flag used internally by Write2Rina module
        self.m_iGenerateRina = 1  # Make 0 if u don't want to write 2 Rina
        self.m_iTradeNum = 1  # Trade number Count
        self.m_strEnterType = ''  # used internally by Write2Rina module

    # =================== Member Functions =========================== 

    def ReadConfFile(self,l_strConfFile):
        l_oConfigFileObject = ConfigParser.ConfigParser()
        l_oConfigFileObject.read(l_strConfFile)

        self.m_strPriceDbName = l_oConfigFileObject.get('SectionConf', 'PriceDbName')  # Name of Database
        self.m_strPriceDbUserName = l_oConfigFileObject.get('SectionConf', 'PriceDbUserName')  # username
        self.m_strPriceDbPassword = l_oConfigFileObject.get('SectionConf', 'PriceDbPassword')  # Password
        self.m_strPriceTableName = l_oConfigFileObject.get('SectionConf', 'PriceTable')  # name of Tick Data Table
        self.m_strPriceDbHost = l_oConfigFileObject.get('SectionConf', 'PriceDbHost')

        self.m_strTradeDbName = l_oConfigFileObject.get('SectionConf', 'TradeDbName')  # Name of Database
        self.m_strTradeDbUserName = l_oConfigFileObject.get('SectionConf', 'TradeDbUserName')  # username
        self.m_strTradeDbPassword = l_oConfigFileObject.get('SectionConf', 'TradeDbPassword')  # Password
        self.m_strTradeDbHost = l_oConfigFileObject.get('SectionConf', 'TradeDbHost')

        self.m_strResultTableName = l_oConfigFileObject.get('SectionConf', 'ResultTable')  # name of Results Table (Contains Date,  Time,  Position)
        self.m_strSignalTableName = l_oConfigFileObject.get('SectionConf', 'SignalTable')  # name of Signal Table
        self.m_strOHLCTableName = l_oConfigFileObject.get('SectionConf', 'OHLCTable')  # name of OHLC Table
        self.m_strRinaFileName = l_oConfigFileObject.get('SectionConf', 'RinaFileName')  # Rina File name

        self.m_iBarTimeInterval = l_oConfigFileObject.getint('SectionConf', 'BarSize')  # Bar Size
        self.m_strSessionCloseTime = l_oConfigFileObject.get('SectionConf', 'SessionEnd')  # Session Close Time (Should have same format as Tick_Time in DataTable)
        self.m_strSessionBeginTime = l_oConfigFileObject.get('SectionConf', 'SessionBegin')  # Session Begin Time (Should have same format as Tick_Time in DataTable)
        self.m_strRunStartDate = l_oConfigFileObject.get('SectionConf', 'RunStartDate')
        self.m_strRunStopDate = l_oConfigFileObject.get('SectionConf', 'RunStopDate')

        self.m_iRestartFlag = int(l_oConfigFileObject.get('SectionConf', 'RestartFlag'))
        self.m_iLiveMode = int(l_oConfigFileObject.get('SectionConf', 'LiveMode'))

        self.m_strLogFileName = l_oConfigFileObject.get('SectionConf', 'LogFileName')
        self.m_iNumberofLots = int(l_oConfigFileObject.get('SectionConf', 'NumberofLots'))
        self.m_iLotSize = int(l_oConfigFileObject.get('SectionConf', 'LotSize'))
        self.m_iShareQuantity=self.m_iNumberofLots*self.m_iLotSize

    #--------------Change_B4-----------------
    def LoginToPriceDb(self):  # login into the database
        l_PriceDbHandle = mariadb.connect(user=self.m_strPriceDbUserName, passwd=self.m_strPriceDbPassword, host= self.m_strPriceDbHost,  db=self.m_strPriceDbName)
        return l_PriceDbHandle

    def LoginToTradeDb(self):  # login into the database
        l_TradeDbHandle = mariadb.connect(user=self.m_strTradeDbUserName, passwd=self.m_strTradeDbPassword, host= self.m_strTradeDbHost, db=self.m_strTradeDbName)
        return l_TradeDbHandle

    def MovingAverage(self, l_afSeries, l_iWindow):  # Computes Moving Average
        l_iSz = len(l_afSeries)
        l_afAverage = np.zeros(l_iSz, dtype='float64')
        l_afConvolveVector = np.repeat(1.0, l_iWindow) / l_iWindow
        l_afAverage[0:l_iWindow - 1] = 0
        l_afAverage[l_iWindow - 1:l_iSz] = np.convolve(l_afSeries, l_afConvolveVector, 'valid')
        return l_afAverage

    def Kernel(self, l_fVar1, l_fVar2):  # Kernel Function (Using RBF kernel)
        l_fNorm = np.linalg.norm(l_fVar1 - l_fVar2)
        l_fSigma = 1
        return (math.exp((-1 * (l_fNorm ** 2)) / (2 * l_fSigma * l_fSigma)))


    # Retain data for last 2*TradingWindowSize bars if the size of matrices increases beyond a point
    def FlushData(self):
        self.m_liPosition = self.m_liPosition[3*self.m_iTradingWindowSize:5*self.m_iTradingWindowSize]
        self.m_afCumulativeProfit = self.m_afCumulativeProfit[3*self.m_iTradingWindowSize:5*self.m_iTradingWindowSize]
        self.m_afProfit = self.m_afProfit[3*self.m_iTradingWindowSize:5*self.m_iTradingWindowSize]
        self.m_afTempPosition = self.m_afTempPosition[3*self.m_iTradingWindowSize:5*self.m_iTradingWindowSize]
        self.m_afReturns = self.m_afReturns[3*self.m_iTradingWindowSize:5*self.m_iTradingWindowSize]
        self.m_2dafWeights = self.m_2dafWeights[:][3*self.m_iTradingWindowSize:5*self.m_iTradingWindowSize]
        self.m_2dafFeatureMatrix = self.m_2dafFeatureMatrix [:][3*self.m_iTradingWindowSize:5*self.m_iTradingWindowSize]
        self.m_2dlfOHLCMatrix = self.m_2dlfOHLCMatrix[3*self.m_iTradingWindowSize:5*self.m_iTradingWindowSize][:]

        self.m_iBarNumber=3*self.m_iTradingWindowSize

    def CreateTable(self):  # Create Required Tables
        l_TradeDbHandle = self.LoginToTradeDb()
        l_TradeDbCursor = l_TradeDbHandle.cursor()
        l_TradeDbCursor.execute("show tables like '%s'" % (self.m_strResultTableName))
        l_QueryResult = l_TradeDbCursor.fetchall()
        if not l_QueryResult:  # Create table if it does not exist
            l_TradeDbCursor.execute("create table %s (Date int NOT NULL,  Time int NOT NULL,  Position int NOT NULL, TempPosition real NOT NULL)" % (self.m_strResultTableName))
            l_TradeDbHandle.commit()
        else:  # If it Exists,  Delete data present in table
            l_TradeDbCursor.execute("delete from %s;" % (self.m_strResultTableName))
            l_TradeDbHandle.commit()
        self.m_LoggerHandle.info('Result Table Created')

        l_TradeDbCursor.execute("show tables like '%s'" % (self.m_strOHLCTableName))
        l_QueryResult = l_TradeDbCursor.fetchall()
        if not l_QueryResult:  # Create table if it does not exist
            l_TradeDbCursor.execute("create table %s (Date text NOT NULL,  Time text NOT NULL,  Open real NOT NULL,  High real NOT NULL,  Low real NOT NULL,  Close real NOT NULL)" % (self.m_strOHLCTableName))
            l_TradeDbHandle.commit()
        else:  # If it Exists,  Delete data present in table
            l_TradeDbCursor.execute("delete from %s;" % (self.m_strOHLCTableName))
            l_TradeDbHandle.commit()
        self.m_LoggerHandle.info('OHLC Table Created')

        l_TradeDbCursor.execute("show tables like '%s'" % (self.m_strSignalTableName))
        l_QueryResult = l_TradeDbCursor.fetchall()
        if not l_QueryResult:  # Create table if it does not exist
            l_TradeDbCursor.execute("create table %s (Date text NOT NULL, Time text NOT NULL, Price Real NOT NULL, TradeType text NOT NULL, Qty int NOT NULL, Remarks text)" % (self.m_strSignalTableName))
            l_TradeDbHandle.commit()
        else:  #Else Delete data present in table
            l_TradeDbCursor.execute("delete from %s;" % (self.m_strSignalTableName))
            l_TradeDbHandle.commit()
        l_TradeDbCursor.close()
        self.m_LoggerHandle.info('Signal Table Created')

    def CreateRinaFile(self):
        l_FileId = open(self.m_strRinaFileName, "w")  # create file if it does not exist else flush the content
        l_FileId.write('"Trade #", "Date", "Time", "Signal", "Price", "Contracts", "% Profit", "Runup", "Entry Eff", "Total", "System"\r\n')
        l_FileId.write('"Type", "Date", "Time", "Signal", "Price", "Profit", "Cum Profit", "Drawdown", "Exit Eff", "Eff", "Market"\r\n')
        l_FileId.close()
        self.m_LoggerHandle.info('Rina File Created with headers')

    def CreateLoggingFile(self):

        self.m_LoggerHandle = logging.getLogger('LogsforTrader')

        self.m_LoggerHandle.setLevel(logging.DEBUG)

        # create console handler and set level to debug
        #l_FileHandlerforLogger = logging.StreamHandler() # To log to Console
        if self.m_iRestartFlag==0 :
            l_FileHandlerforLogger = logging.FileHandler(self.m_strLogFileName,'w')
        else:
            l_FileHandlerforLogger = logging.FileHandler(self.m_strLogFileName,'a')

        l_FileHandlerforLogger.setLevel(logging.DEBUG)

        # create formatter
        l_FormatterforLogger = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        # add formatter to ch
        l_FileHandlerforLogger.setFormatter(l_FormatterforLogger)

        # add ch to logger
        self.m_LoggerHandle.addHandler(l_FileHandlerforLogger)


    def CreateOHLC(self, l_2dlTickDataMatrix, l_iTickNumber):
        l_TradeDbHandle = self.LoginToTradeDb()  # login into database
        l_TradeDbCursor = l_TradeDbHandle.cursor()

        l_strBarDate = l_2dlTickDataMatrix[l_iTickNumber - 1][0]  # date Stored in 1st column of TickMatrix
        l_strBarTime = l_2dlTickDataMatrix[l_iTickNumber - 1][1]  # time stored in 2nd Column of TickMatrix

        l_fBarOpen = (round((float(l_2dlTickDataMatrix[0][2])) * 100)) / 100  # Open in 3rd column of TickMatrix
        l_fBarClose = (round((float(l_2dlTickDataMatrix[l_iTickNumber - 1][5])) * 100)) / 100  # rounded Close for Algorithm

        l_fRoundedHigh = [(round((float(l_2dlTickDataMatrix[l_iIndex][3])) * 100)) / 100 for l_iIndex in range(0, l_iTickNumber)]  # High in 4th column of TickMatrix
        l_fBarHigh = max(l_fRoundedHigh)

        l_fRoundedLow = [(round((float(l_2dlTickDataMatrix[l_iIndex][4])) * 100)) / 100 for l_iIndex in range(0, l_iTickNumber)]  # Close in 5th column of TickMatrix
        l_fBarLow = min(l_fRoundedLow)

        self.m_iBarNumber += 1  # increase Bar Count
        self.m_2dlfOHLCMatrix.append([])
        self.m_2dlfOHLCMatrix[self.m_iBarNumber - 1].extend([l_strBarDate, l_strBarTime, l_fBarOpen, l_fBarHigh, l_fBarLow, l_fBarClose])

        self.m_2dlfNonRoundedClose.append([])  # Non Rounded Close for Trades
        self.m_2dlfNonRoundedClose[self.m_iBarNumber - 1].append(float(l_2dlTickDataMatrix[l_iTickNumber - 1][5]))

        l_TradeDbCursor.execute("Insert into %s (Date, Time, Open, High, Low, Close) values('%s', '%s', '%s', '%s', '%s', '%s');" % (self.m_strOHLCTableName, str(l_strBarDate), str(l_strBarTime), l_fBarOpen, l_fBarHigh, l_fBarLow,l_fBarClose))  # Write into DB
        l_TradeDbHandle.commit()
        l_TradeDbCursor.close()
        self.m_LoggerHandle.info('Writing into OHLC Table for Date= %s and Time= %s' %(l_strBarDate, l_strBarTime))
        return l_strBarDate, l_strBarTime

    def One_SMO(self, l_2dafPhiUsed):

        l_afOptimalLamda = np.zeros(self.m_iTradingWindowSize, dtype='float64')
        l_afOptimalG = np.zeros(self.m_iTradingWindowSize, dtype='float64')


        #----------Optimisation Start------------------------------------------------
        l_fEpsilon = 0.1  # for stopping Criteria
        l_2dafVecX = l_2dafPhiUsed
        l_afVecR = np.zeros(self.m_iTradingWindowSize, dtype='float64')
        l_afVecR[0:self.m_iTradingWindowSize] = self.m_afReturns[self.m_iBarNumber - self.m_iTradingWindowSize:self.m_iBarNumber]

        l_afLamdaNew = 1000 * np.ones(self.m_iTradingWindowSize, dtype='float64')  # initial Point
        l_afGNew = 1000 * np.ones(self.m_iTradingWindowSize, dtype='float64')
        l_afLamdaOld = 1000 * np.ones(self.m_iTradingWindowSize, dtype='float64')
        l_afGOld = 1000 * np.ones(self.m_iTradingWindowSize, dtype='float64')
        l_iIterate = 1

        while (l_iIterate != 0 and l_iIterate < 20):  # start 1SMO algorithm
            #--------------update all lambda and get lamdanew----------
            for l_iLamdaIndex in range(0, self.m_iTradingWindowSize, 1):  # updating each lambda
                l_afXkMinus1 = l_2dafVecX[:, l_iLamdaIndex]
                l_fReturnK = l_afVecR[l_iLamdaIndex]
                l_fVar1 = (l_fReturnK ** 2) * (self.Kernel(l_afXkMinus1, l_afXkMinus1) + 1 / self.m_fA)
                l_fFuncOld = 0
                for l_iUIndex in range(0, self.m_iTradingWindowSize, 1):
                    l_afXuMinus1 = l_2dafVecX[:, l_iUIndex]
                    l_afXu = l_2dafVecX[:, l_iUIndex + 1]

                    l_fFuncOld = l_fFuncOld + 2 * (l_afLamdaOld[l_iUIndex] * l_afVecR[l_iUIndex] * (self.Kernel(l_afXkMinus1, l_afXuMinus1) + 1 / self.m_fA))
                    l_fFuncOld = l_fFuncOld + 2 * (l_afGOld[l_iUIndex] * self.m_fDelta * (self.Kernel(l_afXkMinus1, l_afXu) - self.Kernel(l_afXkMinus1, l_afXuMinus1)))

                l_fFuncOld = l_fFuncOld * l_fReturnK * (-0.5)

                if l_fVar1 == 0:
                    l_fVar1 = l_fVar1 + 1
                l_fLamdaKNew = (l_fFuncOld / l_fVar1) + l_afLamdaOld[l_iLamdaIndex]

                if l_fLamdaKNew > self.m_fAlpha:
                    l_fLamdaKNew = self.m_fAlpha
                elif l_fLamdaKNew < self.m_fBetaa:
                    l_fLamdaKNew = self.m_fBetaa

                l_afLamdaNew[l_iLamdaIndex] = l_fLamdaKNew

            for l_iGIndex in range(0, self.m_iTradingWindowSize, 1):  # update all G
                l_afXkMinus1 = l_2dafVecX[:, l_iGIndex]
                l_afXk = l_2dafVecX[:, l_iGIndex + 1]
                l_fVar1 = (self.m_fDelta ** 2) * (self.Kernel(l_afXk, l_afXk) - self.Kernel(l_afXk, l_afXkMinus1) - self.Kernel(l_afXkMinus1,l_afXk) + self.Kernel(l_afXkMinus1, l_afXkMinus1))
                l_fFuncOld = 0
                for l_iUIndex in range(0, self.m_iTradingWindowSize, 1):
                    l_afXuMinus1 = l_2dafVecX[:, l_iUIndex]
                    l_afXu = l_2dafVecX[:, l_iUIndex + 1]

                    l_fFuncOld = l_fFuncOld + 2 * (l_afVecR[l_iUIndex] * l_afLamdaOld[l_iUIndex] * (self.Kernel(l_afXuMinus1, l_afXk) - self.Kernel(l_afXuMinus1, l_afXkMinus1)))
                    l_fFuncOld = l_fFuncOld + 2 * (l_afGOld[l_iUIndex] * (self.Kernel(l_afXk, l_afXu) - self.Kernel(l_afXk, l_afXuMinus1) - self.Kernel(l_afXkMinus1, l_afXu) + self.Kernel(l_afXkMinus1, l_afXuMinus1)))

                l_fFuncOld = l_fFuncOld * self.m_fDelta * (-0.5)

                if l_fVar1 == 0:
                    l_fVar1 = 1
                l_fGKNew = (l_fFuncOld / l_fVar1) + l_afGOld[l_iGIndex]

                if l_fGKNew > self.m_fGamma:
                    l_fGKNew = self.m_fGamma
                elif l_fGKNew < -self.m_fGamma:
                    l_fGKNew = -self.m_fGamma

                l_afGNew[l_iGIndex] = l_fGKNew

                # compare lambdaold and lambdanew & gold and gnew
            if ((np.linalg.norm(l_afLamdaOld - l_afLamdaNew)) <= l_fEpsilon) and ((np.linalg.norm(l_afGOld - l_afGNew)) <= l_fEpsilon):
                l_iIterate = 0  #Stop iterating
            else:
                l_iIterate = l_iIterate + 1
                l_afGOld[0:self.m_iTradingWindowSize] = l_afGNew[0:self.m_iTradingWindowSize]
                l_afLamdaOld[0:self.m_iTradingWindowSize] = l_afLamdaNew[0:self.m_iTradingWindowSize]

        #--------------------Optimisation finished------------------------------------

        l_afOptimalLamda[0:self.m_iTradingWindowSize] = l_afLamdaNew[0:self.m_iTradingWindowSize]
        l_afOptimalG[0:self.m_iTradingWindowSize] = l_afGNew[0:self.m_iTradingWindowSize]

        return l_afOptimalLamda, l_afOptimalG  # return new values


    #--------My Algo------------------------
    def TradingAlgorithm(self):
        self.m_LoggerHandle.info('In Trading Algorithm to compute position')
        l_2dafOpenPrice = np.matrix(self.m_2dlfOHLCMatrix)[:, 2]
        l_afOpenPrice = np.resize(l_2dafOpenPrice, self.m_iBarNumber)
        l_afOpenPrice = np.asfarray(l_afOpenPrice, dtype='float64')

        l_2dafHighPrice = np.matrix(self.m_2dlfOHLCMatrix)[:, 3]
        l_afHighPrice = np.resize(l_2dafHighPrice, self.m_iBarNumber)
        l_afHighPrice = np.asfarray(l_afHighPrice, dtype='float64')

        l_2dafLowPrice = np.matrix(self.m_2dlfOHLCMatrix)[:, 4]
        l_afLowPrice = np.resize(l_2dafLowPrice, self.m_iBarNumber)
        l_afLowPrice = np.asfarray(l_afLowPrice, dtype='float64')

        l_2dafClosePrice = np.matrix(self.m_2dlfOHLCMatrix)[:, 5]
        l_afClosePrice = np.resize(l_2dafClosePrice, self.m_iBarNumber)
        l_afClosePrice = np.asfarray(l_afClosePrice, dtype='float64')

        l_afTypicalPrice = (l_afClosePrice + l_afHighPrice + l_afLowPrice) / 3

        l_afShortMA = self.MovingAverage(l_afClosePrice, 12)
        l_afLongMA = self.MovingAverage(l_afClosePrice, 20)

        l_afSmoothPrice = self.MovingAverage(l_afTypicalPrice, 7)  # compute Features
        l_afFeatureVector = l_afSmoothPrice

        if (self.m_iBarNumber == self.m_iTradingWindowSize and self.m_iRinaInternalFlag==0):  # Initialise during 1st call
            self.m_afProfit = np.zeros(self.m_iTradingWindowSize - 1, dtype='float64')
            self.m_afCumulativeProfit = np.zeros(self.m_iTradingWindowSize - 1, dtype='float64')
            self.m_2dafFeatureMatrix = np.zeros((self.m_iBarsBack, self.m_iTradingWindowSize), dtype='float64')
            self.m_afReturns = np.zeros(self.m_iTradingWindowSize - 1, dtype='float64')
            l_iIndex = 1
            while (l_iIndex < self.m_iTradingWindowSize - 1):  # loop to assign all the values of r,  cannot put abcd assignment here because it has to be in the same loop as the weight update
                self.m_afReturns[l_iIndex] = l_afClosePrice[l_iIndex] - l_afClosePrice[l_iIndex - 1]
                l_iIndex = l_iIndex + 1
            self.m_2dafWeights = np.zeros((self.m_iBarsBack, self.m_iTradingWindowSize - 1), dtype='float64')

        # ---If restarting, initialize the feature matrix from old prices that have been read from the OHLC table of the database
        if self.m_iInternalRestartFlag == 1:
            self.m_afProfit = np.zeros(self.m_iBarNumber - 1, dtype='float64')
            self.m_afCumulativeProfit = np.zeros(self.m_iBarNumber - 1, dtype='float64')
            #self.m_2dafFeatureMatrix = np.zeros((self.m_iBarsBack, self.m_iTradingWindowSize), dtype='float64')
            self.m_afReturns = np.zeros(self.m_iBarNumber - 1, dtype='float64')
            l_iIndex = 1
            while (l_iIndex < self.m_iBarNumber - 1):  # loop to assign all the values of r,  cannot put abcd assignment here because it has to be in the same loop as the weight update
                self.m_afReturns[l_iIndex] = l_afClosePrice[l_iIndex] - l_afClosePrice[l_iIndex - 1]
                l_iIndex = l_iIndex + 1
            self.m_2dafWeights = np.zeros((self.m_iBarsBack, self.m_iBarNumber - 1), dtype='float64')

            self.m_2dafFeatureMatrix = np.zeros((self.m_iBarsBack, self.m_iBarNumber), dtype='float64')
            for l_iloopvar in range(self.m_iBarsBack,self.m_iBarNumber):
                self.m_2dafFeatureMatrix[:,l_iloopvar] =np.transpose(l_afFeatureVector[l_iloopvar-self.m_iBarsBack:l_iloopvar])
            self.m_2dafFeatureMatrix[:, 0:self.m_iBarsBack] = self.m_2dafFeatureMatrix[:,[self.m_iBarsBack]]
            self.m_iInternalRestartFlag=0

        # ---appending values corresponding to each bar-------
        #  ---append in each call---------------------
        self.m_2dafFeatureMatrix = np.hstack((self.m_2dafFeatureMatrix, np.reshape(l_afFeatureVector[self.m_iBarNumber - self.m_iBarsBack:self.m_iBarNumber],[-1, 1])))  # appending a column to Feature Matrix

        self.m_afReturns = np.append(self.m_afReturns, (l_afClosePrice[self.m_iBarNumber - 1] - l_afClosePrice[self.m_iBarNumber - 2]))

        self.m_2dafWeights = np.hstack((self.m_2dafWeights, np.zeros((self.m_iBarsBack, 1), dtype="float64")))
        self.m_afTempPosition = np.append(self.m_afTempPosition, 0.0)
        self.m_liPosition = np.append(self.m_liPosition, 0.0)
        self.m_afProfit = np.append(self.m_afProfit, 0.0)
        self.m_afCumulativeProfit = np.append(self.m_afCumulativeProfit, 0.0)

        l_2dafPhiUsed = self.m_2dafFeatureMatrix[:,self.m_iBarNumber - self.m_iTradingWindowSize:self.m_iBarNumber + 1]  # Phi used contains last WindowSize+1 Samples

        #---Make Phi 0 mean and unit Variance--------------
        l_fStd = np.std(l_2dafPhiUsed, 1)
        l_afSum = np.sum(l_2dafPhiUsed, axis=1, dtype='float64')
        l_afSum = l_afSum / (self.m_iTradingWindowSize + 1)
        l_afSum = np.reshape(l_afSum, (-1, 1))
        l_2dafSum = np.repeat(l_afSum, self.m_iTradingWindowSize + 1, axis=1)
        l_2dafPhiUsed = l_2dafPhiUsed - l_2dafSum
        for l_iIndex in range(0, len(l_2dafPhiUsed)):
            l_2dafPhiUsed[l_iIndex, :] = l_2dafPhiUsed[l_iIndex, :] / l_fStd[l_iIndex]

        l_fTheta = 0.0

        l_afOptimalLamda, l_afOptimalG = self.One_SMO(l_2dafPhiUsed)  # get optimum l and G

        for l_iIndex in range(0, self.m_iTradingWindowSize, 1):  # compute weights and Theta
            self.m_2dafWeights[:, self.m_iBarNumber - 1] = self.m_2dafWeights[:, self.m_iBarNumber - 1] + ((l_afOptimalLamda[l_iIndex] * self.m_afReturns[self.m_iBarNumber - self.m_iTradingWindowSize + l_iIndex] * l_2dafPhiUsed[:,l_iIndex]) + (l_afOptimalG[l_iIndex] * self.m_fDelta * (l_2dafPhiUsed[:,l_iIndex + 1] - l_2dafPhiUsed[:,l_iIndex])))
            l_fTheta = l_fTheta + (1 / self.m_fA) * self.m_afReturns[self.m_iBarNumber - self.m_iTradingWindowSize + l_iIndex] * l_afOptimalLamda[l_iIndex]

        l_fTempPosition = (np.dot((np.transpose(self.m_2dafWeights[:, [self.m_iBarNumber - 1]])),l_2dafPhiUsed[:, self.m_iTradingWindowSize]) + l_fTheta) / (np.linalg.norm(self.m_2dafWeights[:, [self.m_iBarNumber - 1]]))  # Compute Temp Position
        self.m_afTempPosition[self.m_iBarNumber - 1] = round(l_fTempPosition, 2)


        #----------Positions from TempPositions------------------
        if ((self.m_afTempPosition[self.m_iBarNumber - 1] > self.m_fThreshold) and (self.m_afTempPosition[self.m_iBarNumber - 2] > self.m_fThreshold) and (l_afHighPrice[self.m_iBarNumber - 1] > l_afHighPrice[self.m_iBarNumber - 2]) and l_afShortMA[self.m_iBarNumber - 1] > l_afLongMA[self.m_iBarNumber - 1]):
            self.m_liPosition[self.m_iBarNumber - 1] = 1
        elif ((self.m_afTempPosition[self.m_iBarNumber - 1] > self.m_fThreshold) and (self.m_liPosition[self.m_iBarNumber - 2] == 1) and l_afShortMA[self.m_iBarNumber - 1] >l_afLongMA[self.m_iBarNumber - 1]):
            self.m_liPosition[self.m_iBarNumber - 1] = 1
        elif ((self.m_afTempPosition[self.m_iBarNumber - 1] < -self.m_fThreshold) and (self.m_afTempPosition[self.m_iBarNumber - 2] < -self.m_fThreshold) and (l_afHighPrice[self.m_iBarNumber - 1] < l_afHighPrice[self.m_iBarNumber - 2]) and l_afShortMA[self.m_iBarNumber - 1] < l_afLongMA[self.m_iBarNumber - 1]):
            self.m_liPosition[self.m_iBarNumber - 1] = -1
        elif ((self.m_afTempPosition[self.m_iBarNumber - 1] < -self.m_fThreshold) and (self.m_liPosition[self.m_iBarNumber - 2] == -1) and l_afShortMA[self.m_iBarNumber - 1] < l_afLongMA[self.m_iBarNumber - 1]):
            self.m_liPosition[self.m_iBarNumber - 1] = -1
        else:
            self.m_liPosition[self.m_iBarNumber - 1] = 0

        self.m_afProfit[self.m_iBarNumber - 1] = self.m_liPosition[self.m_iBarNumber - 2] * (l_afClosePrice[self.m_iBarNumber - 1] - l_afClosePrice[self.m_iBarNumber - 2]) - 0.0035 *self.m_iNumberofLots * abs(self.m_liPosition[self.m_iBarNumber - 1] - self.m_liPosition[self.m_iBarNumber - 2])  # instantaneous profit

        self.m_afCumulativeProfit[self.m_iBarNumber - 1] = self.m_afCumulativeProfit[self.m_iBarNumber - 2] + self.m_afProfit[self.m_iBarNumber - 1]  # Cumulative Profit


    def SignalGeneration(self):
        l_strBarDate = str(self.m_2dlfOHLCMatrix[self.m_iBarNumber - 1][0])
        l_strBarTime = str(self.m_2dlfOHLCMatrix[self.m_iBarNumber - 1][1])

        l_fBarClose = float(self.m_2dlfNonRoundedClose[self.m_iBarNumber - 1][0])

        if (self.m_liPosition[self.m_iBarNumber - 2] == 0 and self.m_liPosition[self.m_iBarNumber - 1] == -1 and self.m_iPositionInMarket == 0):  # Generate EnterShort1 Signal (Case 1)
            l_TradeDbHandle = self.LoginToTradeDb()  # login into database
            l_TradeDbCursor = l_TradeDbHandle.cursor()
            l_TradeDbCursor.execute("Insert into %s (Date, Time, Price, Tradetype, Qty, Remarks) values('%s', '%s', '%s', '%s', '%s', '%s');" % (self.m_strSignalTableName, l_strBarDate, l_strBarTime, l_fBarClose, 'sell', self.m_iShareQuantity,'PF_SE_SHORT1_15_001'))  # Write into DB
            l_TradeDbHandle.commit()
            self.m_iTradeType = 1
            self.m_fMarketEnterPrice = l_fBarClose
            self.m_iPositionInMarket = -1


            self.m_LoggerHandle.info('Writing into Signal Table for Date= %s & Time =%s' %(l_strBarDate,l_strBarTime))
            l_TradeDbCursor.close()

        if (self.m_liPosition[self.m_iBarNumber - 2] == 0 and self.m_liPosition[self.m_iBarNumber - 1] == 1 and self.m_iPositionInMarket == 0):  # Generate EnterLong1 Signal (Case 2)
            l_TradeDbHandle = self.LoginToTradeDb()  # login into database
            l_TradeDbCursor = l_TradeDbHandle.cursor()
            l_TradeDbCursor.execute("Insert into %s (Date, Time, Price, Tradetype, Qty, Remarks) values('%s', '%s', '%s', '%s', '%s', '%s');" % (self.m_strSignalTableName, l_strBarDate, l_strBarTime, l_fBarClose, 'buy', self.m_iShareQuantity,'PF_LE_LONG1_15_001'))  # Write into DB
            l_TradeDbHandle.commit()
            self.m_iTradeType = 1
            self.m_fMarketEnterPrice = l_fBarClose
            self.m_iPositionInMarket = 1
            self.m_LoggerHandle.info('Writing into Signal Table for Date= %s & Time =%s' %(l_strBarDate,l_strBarTime))
            l_TradeDbCursor.close()

        if (self.m_liPosition[self.m_iBarNumber - 2] == 1 and self.m_liPosition[self.m_iBarNumber - 1] == -1 and self.m_iTradeType == 1 and self.m_iPositionInMarket == 1):  # clear off your Long1 position (Case3)
            l_TradeDbHandle = self.LoginToTradeDb()  # login into database
            l_TradeDbCursor = l_TradeDbHandle.cursor()
            l_TradeDbCursor.execute("Insert into %s (Date, Time, Price, Tradetype, Qty, Remarks) values('%s', '%s', '%s', '%s', '%s', '%s');" % (self.m_strSignalTableName, l_strBarDate, l_strBarTime, l_fBarClose, 'sell', self.m_iShareQuantity,'PF_LX_LEXIT1_15_001'))  # Write into DB
            l_TradeDbHandle.commit()
            self.m_iPositionInMarket = 0
            self.m_fTrailPrice = 0.0
            self.m_iTrailFlag = 0
            self.m_LoggerHandle.info('Writing into Signal Table for Date= %s & Time =%s' %(l_strBarDate,l_strBarTime))
            l_TradeDbCursor.close()

        if (self.m_liPosition[self.m_iBarNumber - 2] == 1 and self.m_liPosition[self.m_iBarNumber - 1] == -1 and self.m_iTradeType == 2 and self.m_iPositionInMarket == 1):  # clear off your Long2 position (Case 4)
            l_TradeDbHandle = self.LoginToTradeDb()  # login into database
            l_TradeDbCursor = l_TradeDbHandle.cursor()
            l_TradeDbCursor.execute("Insert into %s (Date, Time, Price, Tradetype, Qty, Remarks) values('%s', '%s', '%s', '%s', '%s', '%s');" % (self.m_strSignalTableName, l_strBarDate, l_strBarTime, l_fBarClose, 'sell', self.m_iShareQuantity,'PF_LX_LEXIT2_15_001'))  # Write into DB
            l_TradeDbHandle.commit()
            self.m_iPositionInMarket = 0
            self.m_fTrailPrice = 0.0
            self.m_iTrailFlag = 0
            self.m_LoggerHandle.info('Writing into Signal Table for Date= %s & Time =%s' %(l_strBarDate,l_strBarTime))
            l_TradeDbCursor.close()

        if (self.m_liPosition[self.m_iBarNumber - 2] == 1 and self.m_liPosition[self.m_iBarNumber - 1] == -1 and self.m_iPositionInMarket == 0):  # Generate a Enter Short2 signal (Case 5)
            l_TradeDbHandle = self.LoginToTradeDb()  # login into database
            l_TradeDbCursor = l_TradeDbHandle.cursor()
            l_TradeDbCursor.execute("Insert into %s (Date, Time, Price, Tradetype, Qty, Remarks) values('%s', '%s', '%s', '%s', '%s', '%s');" % (self.m_strSignalTableName, l_strBarDate, l_strBarTime, l_fBarClose, 'sell', self.m_iShareQuantity,'PF_SE_SHORT2_15_001'))  # Write into DB
            l_TradeDbHandle.commit()
            self.m_iTradeType == 2
            self.m_fMarketEnterPrice = l_fBarClose
            self.m_iPositionInMarket = -1
            self.m_LoggerHandle.info('Writing into Signal Table for Date= %s & Time =%s' %(l_strBarDate,l_strBarTime))
            l_TradeDbCursor.close()

        if (self.m_liPosition[self.m_iBarNumber - 2] == -1 and self.m_liPosition[self.m_iBarNumber - 1] == 1 and self.m_iTradeType == 1 and self.m_iPositionInMarket == -1):  # clear off your Short1 position (Case 6)
            l_TradeDbHandle = self.LoginToTradeDb()  # login into database
            l_TradeDbCursor = l_TradeDbHandle.cursor()
            l_TradeDbCursor.execute("Insert into %s (Date, Time, Price, Tradetype, Qty, Remarks) values('%s', '%s', '%s', '%s', '%s', '%s');" % (self.m_strSignalTableName, l_strBarDate, l_strBarTime, l_fBarClose, 'buy', self.m_iShareQuantity,'PF_SX_SEXIT1_15_001'))  # Write into DB
            l_TradeDbHandle.commit()
            self.m_iPositionInMarket = 0
            self.m_fTrailPrice = 0.0
            self.m_iTrailFlag = 0
            self.m_LoggerHandle.info('Writing into Signal Table for Date= %s & Time =%s' %(l_strBarDate,l_strBarTime))
            l_TradeDbCursor.close()

        if (self.m_liPosition[self.m_iBarNumber - 2] == -1 and self.m_liPosition[self.m_iBarNumber - 1] == 1 and self.m_iTradeType == 2 and self.m_iPositionInMarket == -1):  # clear off your Short2 position (Case 7)
            l_TradeDbHandle = self.LoginToTradeDb()  # login into database
            l_TradeDbCursor = l_TradeDbHandle.cursor()
            l_TradeDbCursor.execute("Insert into %s (Date, Time, Price, Tradetype, Qty, Remarks) values('%s', '%s', '%s', '%s', '%s', '%s');" % (self.m_strSignalTableName, l_strBarDate, l_strBarTime, l_fBarClose, 'buy', self.m_iShareQuantity,'PF_SX_SEXIT2_15_001'))  # Write into DB
            l_TradeDbHandle.commit()
            self.m_iPositionInMarket = 0
            self.m_fTrailPrice = 0.0
            self.m_iTrailFlag = 0
            self.m_LoggerHandle.info('Writing into Signal Table for Date= %s & Time =%s' %(l_strBarDate,l_strBarTime))
            l_TradeDbCursor.close()

        if (self.m_liPosition[self.m_iBarNumber - 2] == -1 and self.m_liPosition[self.m_iBarNumber - 1] == 1 and self.m_iPositionInMarket == 0):  # Generate a EnterLong2 signal (Case 8)
            l_TradeDbHandle = self.LoginToTradeDb()  # login into database
            l_TradeDbCursor = l_TradeDbHandle.cursor()
            l_TradeDbCursor.execute("Insert into %s (Date, Time, Price, Tradetype, Qty, Remarks) values('%s', '%s', '%s', '%s', '%s', '%s');" % (self.m_strSignalTableName, l_strBarDate, l_strBarTime, l_fBarClose, 'buy', self.m_iShareQuantity,'PF_LE_LONG2_15_001'))  # Write into DB
            l_TradeDbHandle.commit()
            self.m_iTradeType == 2
            self.m_fMarketEnterPrice = l_fBarClose
            self.m_iPositionInMarket = 1
            self.m_LoggerHandle.info('Writing into Signal Table for Date= %s & Time =%s' %(l_strBarDate,l_strBarTime))
            l_TradeDbCursor.close()

        if (self.m_liPosition[self.m_iBarNumber - 2] == -1 and self.m_liPosition[self.m_iBarNumber - 1] == 0 and self.m_iTradeType == 1 and self.m_iPositionInMarket == -1):  #clear off your Short1 position (Case 9)
            l_TradeDbHandle = self.LoginToTradeDb()  # login into database
            l_TradeDbCursor = l_TradeDbHandle.cursor()
            l_TradeDbCursor.execute("Insert into %s (Date, Time, Price, Tradetype, Qty, Remarks) values('%s', '%s', '%s', '%s', '%s', '%s');" % (self.m_strSignalTableName, l_strBarDate, l_strBarTime, l_fBarClose, 'buy', self.m_iShareQuantity,'PF_SX_SEXIT3_15_001'))  # Write into DB
            l_TradeDbHandle.commit()
            self.m_iPositionInMarket = 0
            self.m_fTrailPrice = 0.0
            self.m_iTrailFlag = 0
            self.m_LoggerHandle.info('Writing into Signal Table for Date= %s & Time =%s' %(l_strBarDate,l_strBarTime))
            l_TradeDbCursor.close()

        if (self.m_liPosition[self.m_iBarNumber - 2] == -1 and self.m_liPosition[self.m_iBarNumber - 1] == 0 and self.m_iTradeType == 2 and self.m_iPositionInMarket == -1):  # clear off your Short2 position(Case 10)
            l_TradeDbHandle = self.LoginToTradeDb()  # login into database
            l_TradeDbCursor = l_TradeDbHandle.cursor()
            l_TradeDbCursor.execute("Insert into %s (Date, Time, Price, Tradetype, Qty, Remarks) values('%s', '%s', '%s', '%s', '%s', '%s');" % (self.m_strSignalTableName, l_strBarDate, l_strBarTime, l_fBarClose, 'buy', self.m_iShareQuantity,'PF_SX_SEXIT4_15_001'))  # Write into DB
            l_TradeDbHandle.commit()
            self.m_iPositionInMarket = 0
            self.m_fTrailPrice = 0.0
            self.m_iTrailFlag = 0
            self.m_LoggerHandle.info('Writing into Signal Table for Date= %s & Time =%s' %(l_strBarDate,l_strBarTime))
            l_TradeDbCursor.close()

        if (self.m_liPosition[self.m_iBarNumber - 2] == 1 and self.m_liPosition[self.m_iBarNumber - 1] == 0 and self.m_iTradeType == 1 and self.m_iPositionInMarket == 1):  #clear off your Long1 position (Case 11)
            l_TradeDbHandle = self.LoginToTradeDb()  # login into database
            l_TradeDbCursor = l_TradeDbHandle.cursor()
            l_TradeDbCursor.execute("Insert into %s (Date, Time, Price, Tradetype, Qty, Remarks) values('%s', '%s', '%s', '%s', '%s', '%s');" % (self.m_strSignalTableName, l_strBarDate, l_strBarTime, l_fBarClose, 'sell', self.m_iShareQuantity,'PF_LX_LEXIT3_15_001'))  # Write into DB
            l_TradeDbHandle.commit()
            self.m_iPositionInMarket = 0
            self.m_fTrailPrice = 0.0
            self.m_iTrailFlag = 0
            self.m_LoggerHandle.info('Writing into Signal Table for Date= %s & Time =%s' %(l_strBarDate,l_strBarTime))
            l_TradeDbCursor.close()

        if (self.m_liPosition[self.m_iBarNumber - 2] == 1 and self.m_liPosition[self.m_iBarNumber - 1] == 0 and self.m_iTradeType == 2 and self.m_iPositionInMarket == 1):  #clear off Long2 your position (Case 12)
            l_TradeDbHandle = self.LoginToTradeDb()  # login into database
            l_TradeDbCursor = l_TradeDbHandle.cursor()
            l_TradeDbCursor.execute("Insert into %s (Date, Time, Price, Tradetype, Qty, Remarks) values('%s', '%s', '%s', '%s', '%s', '%s');" % (self.m_strSignalTableName, l_strBarDate, l_strBarTime, l_fBarClose, 'sell', self.m_iShareQuantity,'PF_LX_LEXIT4_15_001'))  # Write into DB
            l_TradeDbHandle.commit()
            self.m_iPositionInMarket = 0
            self.m_fTrailPrice = 0.0
            self.m_iTrailFlag = 0
            self.m_LoggerHandle.info('Writing into Signal Table for Date= %s & Time =%s' %(l_strBarDate,l_strBarTime))
            l_TradeDbCursor.close()

    def Write2Rina(self, l_SignalsRead):
        #----------Entries in Rina file---------
        l_iProfit = 0
        l_iPerProf = self.m_iTradeNum
        l_iCumProf = self.m_iTradeNum
        l_iEnterEff = 0
        l_iExitEff = 0
        l_iDD = 0
        l_iRunUp = 0
        l_iTot = 0
        l_iEff = 0
        l_strSystem = 'portfolio_1'
        l_strMarket = 'USDINR1'

        l_iDataSize = len(l_SignalsRead)  # number of Signals
        l_FileId = open(self.m_strRinaFileName, "a")

        if (l_iDataSize > 0):
            l_iCounter = 0
            while (l_iCounter < l_iDataSize):
                #Entries in Signal table alternate for Entry and Exit
                if (self.m_iRinaInternalFlag == 1):  # Entry  Signal
                    l_strEnterdate = l_SignalsRead[l_iCounter][0]  # Date in Rina File Will be entered in same format as in data table
                    l_strEntertime = l_SignalsRead[l_iCounter][1]  # Time in Rina File Will be entered in same format as in data table
                    l_fEnterprice = l_SignalsRead[l_iCounter][2]
                    self.m_strEnterType = l_SignalsRead[l_iCounter][3]
                    l_iEntercontracts = l_SignalsRead[l_iCounter][4]
                    l_strEnterSignalName = l_SignalsRead[l_iCounter][5]
                    #l_newenterdate = enterdate.replace('-', '/')
                    l_strWritten = '"' + str(self.m_iTradeNum) + '", "' + l_strEnterdate + '", "' + l_strEntertime + '", "' + l_strEnterSignalName + '", "' + str(l_fEnterprice) + '", "' + str(l_iEntercontracts) + '", "' + str(l_iPerProf) + '", "' + str(l_iRunUp) + '", "' + str(l_iEnterEff) + '", "' + str(l_iTot) + '", "' + str(l_strSystem) + '"\r\n'
                    l_FileId.write(l_strWritten)
                    l_iCounter = l_iCounter + 1
                    self.m_iRinaInternalFlag = 2  # Next Signal will correspond to Exit

                elif (self.m_iRinaInternalFlag == 2):  # Exit Signal
                    l_Exitdate = l_SignalsRead[l_iCounter][0]
                    l_Exittime = l_SignalsRead[l_iCounter][1]
                    l_Exitprice = l_SignalsRead[l_iCounter][2]
                    l_Exittype = l_SignalsRead[l_iCounter][3]
                    l_Exitcontracts = l_SignalsRead[l_iCounter][4]
                    l_Exitsignalname = l_SignalsRead[l_iCounter][5]

                    #newexitdate = exitdate.replace('-', '/')
                    l_strWritten = '"' + str(self.m_strEnterType) + '", "' + l_Exitdate + '", "' + l_Exittime + '", "' + l_Exitsignalname + '", "' + str(l_Exitprice) + '", "' + str(l_iProfit) + '", "' + str(l_iCumProf) + '", "' + str(l_iDD) + '", "' + str(l_iExitEff) + '", "' + str(l_iEff) + '", "' + l_strMarket + '"\r\n'
                    l_FileId.write(l_strWritten)
                    self.m_iTradeNum = self.m_iTradeNum + 1  # Trade Complete
                    l_iPerProf = self.m_iTradeNum
                    l_iCumProf = self.m_iTradeNum
                    l_iCounter = l_iCounter + 1
                    self.m_iRinaInternalFlag = 1  # Next Signal will correspond to Entry

        l_FileId.close()
        return

    def Trading(self):
        self.ReadConfFile(self.m_strConfFile) # Read Configuration file
        #logging.basicConfig(filename='logdata.log',format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p') # LOgs to the file
        #------------ local variables-----------
        l_iProgramFlag = 1  # loop variable
        l_iTickNumber = 0  # Count the ticks for Bar Creation
        l_iDateForPreviousTick=0
        l_iTimeForPreviousTick=0
        l_strTickDate = ''
        l_strTickTime = ''
        l_fTickOpen = 0.0
        l_fTickHigh = 0.0
        l_fTickLow = 0.0
        l_fTickClose = 0.0
        l_2dlTickDataMatrix = [[None for _ in range(6)] for _ in range(self.m_iBarTimeInterval)]  # Creating a matrix for tick data

        self.CreateLoggingFile()
        if (self.m_iRestartFlag==0):
            self.CreateTable()  # Create tables
            self.CreateRinaFile()  # Create Rina file with header

        print "Start date " + time.strftime("%x")
        print "Start time " + time.strftime("%X")

        while (l_iProgramFlag == 1):

            #==================================================================================
            #-----------------If Restarting----------------------------------
            if (self.m_iRestartFlag==1 and self.m_iLiveMode ==1): # If u r restarting then

                self.m_LoggerHandle.info('-------------------------------------------------')
                self.m_LoggerHandle.info('-------------------------------------------------')
                self.m_LoggerHandle.info('--------------------Restarting-------------------')
                self.m_liPosition=[]
                self.m_afTempPosition = np.zeros(0, dtype='float64')

                #---------Opening Connnection for trade DB-----
                self.m_LoggerHandle.info("Connecting to Trade DB to load necessary data...")
                l_iNoTradeDbConnectionFlag=0
                l_TradeDbHandle= None
                try:
                    l_TradeDbHandle = self.LoginToTradeDb()  # login into database
                except:
                    self.m_LoggerHandle.info("Exception thrown in db connection")
                    l_iNoTradeDbConnectionFlag=1

                # If u fail to connect (server is off), go to sleep
                # If server is off, it may happen that an exception is thrown or we get nothing in db handle
                if (l_iNoTradeDbConnectionFlag) or not(l_TradeDbHandle):
                    self.m_LoggerHandle.info('failed to connect to trade db..cant proceed...trying again after 20 seconds')
                    time.sleep(20)
                    continue
                l_TradeDbCursor = l_TradeDbHandle.cursor()


                self.m_LoggerHandle.info('connected to trade db')
                self.m_LoggerHandle.info("Loading necessary data...")

                #----------------Count the number of positions that were stored in database before u crashed
                # checking the discrepency bewtween OHLC table and Position Table

                l_TradeDbCursor.execute("select count(*) from %s" % (self.m_strResultTableName))  # read 1 line form database
                l_QueryResult = l_TradeDbCursor.fetchall()  # Query result is a tuple
                l_iTotalRows = int(l_QueryResult[0][0])
                # chddcking if number of OHLC bars == number of Positions
                l_TradeDbCursor.execute("select count(*) from %s" % (self.m_strOHLCTableName))  # read 1 line form database
                l_QueryResult = l_TradeDbCursor.fetchall()  # Query result is a tuple
                l_iTotalOHLCRows = int(l_QueryResult[0][0])

                if l_iTotalRows<> l_iTotalOHLCRows: # if not equal, match the 2
                    l_TradeDbCursor.execute("select * from %s LIMIT %s,1" % (self.m_strResultTableName, l_iTotalRows-1))  # read 1 line form database
                    l_QueryResult = l_TradeDbCursor.fetchall()
                    l_iDateLastInResultTable=int(l_QueryResult[0][0])
                    l_iTimeLastInResultTable=int(l_QueryResult[0][1])
                    l_TradeDbCursor.execute("delete from %s where (date = %s and time > %s)  or (date>%s)" % (self.m_strOHLCTableName, l_iDateLastInResultTable, l_iTimeLastInResultTable, l_iDateLastInResultTable))  # read 1 line form database
                    self.m_LoggerHandle.info('Deleted Extra Rows from OHLC table...')
                    l_TradeDbHandle.commit()
                    #l_TradeDbHandle.close()
                #------------------------------------------------------------------------------------------------

                #---------------Fetch Required Data from database-------------------------------------------------
                if l_iTotalRows>=5*self.m_iTradingWindowSize:
                    l_iNumberOfRow = 5*self.m_iTradingWindowSize - 1
                    l_iFromRow = l_iTotalRows - (l_iNumberOfRow)
                    self.m_iBarNumber = l_iNumberOfRow # ASSUMPTION: The code had crashed after running for more than 5*TradingWindow bars
                    self.m_iInternalRestartFlag = 1

                elif l_iTotalRows < 5 * self.m_iTradingWindowSize and l_iTotalRows >= self.m_iTradingWindowSize:
                    l_iNumberOfRow = l_iTotalRows
                    l_iFromRow = 0
                    self.m_iBarNumber = l_iNumberOfRow  # ASSUMPTION: The code had crashed after running for less than 5*TradingWindow bars
                                                        # but more than TradningWindow Bars
                    self.m_iInternalRestartFlag = 1

                else:
                    l_iNumberOfRow = l_iTotalRows
                    l_iFromRow=0
                    self.m_iBarNumber = l_iNumberOfRow  # ASSUMPTION: The code had crashed after running for less than TradingWindow bars
                    self.m_iInternalRestartFlag = 0

                # Fetch the required number of positions, Temp Positions and OHLC from the database
                # Fetch Positions
                self.m_LoggerHandle.info("Fetching Old Postions...")
                l_TradeDbCursor.execute("select Date, Time, Position,TempPosition from %s LIMIT %s, %s;" % (self.m_strResultTableName, l_iFromRow,l_iNumberOfRow))  # read 1 line form database
                l_QueryResult=l_TradeDbCursor.fetchall()
                for l_tItem in l_QueryResult: # item is tuple and local
                    self.m_liPosition.append(int(l_tItem[2]))
                    self.m_afTempPosition = np.append(self.m_afTempPosition, float(l_tItem[3]))
                l_iLastPositionBeforeCrash=(int(l_tItem[2]))
                l_iLastTempPostionBeforeCrash=float(l_tItem[3])
                #-----Fetch OHLC from database
                self.m_LoggerHandle.info("Fetching Old Prices...")
                l_TradeDbCursor.execute("select Date, Time, Open,Low,High,Close from %s LIMIT %s, %s;" % (self.m_strOHLCTableName,l_iFromRow,l_iNumberOfRow))  # read 1 line form database
                l_QueryResult=l_TradeDbCursor.fetchall()
                for l_tItem in l_QueryResult:
                    self.m_2dlfOHLCMatrix.append([])
                    self.m_2dlfOHLCMatrix[-1].extend([l_tItem[0],l_tItem[1],l_tItem[2],l_tItem[3],l_tItem[4],l_tItem[5]])
                    self.m_2dlfNonRoundedClose.append([])  # Close for Trades
                    self.m_2dlfNonRoundedClose[-1].append(float(l_tItem[5]))
                #-----------------------------------------------------------------------------------------------------------------


                #---------------------Check if last trade was open
                l_TradeDbCursor.execute("select count(*) from %s" % (self.m_strSignalTableName))  # read 1 line form database
                l_QueryResult = l_TradeDbCursor.fetchall()  # Query result is a tuple
                l_iTotalSignalsInSignalTable = int(l_QueryResult[0][0])
                if l_iTotalSignalsInSignalTable>0: # If there is any signal
                    l_iFromRow=l_iTotalSignalsInSignalTable-1
                    l_iNumberOfRow=1
                    
                    # Fetch last signal from signal table
                    l_TradeDbCursor.execute("select * from %s LIMIT %s, %s;" % (self.m_strSignalTableName, l_iFromRow, l_iNumberOfRow))  # read 1 line form database
                    l_QueryResult = l_TradeDbCursor.fetchall()

                    l_strDateLastInSignalTable=str(l_QueryResult[0][0])
                    l_strTimeLastInSignalTable=str(l_QueryResult[0][1])
                    l_fPriceLastInSignalTable=float(l_QueryResult[0][2])
                    l_strRemarksLastInSignalTable = str(l_QueryResult[0][5])

                    #If the last signal corresponds to Open Trade
                    if ('_LE_' in l_strRemarksLastInSignalTable.upper()):
                        self.m_iPositionInMarket=1
                        self.m_fMarketEnterPrice=l_fPriceLastInSignalTable
                        if ('LONG1' in l_strRemarksLastInSignalTable.upper()):
                            self.m_iTradeType=1
                        elif ('LONG2' in l_strRemarksLastInSignalTable.upper()):
                            self.m_iTradeType = 2
                        #Check the status of trailing before it crashed, set appropriate flags
                        #Check if the trail was ON. It it was, follow the trail
                        l_iCheckLongTrailFlag=0
                        for l_lRowOfOHLC in self.m_2dlfOHLCMatrix:
                            if (l_strDateLastInSignalTable in l_lRowOfOHLC) and (l_strTimeLastInSignalTable in l_lRowOfOHLC):
                                #print "hello1"
                                self.m_LoggerHandle.info("matched trade's date %s and time %s....now checking for trail of Long position" % (l_lRowOfOHLC[0], l_lRowOfOHLC[1]))
                                l_iCheckLongTrailFlag=1

                            if (l_iCheckLongTrailFlag) and self.m_iTrailFlag == 0 and (l_lRowOfOHLC[5]>= l_fPriceLastInSignalTable + l_fPriceLastInSignalTable*0.01) :
                                self.m_iTrailFlag=1
                                self.m_fTrailPrice= l_lRowOfOHLC[5]
                                self.m_LoggerHandle.info("Trail ON on Date %s and Time %s" % (l_lRowOfOHLC[0], l_lRowOfOHLC[1]))


                            if self.m_iTrailFlag ==1 and (l_lRowOfOHLC[5]>= self.m_fTrailPrice):
                                self.m_fTrailPrice = l_lRowOfOHLC[5]
                                self.m_LoggerHandle.info(
                                    "Trailing on Date %s and Time %s" % (l_lRowOfOHLC[0], l_lRowOfOHLC[1]))
                    # Do the same with Short Open trade
                    if ('_SE_' in l_strRemarksLastInSignalTable.upper()):
                        self.m_iPositionInMarket = -1
                        self.m_fMarketEnterPrice = l_fPriceLastInSignalTable
                        if ('SHORT1' in l_strRemarksLastInSignalTable.upper()):
                            self.m_iTradeType = 1
                        elif ('SHORT2' in l_strRemarksLastInSignalTable.upper()):
                            self.m_iTradeType = 2

                        l_iCheckShortTrailFlag = 0
                        for l_lRowOfOHLC in self.m_2dlfOHLCMatrix:

                            if (l_strDateLastInSignalTable in l_lRowOfOHLC) and (l_strTimeLastInSignalTable in l_lRowOfOHLC):
                                l_iCheckShortTrailFlag = 1
                                self.m_LoggerHandle.info("matched trade's date %s and time %s....now checking for trail of short position" %(l_lRowOfOHLC[0], l_lRowOfOHLC[1]))

                            if (l_iCheckShortTrailFlag)and self.m_iTrailFlag == 0 and (l_lRowOfOHLC[5] <= l_fPriceLastInSignalTable - l_fPriceLastInSignalTable * 0.01):
                                self.m_iTrailFlag = -1
                                self.m_fTrailPrice = l_lRowOfOHLC[5]
                                self.m_LoggerHandle.info("Trail ON on Date %s and Time %s" %(l_lRowOfOHLC[0], l_lRowOfOHLC[1]))
                            if self.m_iTrailFlag == -1 and (l_lRowOfOHLC[5] <= self.m_fTrailPrice):
                                self.m_fTrailPrice = l_lRowOfOHLC[5]
                                self.m_LoggerHandle.info("Trailing on Date %s and Time %s" % (l_lRowOfOHLC[0], l_lRowOfOHLC[1]))

                    #-------------------------------------------------------------------------------------

                    #--------------------------------------------------------------------------------------
                    #Adding a virtual environment that checks for the trail/hit or SL hit during the missed period
                    # It is basically helpful for a time when the trader goes off for a long period of time

                    l_iCrashDate=int(self.m_2dlfOHLCMatrix[-1][0]) # Last Date Before crash
                    l_iCrashTime=int(self.m_2dlfOHLCMatrix[-1][1]) # Lst Time Before crash
                    l_iRestartDate=int(time.strftime("%Y%m%d")) - 19000000  # Current date
                    l_iRestartTime=int(time.strftime("%H%M")) # Current time
                    # Fetch missing data from price DB (from crash date till restart Date)
                    l_iNoPriceDbConnectionFlag = 0
                    l_PriceDbHandle = None

                    try:
                        l_PriceDbHandle = self.LoginToPriceDb()  # login into database
                    except:
                        self.m_LoggerHandle.info("Exception thrown in db connection")
                        l_iNoPriceDbConnectionFlag = 1

                    # If u fail to connect (server is off), skip the step
                    # If server is off, it may happen that an exception is thrown or we get nothing in db handle
                    if (l_iNoPriceDbConnectionFlag) or not (l_PriceDbHandle):
                        # If u dont connect, miss the trail check and bar formation
                        self.m_LoggerHandle.info('failed to connect to price db...Not checking the hit of virtual SL or Trail')
                    else:
                        self.m_LoggerHandle.info('connected to price db...Fetching the missed data')
                        l_PriceDbCursor = l_PriceDbHandle.cursor()  # since you have got the connection grab a cursor
                        # Fetch data till date = current date and time = current time"
                        l_PriceDbCursor.execute(
                            "select * from %s where (date = %s and time > %s)  or (date>%s and  date < %s) or (date = %s  and time <= %s) ;" % (self.m_strPriceTableName, l_iCrashDate,l_iCrashTime,l_iCrashDate,l_iRestartDate, l_iRestartDate,l_iRestartTime))  # read 1 line form database
                        l_QueryResult = l_PriceDbCursor.fetchall()
                        # closing Connection
                        l_PriceDbCursor.close()
                        # If u dont get data during live mode
                        if (not l_QueryResult):
                            # if no data, miss the trail check and bar formation
                            self.m_LoggerHandle.info("Did not get data missed data...Not checking the hit of virtual SL or Trail ...Moving ahead..... ")
                        else:
                            self.m_LoggerHandle.info(
                                'Fetched missing data...')
                            l_iDateForPreviousTick=l_iRestartDate
                            l_iTimeForPreviousTick=l_iRestartTime
                            self.m_LoggerHandle.info(
                                'Checking for the Virtual Trail/StopLossss hit...')
                            # check if it would have hit the trailing/SL, had it been on.
                            l_iVirtualLSLFlag=0
                            l_iVirtualLTLFlag=0
                            l_iVirtualSSLFlag=0
                            l_iVirtualSTLFlag=0
                            for l_lFetchedRow in l_QueryResult:
                                l_fTickClose=float(l_lFetchedRow[5])
                                # -------------------TRAILING-------------------------------------------------------
                                # --------------Trailing long position----------------------------------------------
                                if (self.m_iPositionInMarket == 1 and self.m_iTrailFlag == 1 and l_fTickClose >= self.m_fTrailPrice):  # trail is ON and market is going up (Follow Trail)
                                    self.m_fTrailPrice = l_fTickClose
                                elif (self.m_iPositionInMarket == 1 and self.m_iTrailFlag == 1 and l_fTickClose <= self.m_fTrailPrice - 0.0035 * self.m_fTrailPrice):  # trail is ON and market moved down (Trail Hit)
                                    l_iVirtualLTLFlag=1
                                    self.m_LoggerHandle.info(
                                        'Virtual Trail Hit for Long Position...')
                                    self.m_LoggerHandle.info(
                                        'Will Exit the open trade as market opens')
                                elif (self.m_iPositionInMarket == 1 and self.m_iTrailFlag == 0 and l_fTickClose >= self.m_fMarketEnterPrice + self.m_fMarketEnterPrice * 0.01):  # Trail  On
                                    self.m_iTrailFlag = 1
                                    self.m_fTrailPrice = l_fTickClose
                                # -------------------------------------------------------------------------------------------
                                # ---------------------Trailing Short position-------------
                                if (self.m_iPositionInMarket == -1 and self.m_iTrailFlag == -1 and l_fTickClose <= self.m_fTrailPrice):  # trail is ON and market is going down (Follow Trail)
                                    self.m_fTrailPrice = l_fTickClose
                                elif(self.m_iPositionInMarket == -1 and self.m_iTrailFlag == -1 and l_fTickClose >= self.m_fTrailPrice + 0.0035 * self.m_fTrailPrice):  # trail is ON and market moved up (Trail Hit)
                                    l_iVirtualSTLFlag=1
                                    self.m_LoggerHandle.info(
                                        'Virtual Trail Hit for Short Position...')
                                    self.m_LoggerHandle.info(
                                        'Will Exit the open trade as market opens')

                                elif (self.m_iPositionInMarket == -1 and self.m_iTrailFlag == 0 and l_fTickClose <= self.m_fMarketEnterPrice - self.m_fMarketEnterPrice * 0.01):  # Trail On
                                    self.m_iTrailFlag = -1
                                    self.m_fTrailPrice = l_fTickClose

                                # -----------------------------------------------------------------------------------------------------------------%
                                # -------------------STOPLOSS------------------------%
                                # ---------------for Long position-------------------%
                                if ((self.m_iPositionInMarket == 1) and (l_fTickClose <= (self.m_fMarketEnterPrice - 0.0025 * self.m_fMarketEnterPrice))):  # Stop Loss
                                    l_iVirtualLSLFlag=1
                                    self.m_LoggerHandle.info('Virtual SL Hit for Long Position...')
                                    self.m_LoggerHandle.info('Will Exit the open trade as market opens')


                                # ---------------for short position------------------%
                                if ((self.m_iPositionInMarket == -1) and (l_fTickClose >= (self.m_fMarketEnterPrice + 0.0025 * self.m_fMarketEnterPrice))):
                                    l_iVirtualSSLFlag=1
                                    self.m_LoggerHandle.info('Virtual SL Hit for Short Position...')
                                    self.m_LoggerHandle.info('Will Exit the open trade as market opens')

                                if (l_iVirtualLSLFlag or l_iVirtualLTLFlag or l_iVirtualSSLFlag or l_iVirtualSTLFlag):
                                    self.m_iVirtualExitAfterRestart = 1
                                    break


                                    # ------------------------------------------------------%
                            #if (l_iVirtualLSLFlag or l_iVirtualLTLFlag or l_iVirtualSSLFlag or l_iVirtualSTLFlag):
                            #    l_iPositionToBeFilled=0
                            #    l_fTempToBeFilled=0.0
                            #    self.m_iVirtualExitAfterRestart=1
                            #else:
                            #l_iPositionToBeFilled=self.m_iPositionInMarket
                            #l_fTempToBeFilled = float(self.m_iPositionInMarket)
                            #--------------------------------------------------------------
                            # Fill the gap with positions and OHLC bars
                            l_iPositionToBeFilled =l_iLastPositionBeforeCrash
                            l_fTempToBeFilled = l_iLastTempPostionBeforeCrash
                            l_iTickNumber=0
                            l_iTickCountMissed=0
                            l_iTotalTicksMissed=len(l_QueryResult)
                            self.m_LoggerHandle.info('Creating OHLC from missed data...')

                            for l_lFetchedRow in l_QueryResult:
                                l_iTickCountMissed+=1
                                l_iTickNumber = l_iTickNumber + 1  # Increment tick number
                                self.m_LoggerHandle.info('Tick Number is %s' % (l_iTickNumber))
                                l_strTickDate = str(l_lFetchedRow[0])
                                # print l_strTickDate
                                l_strTickTime = str(l_lFetchedRow[1])
                                # print l_strTickTime
                                l_fTickOpen = l_lFetchedRow[2]
                                l_fTickHigh = l_lFetchedRow[3]
                                l_fTickLow = l_lFetchedRow[4]
                                l_fTickClose = l_lFetchedRow[5]
                                l_2dlTickDataMatrix[l_iTickNumber - 1][0] = l_strTickDate
                                l_2dlTickDataMatrix[l_iTickNumber - 1][1] = l_strTickTime
                                l_2dlTickDataMatrix[l_iTickNumber - 1][2] = l_fTickOpen
                                l_2dlTickDataMatrix[l_iTickNumber - 1][3] = l_fTickHigh
                                l_2dlTickDataMatrix[l_iTickNumber - 1][4] = l_fTickLow
                                l_2dlTickDataMatrix[l_iTickNumber - 1][5] = l_fTickClose

                                if (l_iTickNumber == self.m_iBarTimeInterval) or (str(l_strTickTime) == self.m_strSessionCloseTime): #or (l_iTickCountMissed == l_iTotalTicksMissed) # If BarTimeInterval ticks or Session closing Time then Create the Bar
                                    l_strBarDate, l_strBarTime = self.CreateOHLC(l_2dlTickDataMatrix[0:l_iTickNumber][:],l_iTickNumber)  # TickNumber takes care of the fact that there can be situation of  Ticks<BarTimeInterval
                                    l_iTickNumber = 0  # reset Tick Count for next bar
                                    self.m_LoggerHandle.info('Tick number is reset to 0')
                                    self.m_liPosition.append(l_iPositionToBeFilled)
                                    self.m_afTempPosition = np.append(self.m_afTempPosition, l_fTempToBeFilled)
                                    l_TradeDbCursor.execute(
                                        "Insert into %s (Date, Time, Position,TempPosition) values('%s', '%s', '%s','%s');" % (
                                        self.m_strResultTableName, str(l_strBarDate), str(l_strBarTime),
                                        self.m_liPosition[self.m_iBarNumber - 1],
                                        self.m_afTempPosition[self.m_iBarNumber - 1]))  # Write into DB
                                    l_TradeDbHandle.commit()
                                    self.m_LoggerHandle.info(
                                        'Writing Position in Result Table for Date= %s and Time= %s' % (
                                        l_strBarDate, l_strBarTime))

                    #-----------------------------------------------------------------------------------------

                    #------------------------Check status of Rina File------------------
                    # If there is any discrepancy in Rina File and Signal Table

                    self.m_LoggerHandle.info("Checking if all signals of table were present in Rina File...")

                    if (self.m_iGenerateRina == 1):
                        l_iCurrentDate = int(time.strftime("%Y%m%d")) - 19000000  # Change_C
                        l_iCurrentTime = int(time.strftime("%H%M"))
                        l_iTotalSignalsInRina=-2
                        with open(self.m_strRinaFileName, 'r') as l_RinaFileHandle:
                            l_lLastRinaRow = None
                            for l_lLastRinaRow in csv.reader(l_RinaFileHandle):
                                l_iTotalSignalsInRina+=1

                        if l_iTotalSignalsInSignalTable <> l_iTotalSignalsInRina:
                            if l_iTotalSignalsInRina >0:
                                # Fetch the last row of Rina File
                                self.m_LoggerHandle.info(
                                    "Number of signals in table <> Signals in Rina ... Syncing the 2")
                                self.m_LoggerHandle.info(
                                    "Adding Signals To Rina...")

                                l_iTradeNumLastInRina = int(l_lLastRinaRow[6].strip().strip('"'))
                                l_iDateLastInRina=int(l_lLastRinaRow[1].strip().strip('"'))
                                l_strSignalRemarkLastInRina=l_lLastRinaRow[3].strip().strip('"')


                                if '_LE_' in  l_strSignalRemarkLastInRina.upper() or '_SE_' in  l_strSignalRemarkLastInRina.upper():
                                    self.m_iRinaInternalFlag = 2  # Flag used internally by Write2Rina module
                                    self.m_iTradeNum = l_iTradeNumLastInRina
                                    if '_LE_' in l_strSignalRemarkLastInRina.upper():
                                        self.m_strEnterType = 'buy'  # used internally by Write2Rina module
                                    elif '_SE_' in l_strSignalRemarkLastInRina.upper():
                                        self.m_strEnterType = 'sell'  # used internally by Write2Rina module
                                elif '_LX_' in  l_strSignalRemarkLastInRina.upper() or '_SX_' in  l_strSignalRemarkLastInRina.upper():
                                    self.m_iRinaInternalFlag = 1  # Flag used internally by Write2Rina module
                                    self.m_iTradeNum = l_iTradeNumLastInRina + 1
                                    #If Current time > than session close time, fetch todays data also
                                if l_iCurrentTime > self.m_strSessionCloseTime:
                                    l_TradeDbCursor.execute("Select * from %s where date  > %s;" % (self.m_strSignalTableName, l_iDateLastInRina))  # get Today's signals
                                    self.m_LoggerHandle.info(
                                        "Adding all signals till today")
                                else:
                                    # If Current time < session close time, leave todays data because it will be added at the end of session
                                    l_TradeDbCursor.execute("Select * from %s where date  > %s and date < %s;" % (self.m_strSignalTableName, l_iDateLastInRina, l_iCurrentDate))
                                    self.m_LoggerHandle.info(
                                        "Adding all signals except today's..they will be added at session end...")
                                l_SignalsRead = l_TradeDbCursor.fetchall()
                                  # Trade number Count
                            else:  # IF no signal has come to rina, but signals are there in Signal table
                                self.m_LoggerHandle.info(
                                    "Rina File is Empty...Signal table is not...syncing the two...")
                                if l_iCurrentTime >= self.m_strSessionCloseTime:
                                    l_TradeDbCursor.execute("Select * from %s ;" % (self.m_strSignalTableName))  # get Today's signals
                                    self.m_LoggerHandle.info(
                                        "Adding all signals till today")
                                else:
                                    # If Current time < session close time, leave todays data because it will be added at the end of session
                                    l_TradeDbCursor.execute("Select * from %s where date < %s;" % (self.m_strSignalTableName,l_iCurrentDate))
                                    self.m_LoggerHandle.info(
                                        "Adding all signals except today's..they will be added at session end...")
                                l_SignalsRead = l_TradeDbCursor.fetchall()
                            if (l_SignalsRead):  # if we have signals
                                self.m_LoggerHandle.info('Writing into Rina File.....')
                                self.Write2Rina(l_SignalsRead)  # write into Rina



                self.m_iRestartFlag = 0
                #Closing Connection
                l_TradeDbCursor.close()
                print self.m_iPositionInMarket
                print self.m_iTrailFlag
                print self.m_fTrailPrice
                #======================================================================================================================================


            #-----------Working in Live Mode-----------------

            if self.m_iLiveMode:
                # Try to connect to database
                l_iNoPriceDbConnectionFlag=0
                l_PriceDbHandle= None

                #-------If live, start fetching data for current date and time
                l_iCurrentDate=int(time.strftime("%Y%m%d")) - 19000000   #Change_C
                l_iCurrentTime=int(time.strftime("%H%M"))


                if (l_iCurrentTime < int(self.m_strSessionBeginTime)) or (l_iCurrentTime>int(self.m_strSessionCloseTime)):
                    self.m_LoggerHandle.info("No Session time...sleeping for 20 secs")
                    time.sleep(20)
                    continue


                if (l_iCurrentDate==l_iDateForPreviousTick and l_iCurrentTime==l_iTimeForPreviousTick):
                    self.m_LoggerHandle.info("No new date and time...sleeping for 20 secs")
                    time.sleep(20)
                    continue

                # Opening Connnection
                try:
                    l_PriceDbHandle = self.LoginToPriceDb()  # login into database
                except:
                    self.m_LoggerHandle.info("Exception thrown in db connection")
                    l_iNoPriceDbConnectionFlag=1

                # If u fail to connect (server is off), go to sleep
                # If server is off, it may happen that an exception is thrown or we get nothing in db handle
                if (l_iNoPriceDbConnectionFlag) or not(l_PriceDbHandle):
                    self.m_LoggerHandle.info('failed to connect to price db...trying again after 20 seconds')
                    time.sleep(20)
                    continue
                self.m_LoggerHandle.info('connected to price db')
                l_PriceDbCursor = l_PriceDbHandle.cursor() # since you have got the connection grab a cursor

                #Fetch data where date = current date and time = current time"
                l_PriceDbCursor.execute("select Date, Time, Open, High, Low, Close from %s where Date = %s and Time = %s ;" % (self.m_strPriceTableName, l_iCurrentDate, l_iCurrentTime))  # read 1 line form database
                l_QueryResult = l_PriceDbCursor.fetchall()
                #closing Connection
                l_PriceDbCursor.close()
                # If u dont get data during live mode
                if (not l_QueryResult):
                    # if no data then wait for some time and ping again
                    self.m_LoggerHandle.info('Did not get data for Date %s and Time %s... Going to sleep for 20 seconds.........' %(l_iCurrentDate,l_iCurrentTime))
                    time.sleep(20)
                    continue
                self.m_LoggerHandle.info('Fetched data for Date %s and Time %s...' %(l_iCurrentDate,l_iCurrentTime))
                l_iDateForPreviousTick=l_iCurrentDate
                l_iTimeForPreviousTick=l_iCurrentTime

            #-------If testing, start fetching data from the startdate till the stopdate
            if not self.m_iLiveMode:
                # opening connection
                l_PriceDbHandle = self.LoginToPriceDb()  # login into database
                l_PriceDbCursor = l_PriceDbHandle.cursor()
                l_PriceDbCursor.execute("select Date, Time, Open, High, Low, Close from %s where Date >=%s and Date <=%s LIMIT %s, 1;" % (self.m_strPriceTableName, self.m_strRunStartDate, self.m_strRunStopDate, self.m_iTotalTicks))  # read 1 line form database
                l_QueryResult = l_PriceDbCursor.fetchall()
                l_PriceDbCursor.close()
                #closing connection

                #if u dont get data during test mode
                if (not l_QueryResult) :  # if no data then Exit form loop
                    print "No data Present \n"
                    print "End date " + time.strftime("%x")
                    print "End time " + time.strftime("%X")
                    l_iProgramFlag = 0
                    break

            #-----------We have data for a tick now---------------
            # opening connection
            l_TradeDbHandle = self.LoginToTradeDb()  # login into database
            l_TradeDbCursor= l_TradeDbHandle.cursor()

            l_strTickDate = str(l_QueryResult[0][0])
            #print l_strTickDate
            l_strTickTime = str(l_QueryResult[0][1])
            #print l_strTickTime
            l_fTickOpen = l_QueryResult[0][2]
            l_fTickHigh = l_QueryResult[0][3]
            l_fTickLow = l_QueryResult[0][4]
            l_fTickClose = l_QueryResult[0][5]
            # ------------------------------------------------------------
            if self.m_iVirtualExitAfterRestart == 1:
                # If it would have hit the SL/Trail, Exit NOW
                # Also Skip this tick's count
                self.m_LoggerHandle.info('Skipping 1 tick for bar formation...')
                if self.m_iPositionInMarket == 1:
                    self.m_iPositionInMarket = 0
                    self.m_iTrailFlag = 0
                    self.m_fTrailPrice = 0.0
                    l_TradeDbCursor.execute(
                        "Insert into %s (Date, Time, Price, Tradetype, Qty, Remarks) values('%s', '%s', '%s', '%s', '%s', '%s');" % (
                            self.m_strSignalTableName, l_strTickDate, l_strTickTime, l_fTickClose, 'sell',
                            self.m_iShareQuantity, 'PF_LX_VirtualExit_15_001'))  # Exit Trade
                    l_TradeDbHandle.commit()
                    l_TradeDbCursor.close()
                    self.m_LoggerHandle.info(
                        'Writing into Signal Table for Date= %s & Time =%s' % (l_strTickDate, l_strTickTime))
                    self.m_iVirtualExitAfterRestart = 0
                    continue
                if self.m_iPositionInMarket == -1:
                    self.m_iPositionInMarket = 0
                    self.m_iTrailFlag = 0
                    self.m_fTrailPrice = 0.0
                    l_TradeDbCursor.execute(
                        "Insert into %s (Date, Time, Price, Tradetype, Qty, Remarks) values('%s', '%s', '%s', '%s', '%s', '%s');" % (
                            self.m_strSignalTableName, l_strBarDate, l_strBarTime, l_fTickClose, 'buy',
                            self.m_iShareQuantity, 'PF_SX_VirtualExit_15_001'))  # Exit Trade
                    l_TradeDbHandle.commit()
                    l_TradeDbCursor.close()
                    self.m_LoggerHandle.info(
                        'Writing into Signal Table for Date= %s & Time =%s' % (l_strTickDate, l_strTickTime))
                    self.m_iVirtualExitAfterRestart = 0
                    continue

            l_iTickNumber = l_iTickNumber + 1  # Increment tick number
            self.m_iTotalTicks += 1  # Increment Total Tick number
            self.m_LoggerHandle.info('Tick Number is %s' % (l_iTickNumber))
            l_2dlTickDataMatrix[l_iTickNumber - 1][0] = l_strTickDate
            l_2dlTickDataMatrix[l_iTickNumber - 1][1] = l_strTickTime
            l_2dlTickDataMatrix[l_iTickNumber - 1][2] = l_fTickOpen
            l_2dlTickDataMatrix[l_iTickNumber - 1][3] = l_fTickHigh
            l_2dlTickDataMatrix[l_iTickNumber - 1][4] = l_fTickLow
            l_2dlTickDataMatrix[l_iTickNumber - 1][5] = l_fTickClose

            if (l_iTickNumber == self.m_iBarTimeInterval) or (str(l_strTickTime) == self.m_strSessionCloseTime):  # If BarTimeInterval ticks or Session closing Time then Create the Bar
                l_strBarDate, l_strBarTime = self.CreateOHLC(l_2dlTickDataMatrix[0:l_iTickNumber][:],l_iTickNumber)  # TickNumber takes care of the fact that there can be situation of  Ticks<BarTimeInterval
                l_iTickNumber = 0  # reset Tick Count for next bar
                self.m_LoggerHandle.info('Tick number is reset to 0')
                # Exiting the virtually set trade

                if self.m_iBarNumber < self.m_iTradingWindowSize:
                    self.m_liPosition.append(0)
                    self.m_afTempPosition=np.append(self.m_afTempPosition,0.0)
                    l_TradeDbCursor.execute("Insert into %s (Date, Time, Position,TempPosition) values('%s', '%s', '%s','%s');" % (self.m_strResultTableName, str(l_strBarDate), str(l_strBarTime),self.m_liPosition[self.m_iBarNumber - 1],self.m_afTempPosition[self.m_iBarNumber - 1]))  # Write into DB
                    l_TradeDbHandle.commit()
                    self.m_LoggerHandle.info('Writing Position in Result Table for Date= %s and Time= %s' %(l_strBarDate, l_strBarTime))
                else:
                    self.TradingAlgorithm()
                    l_TradeDbCursor.execute("Insert into %s (Date, Time, Position,TempPosition) values('%s', '%s', '%s','%s');" % (self.m_strResultTableName, str(l_strBarDate), str(l_strBarTime),self.m_liPosition[self.m_iBarNumber - 1],self.m_afTempPosition[self.m_iBarNumber - 1]))  # Write into DB
                    l_TradeDbHandle.commit()
                    self.m_LoggerHandle.info('Writing Position in Result Table for Date= %s and Time= %s' %(l_strBarDate, l_strBarTime))
                #print "BarNumber", self.m_iBarNumber
                #print "Position", self.m_liPosition[self.m_iBarNumber - 1]
                self.SignalGeneration()

            #-------------------TRAILING-------------------------------------------------------
            #--------------Trailing long position----------------------------------------------
            if (self.m_iPositionInMarket == 1 and self.m_iTrailFlag == 1 and l_fTickClose >= self.m_fTrailPrice):  # trail is ON and market is going up (Follow Trail)
                self.m_fTrailPrice = l_fTickClose
            elif (self.m_iPositionInMarket == 1 and self.m_iTrailFlag == 1 and l_fTickClose <= self.m_fTrailPrice - 0.0035 * self.m_fTrailPrice):  # trail is ON and market moved down (Trail Hit)
                self.m_iPositionInMarket = 0
                self.m_iTrailFlag = 0
                self.m_fTrailPrice = 0.0
                l_TradeDbCursor.execute("Insert into %s (Date, Time, Price, Tradetype, Qty, Remarks) values('%s', '%s', '%s', '%s', '%s', '%s');" % (self.m_strSignalTableName, l_strTickDate, l_strTickTime, l_fTickClose, 'sell',self.m_iShareQuantity, 'PF_LX_LTL_15_001'))  #Exit Trade
                l_TradeDbHandle.commit()
                self.m_LoggerHandle.info('Writing into Signal Table for Date= %s & Time =%s' %(l_strTickDate,l_strTickTime))

            elif (self.m_iPositionInMarket == 1 and self.m_iTrailFlag == 0 and l_fTickClose >= self.m_fMarketEnterPrice + self.m_fMarketEnterPrice * 0.01):  # Trail  On
                self.m_iTrailFlag = 1
                self.m_fTrailPrice = l_fTickClose
            #-------------------------------------------------------------------------------------------
            #---------------------Trailing Short position-------------
            if (self.m_iPositionInMarket == -1 and self.m_iTrailFlag == -1 and l_fTickClose <= self.m_fTrailPrice):  # trail is ON and market is going down (Follow Trail)
                     self.m_fTrailPrice = l_fTickClose
            elif (self.m_iPositionInMarket == -1 and self.m_iTrailFlag == -1 and l_fTickClose >= self.m_fTrailPrice + 0.0035 * self.m_fTrailPrice):  # trail is ON and market moved up (Trail Hit)
                    self.m_iPositionInMarket = 0
                    self.m_iTrailFlag = 0
                    self.m_fTrailPrice = 0.0
                    l_TradeDbCursor.execute("Insert into %s (Date, Time, Price, Tradetype, Qty, Remarks) values('%s', '%s', '%s', '%s', '%s', '%s');" % (self.m_strSignalTableName, l_strTickDate, l_strTickTime, l_fTickClose, 'buy',self.m_iShareQuantity, 'PF_SX_STL_15_001'))  # Exit Trade
                    l_TradeDbHandle.commit()
                    self.m_LoggerHandle.info('Writing into Signal Table for Date= %s & Time =%s' %(l_strTickDate,l_strTickTime))

            elif (self.m_iPositionInMarket == -1 and self.m_iTrailFlag == 0 and l_fTickClose <= self.m_fMarketEnterPrice - self.m_fMarketEnterPrice * 0.01):  # Trail On
                self.m_iTrailFlag = -1
                self.m_fTrailPrice = l_fTickClose

            #-----------------------------------------------------------------------------------------------------------------%
            #-------------------STOPLOSS------------------------%
            #---------------for Long position-------------------%
            if ((self.m_iPositionInMarket == 1) and (l_fTickClose <= (self.m_fMarketEnterPrice - 0.0025 * self.m_fMarketEnterPrice))):  # Stop Loss
                self.m_iPositionInMarket = 0
                self.m_iTrailFlag = 0
                self.m_fTrailPrice = 0.0
                l_TradeDbCursor.execute("Insert into %s (Date, Time, Price, Tradetype, Qty, Remarks) values('%s', '%s', '%s', '%s', '%s', '%s');" % (self.m_strSignalTableName, l_strTickDate, l_strTickTime, l_fTickClose, 'sell',self.m_iShareQuantity, 'PF_LX_LSL_15_001'))  # Exit Trade
                l_TradeDbHandle.commit()
                self.m_LoggerHandle.info('Writing into Signal Table for Date= %s & Time =%s' %(l_strTickDate,l_strTickTime))

            #---------------for short position------------------%
            if ((self.m_iPositionInMarket == -1) and (l_fTickClose >= (self.m_fMarketEnterPrice + 0.0025 * self.m_fMarketEnterPrice))):
                self.m_iPositionInMarket = 0
                self.m_iTrailFlag = 0
                self.m_fTrailPrice = 0.0
                l_TradeDbCursor.execute("Insert into %s (Date, Time, Price, Tradetype, Qty, Remarks) values('%s', '%s', '%s', '%s', '%s', '%s');" % (self.m_strSignalTableName, l_strTickDate, l_strTickTime, l_fTickClose, 'buy',self.m_iShareQuantity, 'PF_SX_SSL_15_001'))  # Exit Trade
                l_TradeDbHandle.commit()
                self.m_LoggerHandle.info('Writing into Signal Table for Date= %s & Time =%s' %(l_strTickDate,l_strTickTime))

            #------------------------------------------------------%
            if (l_strTickTime == self.m_strSessionCloseTime):  # reached end of the day
                if (self.m_iGenerateRina == 1):
                    l_TradeDbCursor.execute("Select * from %s where date  = %s;" % (self.m_strSignalTableName, int(l_strTickDate)))  # get Today's signals
                    l_SignalsRead = l_TradeDbCursor.fetchall()
                    if (l_SignalsRead):  # if we have signals
                        self.m_LoggerHandle.info('Writing into Rina File for Date= %s & Time =%s' %(l_strTickDate,l_strTickTime))
                        self.Write2Rina(l_SignalsRead)  # write into Rina

            #--------------Change_B2-----------------
            # if no of bars reaches 5*TradingWindowSize flush the data
            if (l_iTickNumber == self.m_iBarTimeInterval) or (str(l_strTickTime) == self.m_strSessionCloseTime):  # If BarTimeInterval ticks or Session closing Time then Create the Bar
                if self.m_iBarNumber == 5*self.m_iTradingWindowSize:
                    self.DataFlush()

            l_TradeDbCursor.close()
            #Closing Connection




#------------------End Of Class-------------------------------
#=============================Main Function==========================
#l_strLogFile= 'LogFile'
# If u want logging info to both console and file then uncomment below
#l_strLogFile=''
#logging.basicConfig(filename=l_strLogFile,level=logging.DEBUG)

#-----------CHANGE 4---------------------
l_strConfFile='ConfFile1c.txt'  # Please see the change in Configuration file
l_oTraderObject1 = Trader(l_strConfFile)  # Trader Instance
l_oTraderObject1.Trading()
#-----------End of CHANGE 4-------------
'''
l_strConfFile='ConfFile2.txt'
l_oTraderObject2 = Trader(l_strConfFile)  # Trader Instance
l_oTraderObject2.Trading()


l_strConfFile='ConfFile3.txt'
l_oTraderObject3 = Trader(l_strConfFile)  # Trader Instance
l_oTraderObject3.Trading()


l_strConfFile='ConfFile4.txt'
l_oTraderObject4 = Trader(l_strConfFile)  # Trader Instance
l_oTraderObject4.Trading()
'''

