from __future__ import division

__author__ = 'pawasgupta'

# ----------Modules Required---------
import MySQLdb as mariadb
import numpy as np
import math
import time
import ConfigParser

#-------------Class Defination Start---------


class Trader:

    def __init__(self):  # constructor to initialise the members

        self.m_strDbName = ''  # Name of Database
        self.m_strUserName = ''  # username
        self.m_strPassword = ''  # Password
        self.m_strDataTableName = ''  # name of Tick Data Table
        self.m_strResultTableName = ''  # name of Results Table (Contains Date,  Time,  Position)
        self.m_strSignalTableName = ''  # name of Signal Table
        self.m_strOHLCTableName = ''  # name of Signal Table
        self.m_strRinaFileName = ""  # Rina File name

        self.m_iBarTimeInterval = 15  # Bar Size
        self.m_strSessionCloseTime = "17:00"  # Session Close Time (Should have same format as Tick_Time in DataTable)

        self.m_strRunStartDate=''
        self.m_strRunStopDate=''

        self.m_iTotalTicks = 0  # Counts the Total Ticks
        self.m_iBarNumber = 0  # Time Instant or  Bar Count
        self.m_2dlfOHLCMatrix = []  # Contains Date, Time, O, H, L, C for Each bar
        self.m_2dlfNonRoundedClose = []

        #-----Parameters of my Algo------
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
        self.m_iShareQuantity = 10000  # Share Quantity
        self.m_iPositionInMarket = 0  # variable that stores current position in market
        self.m_fMarketEnterPrice = 0.0  # trade Entry Price (used in Trailing and Stoploss)

        self.m_iRinaInternalFlag = 1  # Flag used internally by Write2Rina module
        self.m_iGenerateRina = 1  # Make 0 if u don't want to write 2 Rina
        self.m_iTradeNum = 1  # Trade number Count
        self.m_strEnterType = ''  # used internally by Write2Rina module


    # =================== Member Functions =========================== 

    def ReadConfFile(self,l_strConfFile):

        l_oConfigFileObbject=ConfigParser.ConfigParser()
        l_oConfigFileObbject.read(l_strConfFile)
        self.m_strDbName = l_oConfigFileObbject.get('SectionConf', 'DbName')  # Name of Database
        self.m_strUserName = l_oConfigFileObbject.get('SectionConf', 'UserName')  # username
        self.m_strPassword = l_oConfigFileObbject.get('SectionConf', 'Password')  # Password
        self.m_strDataTableName = l_oConfigFileObbject.get('SectionConf', 'DataTable')  # name of Tick Data Table
        self.m_strResultTableName = l_oConfigFileObbject.get('SectionConf', 'ResultTable')  # name of Results Table (Contains Date,  Time,  Position)
        self.m_strSignalTableName = l_oConfigFileObbject.get('SectionConf', 'SignalTable')  # name of Signal Table
        self.m_strOHLCTableName = l_oConfigFileObbject.get('SectionConf', 'OHLCTable')  # name of OHLC Table
        self.m_strRinaFileName = l_oConfigFileObbject.get('SectionConf', 'RinaFileName')  # Rina File name

        self.m_iBarTimeInterval = l_oConfigFileObbject.getint('SectionConf', 'BarSize')  # Bar Size
        self.m_strSessionCloseTime = l_oConfigFileObbject.get('SectionConf', 'SessionEnd')  # Session Close Time (Should have same format as Tick_Time in DataTable)
        self.m_strRunStartDate = l_oConfigFileObbject.get('SectionConf', 'RunStartDate')
        self.m_strRunStopDate = l_oConfigFileObbject.get('SectionConf', 'RunStopDate')


    def Login(self):  # login into the database
        l_Db = mariadb.connect(user=self.m_strUserName, passwd=self.m_strPassword, db=self.m_strDbName)
        return l_Db

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

    def CreateTable(self):  # Create Required Tables
        l_DbHandle = self.Login()
        l_Cursor = l_DbHandle.cursor()
        l_Cursor.execute("show tables like '%s'" % (self.m_strResultTableName))
        l_QueryResult = l_Cursor.fetchall()
        if not l_QueryResult:  # Create table if it does not exist
            l_Cursor.execute("create table %s (Date text NOT NULL,  Time text NOT NULL,  Position int NOT NULL)" % (self.m_strResultTableName))
        else:  # If it Exits,  Delete data present in table
            l_Cursor.execute("delete from %s;" % (self.m_strResultTableName))
            l_DbHandle.commit()

        l_Cursor.execute("show tables like '%s'" % (self.m_strOHLCTableName))
        l_QueryResult = l_Cursor.fetchall()
        if not l_QueryResult:  # Create table if it does not exist
            l_Cursor.execute("create table %s (Date text NOT NULL,  Time text NOT NULL,  Open real NOT NULL,  High real NOT NULL,  Low real NOT NULL,  Close real NOT NULL)" % (self.m_strOHLCTableName))
        else:  #If it Exits,  Delete data present in table
            l_Cursor.execute("delete from %s;" % (self.m_strOHLCTableName))
            l_DbHandle.commit()

        l_Cursor.execute("show tables like '%s'" % (self.m_strSignalTableName))
        l_QueryResult = l_Cursor.fetchall()
        if not l_QueryResult:  # Create table if it does not exist
            l_Cursor.execute("create table %s (Date text NOT NULL, Time text NOT NULL, Price Real NOT NULL, TradeType text NOT NULL, Qty int NOT NULL, Remarks text)" % (self.m_strSignalTableName))
        else:  #Else Delete data present in table
            l_Cursor.execute("delete from %s;" % (self.m_strSignalTableName))
            l_DbHandle.commit()
        l_Cursor.close()

    def CreateRinaFile(self):
        l_FileId = open(self.m_strRinaFileName, "w")  # create file if it does not exist else flush the content
        l_FileId.write('"Trade #", "Date", "Time", "Signal", "Price", "Contracts", "% Profit", "Runup", "Entry Eff", "Total", "System"\r\n')
        l_FileId.write('"Type", "Date", "Time", "Signal", "Price", "Profit", "Cum Profit", "Drawdown", "Exit Eff", "Eff", "Market"\r\n')
        l_FileId.close()


    def CreateOHLC(self, l_2dlTickDataMatrix, l_iTickNumber):
        l_DbHandle = self.Login()  # login into database
        l_Cursor = l_DbHandle.cursor()

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

        l_Cursor.execute("Insert into %s (Date, Time, Open, High, Low, Close) values('%s', '%s', '%s', '%s', '%s', '%s');" % (l_oTraderObject.m_strOHLCTableName, str(l_strBarDate), str(l_strBarTime), l_fBarOpen, l_fBarHigh, l_fBarLow,l_fBarClose))  # Write into DB
        l_DbHandle.commit()
        l_Cursor.close()

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

        if (self.m_iBarNumber == self.m_iTradingWindowSize):  # Initialise during 1st call
            self.m_afProfit = np.zeros(self.m_iTradingWindowSize - 1, dtype='float64')
            self.m_afCumulativeProfit = np.zeros(self.m_iTradingWindowSize - 1, dtype='float64')
            self.m_afTempPosition = np.zeros(self.m_iTradingWindowSize - 1, dtype='float64')

            self.m_2dafFeatureMatrix = np.zeros((self.m_iBarsBack, self.m_iTradingWindowSize), dtype='float64')

            self.m_afReturns = np.zeros(self.m_iTradingWindowSize - 1, dtype='float64')
            l_iIndex = 1
            while (l_iIndex < self.m_iTradingWindowSize - 1):  # loop to assign all the values of r,  cannot put abcd assignment here because it has to be in the same loop as the weight update
                self.m_afReturns[l_iIndex] = l_afClosePrice[l_iIndex] - l_afClosePrice[l_iIndex - 1]
                l_iIndex = l_iIndex + 1

            self.m_2dafWeights = np.zeros((self.m_iBarsBack, self.m_iTradingWindowSize - 1), dtype='float64')


        # ---appending values corresponding to each bar-------
        #  ---append in each call---------------------
        self.m_2dafFeatureMatrix = np.hstack((self.m_2dafFeatureMatrix, np.reshape(l_afFeatureVector[self.m_iBarNumber - self.m_iBarsBack:self.m_iBarNumber],[-1, 1])))  # appending a column to Feature Matrix
        #tot = np.sum(self.m_2dafFeatureMatrix, axis = 0, dtype = 'float64')
        #self.m_2dafFeatureMatrix[:, self.m_iBarNumber] = self.m_2dafFeatureMatrix[:, self.m_iBarNumber]/tot[self.m_iBarNumber]

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

        self.m_afProfit[self.m_iBarNumber - 1] = self.m_liPosition[self.m_iBarNumber - 2] * (l_afClosePrice[self.m_iBarNumber - 1] - l_afClosePrice[self.m_iBarNumber - 2]) - 0.0035 * abs(self.m_liPosition[self.m_iBarNumber - 1] - self.m_liPosition[self.m_iBarNumber - 2])  # instantaneous profit

        self.m_afCumulativeProfit[self.m_iBarNumber - 1] = self.m_afCumulativeProfit[self.m_iBarNumber - 2] + self.m_afProfit[self.m_iBarNumber - 1]  # Cumulative Profit


    def SignalGeneration(self):
        l_strBarDate = str(self.m_2dlfOHLCMatrix[self.m_iBarNumber - 1][0])
        l_strBarTime = str(self.m_2dlfOHLCMatrix[self.m_iBarNumber - 1][1])

        l_fBarClose = float(self.m_2dlfNonRoundedClose[self.m_iBarNumber - 1][0])

        l_DbHandle = self.Login()  # login into database
        l_Cursor = l_DbHandle.cursor()

        if (self.m_liPosition[self.m_iBarNumber - 2] == 0 and self.m_liPosition[self.m_iBarNumber - 1] == -1 and self.m_iPositionInMarket == 0):  # Generate EnterShort1 Signal (Case 1)
            l_Cursor.execute("Insert into %s (Date, Time, Price, Tradetype, Qty, Remarks) values('%s', '%s', '%s', '%s', '%s', '%s');" % (self.m_strSignalTableName, l_strBarDate, l_strBarTime, l_fBarClose, 'sell', self.m_iShareQuantity,'Enter_Short1'))  # Write into DB
            l_DbHandle.commit()
            self.m_iTradeType = 1
            self.m_fMarketEnterPrice = l_fBarClose
            self.m_iPositionInMarket = -1

        if (self.m_liPosition[self.m_iBarNumber - 2] == 0 and self.m_liPosition[self.m_iBarNumber - 1] == 1 and self.m_iPositionInMarket == 0):  # Generate EnterLong1 Signal (Case 2)
            l_Cursor.execute("Insert into %s (Date, Time, Price, Tradetype, Qty, Remarks) values('%s', '%s', '%s', '%s', '%s', '%s');" % (self.m_strSignalTableName, l_strBarDate, l_strBarTime, l_fBarClose, 'buy', self.m_iShareQuantity,'Enter_Long1'))  # Write into DB
            l_DbHandle.commit()
            self.m_iTradeType = 1
            self.m_fMarketEnterPrice = l_fBarClose
            self.m_iPositionInMarket = 1

        if (self.m_liPosition[self.m_iBarNumber - 2] == 1 and self.m_liPosition[self.m_iBarNumber - 1] == -1 and self.m_iTradeType == 1 and self.m_iPositionInMarket == 1):  # clear off your Long1 position (Case3)
            l_Cursor.execute("Insert into %s (Date, Time, Price, Tradetype, Qty, Remarks) values('%s', '%s', '%s', '%s', '%s', '%s');" % (self.m_strSignalTableName, l_strBarDate, l_strBarTime, l_fBarClose, 'sell', self.m_iShareQuantity,'Long_Exit1'))  # Write into DB
            l_DbHandle.commit()
            self.m_iPositionInMarket = 0
            self.m_fTrailPrice = 0.0
            self.m_iTrailFlag = 0

        if (self.m_liPosition[self.m_iBarNumber - 2] == 1 and self.m_liPosition[self.m_iBarNumber - 1] == -1 and self.m_iTradeType == 2 and self.m_iPositionInMarket == 1):  # clear off your Long2 position (Case 4)
            l_Cursor.execute("Insert into %s (Date, Time, Price, Tradetype, Qty, Remarks) values('%s', '%s', '%s', '%s', '%s', '%s');" % (self.m_strSignalTableName, l_strBarDate, l_strBarTime, l_fBarClose, 'sell', self.m_iShareQuantity,'Long_Exit2'))  # Write into DB
            l_DbHandle.commit()
            self.m_iPositionInMarket = 0
            self.m_fTrailPrice = 0.0
            self.m_iTrailFlag = 0

        if (self.m_liPosition[self.m_iBarNumber - 2] == 1 and self.m_liPosition[self.m_iBarNumber - 1] == -1 and self.m_iPositionInMarket == 0):  # Generate a Enter Short2 signal (Case 5)
            l_Cursor.execute("Insert into %s (Date, Time, Price, Tradetype, Qty, Remarks) values('%s', '%s', '%s', '%s', '%s', '%s');" % (self.m_strSignalTableName, l_strBarDate, l_strBarTime, l_fBarClose, 'sell', self.m_iShareQuantity,'Enter_Short2'))  # Write into DB
            l_DbHandle.commit()
            self.m_iTradeType == 2
            self.m_fMarketEnterPrice = l_fBarClose
            self.m_iPositionInMarket = -1

        if (self.m_liPosition[self.m_iBarNumber - 2] == -1 and self.m_liPosition[self.m_iBarNumber - 1] == 1 and self.m_iTradeType == 1 and self.m_iPositionInMarket == -1):  # clear off your Short1 position (Case 6)
            l_Cursor.execute("Insert into %s (Date, Time, Price, Tradetype, Qty, Remarks) values('%s', '%s', '%s', '%s', '%s', '%s');" % (self.m_strSignalTableName, l_strBarDate, l_strBarTime, l_fBarClose, 'buy', self.m_iShareQuantity,'Short_Exit1'))  # Write into DB
            l_DbHandle.commit()
            self.m_iPositionInMarket = 0
            self.m_fTrailPrice = 0.0
            self.m_iTrailFlag = 0

        if (self.m_liPosition[self.m_iBarNumber - 2] == -1 and self.m_liPosition[self.m_iBarNumber - 1] == 1 and self.m_iTradeType == 2 and self.m_iPositionInMarket == -1):  # clear off your Short2 position (Case 7)
            l_Cursor.execute("Insert into %s (Date, Time, Price, Tradetype, Qty, Remarks) values('%s', '%s', '%s', '%s', '%s', '%s');" % (self.m_strSignalTableName, l_strBarDate, l_strBarTime, l_fBarClose, 'buy', self.m_iShareQuantity,'Short_Exit2'))  # Write into DB
            l_DbHandle.commit()
            self.m_iPositionInMarket = 0
            self.m_fTrailPrice = 0.0
            self.m_iTrailFlag = 0

        if (self.m_liPosition[self.m_iBarNumber - 2] == -1 and self.m_liPosition[self.m_iBarNumber - 1] == 1 and self.m_iPositionInMarket == 0):  # Generate a EnterLong2 signal (Case 8)
            l_Cursor.execute("Insert into %s (Date, Time, Price, Tradetype, Qty, Remarks) values('%s', '%s', '%s', '%s', '%s', '%s');" % (self.m_strSignalTableName, l_strBarDate, l_strBarTime, l_fBarClose, 'buy', self.m_iShareQuantity,'Enter_Long2'))  # Write into DB
            l_DbHandle.commit()
            self.m_iTradeType == 2
            self.m_fMarketEnterPrice = l_fBarClose
            self.m_iPositionInMarket = 1

        if (self.m_liPosition[self.m_iBarNumber - 2] == -1 and self.m_liPosition[self.m_iBarNumber - 1] == 0 and self.m_iTradeType == 1 and self.m_iPositionInMarket == -1):  #clear off your Short1 position (Case 9)
            l_Cursor.execute("Insert into %s (Date, Time, Price, Tradetype, Qty, Remarks) values('%s', '%s', '%s', '%s', '%s', '%s');" % (self.m_strSignalTableName, l_strBarDate, l_strBarTime, l_fBarClose, 'buy', self.m_iShareQuantity,'Short_Exit3'))  # Write into DB
            l_DbHandle.commit()
            self.m_iPositionInMarket = 0
            self.m_fTrailPrice = 0.0
            self.m_iTrailFlag = 0

        if (self.m_liPosition[self.m_iBarNumber - 2] == -1 and self.m_liPosition[self.m_iBarNumber - 1] == 0 and self.m_iTradeType == 2 and self.m_iPositionInMarket == -1):  # clear off your Short2 position(Case 10)
            l_Cursor.execute("Insert into %s (Date, Time, Price, Tradetype, Qty, Remarks) values('%s', '%s', '%s', '%s', '%s', '%s');" % (self.m_strSignalTableName, l_strBarDate, l_strBarTime, l_fBarClose, 'buy', self.m_iShareQuantity,'Short_Exit4'))  # Write into DB
            l_DbHandle.commit()
            self.m_iPositionInMarket = 0
            self.m_fTrailPrice = 0.0
            self.m_iTrailFlag = 0

        if (self.m_liPosition[self.m_iBarNumber - 2] == 1 and self.m_liPosition[self.m_iBarNumber - 1] == 0 and self.m_iTradeType == 1 and self.m_iPositionInMarket == 1):  #clear off your Long1 position (Case 11)
            l_Cursor.execute("Insert into %s (Date, Time, Price, Tradetype, Qty, Remarks) values('%s', '%s', '%s', '%s', '%s', '%s');" % (self.m_strSignalTableName, l_strBarDate, l_strBarTime, l_fBarClose, 'sell', self.m_iShareQuantity,'Long_Exit3'))  # Write into DB
            l_DbHandle.commit()
            self.m_iPositionInMarket = 0
            self.m_fTrailPrice = 0.0
            self.m_iTrailFlag = 0

        if (self.m_liPosition[self.m_iBarNumber - 2] == 1 and self.m_liPosition[self.m_iBarNumber - 1] == 0 and self.m_iTradeType == 2 and self.m_iPositionInMarket == 1):  #clear off Long2 your position (Case 12)
            l_Cursor.execute("Insert into %s (Date, Time, Price, Tradetype, Qty, Remarks) values('%s', '%s', '%s', '%s', '%s', '%s');" % (self.m_strSignalTableName, l_strBarDate, l_strBarTime, l_fBarClose, 'sell', self.m_iShareQuantity,'Long_Exit4'))  # Write into DB
            l_DbHandle.commit()
            self.m_iPositionInMarket = 0
            self.m_fTrailPrice = 0.0
            self.m_iTrailFlag = 0

        l_Cursor.close()


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
            l_iCounter = 0;
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


#------------------End Of Class-------------------------------

#=============================Main Function========================== 

l_oTraderObject = Trader()  # Trader Instance
#------------ local variables-----------
l_strConfFile='ConfFile.txt'
l_oTraderObject.ReadConfFile(l_strConfFile)
l_iProgramFlag = 1  # loop variable
l_iTickNumber = 0  # Count the ticks for Bar Creation
l_strTickDate = ''
l_strTickTime = ''
l_fTickOpen = 0.0
l_fTickHigh = 0.0
l_fTickLow = 0.0
l_fTickClose = 0.0
l_2dlTickDataMatrix = [[None for _ in range(6)] for _ in range(l_oTraderObject.m_iBarTimeInterval)]  # Creating a matrix for tick data
l_DbHandle = l_oTraderObject.Login()  # login into database
l_Cursor = l_DbHandle.cursor()

l_oTraderObject.CreateTable()  # Create tables
l_oTraderObject.CreateRinaFile()  # Create Rina file with header

print "Start date " + time.strftime("%x")
print "Start time " + time.strftime("%X")

while (l_iProgramFlag == 1):
    #---------------Read Tick data and create OHLC matrix----------------------------------

    l_Cursor.execute("select TICK_DATE, TICK_TIME, OPEN, HIGH, LOW, CLOSE from %s where TICK_DATE >='%s' and TICK_DATE <='%s' LIMIT %s, 1;" % (l_oTraderObject.m_strDataTableName, l_oTraderObject.m_strRunStartDate, l_oTraderObject.m_strRunStopDate, l_oTraderObject.m_iTotalTicks))  # read 1 line form database
    l_QueryResult = l_Cursor.fetchall()
    if not l_QueryResult:  # if no data then Exit form loop
        print "No data Present \n"
        print "End date " + time.strftime("%x")
        print "End time " + time.strftime("%X")
        l_iProgramFlag = 0
        break

    else:
        l_iTickNumber = l_iTickNumber + 1  # Increment tick number
        l_oTraderObject.m_iTotalTicks += 1  # Increment Total Tick number
        l_strTickDate = l_QueryResult[0][0]
        l_strTickTime = l_QueryResult[0][1]
        l_fTickOpen = l_QueryResult[0][2]
        l_fTickHigh = l_QueryResult[0][3]
        l_fTickLow = l_QueryResult[0][4]
        l_fTickClose = l_QueryResult[0][5]
        l_2dlTickDataMatrix[l_iTickNumber - 1][0] = l_strTickDate
        l_2dlTickDataMatrix[l_iTickNumber - 1][1] = l_strTickTime
        l_2dlTickDataMatrix[l_iTickNumber - 1][2] = l_fTickOpen
        l_2dlTickDataMatrix[l_iTickNumber - 1][3] = l_fTickHigh
        l_2dlTickDataMatrix[l_iTickNumber - 1][4] = l_fTickLow
        l_2dlTickDataMatrix[l_iTickNumber - 1][5] = l_fTickClose

        if (l_iTickNumber == l_oTraderObject.m_iBarTimeInterval) or (str(l_strTickTime) == l_oTraderObject.m_strSessionCloseTime):  # If BarTimeInterval ticks or Session closing Time then Create the Bar
            l_strBarDate, l_strBarTime = l_oTraderObject.CreateOHLC(l_2dlTickDataMatrix[0:l_iTickNumber][:],l_iTickNumber)  # TickNumber takes care of the fact that there can be situation of  Ticks<BarTimeInterval
            l_iTickNumber = 0  # reset Tick Count for next bar
            if l_oTraderObject.m_iBarNumber < l_oTraderObject.m_iTradingWindowSize:
                l_oTraderObject.m_liPosition.append(0)
            else:
                l_oTraderObject.TradingAlgorithm()

            l_Cursor.execute("Insert into %s (Date, Time, Position) values('%s', '%s', '%s');" % (l_oTraderObject.m_strResultTableName, str(l_strBarDate), str(l_strBarTime),l_oTraderObject.m_liPosition[l_oTraderObject.m_iBarNumber - 1]))  # Write into DB
            l_DbHandle.commit()

            #print str(l_strBarDate)
            #print str(l_strBarTime)
            #print l_oTraderObject.m_liPosition[l_oTraderObject.m_iBarNumber-1]

            l_oTraderObject.SignalGeneration()

        #-------------------TRAILING-------------------------------------------------------
        #--------------Trailing long position----------------------------------------------
        if (l_oTraderObject.m_iPositionInMarket == 1 and l_oTraderObject.m_iTrailFlag == 1 and l_fTickClose >= l_oTraderObject.m_fTrailPrice):  # trail is ON and market is going up (Follow Trail)
            l_oTraderObject.m_fTrailPrice = l_fTickClose
        elif (l_oTraderObject.m_iPositionInMarket == 1 and l_oTraderObject.m_iTrailFlag == 1 and l_fTickClose <= l_oTraderObject.m_fTrailPrice - 0.0035 * l_oTraderObject.m_fTrailPrice):  # trail is ON and market moved down (Trail Hit)
            l_oTraderObject.m_iPositionInMarket = 0
            l_oTraderObject.m_iTrailFlag = 0
            l_oTraderObject.m_fTrailPrice = 0.0
            l_Cursor.execute("Insert into %s (Date, Time, Price, Tradetype, Qty, Remarks) values('%s', '%s', '%s', '%s', '%s', '%s');" % (l_oTraderObject.m_strSignalTableName, l_strTickDate, l_strTickTime, l_fTickClose, 'sell',l_oTraderObject.m_iShareQuantity, 'Long_TL_Hit'))  #Exit Trade
            l_DbHandle.commit()

        elif (l_oTraderObject.m_iPositionInMarket == 1 and l_oTraderObject.m_iTrailFlag == 0 and l_fTickClose >= l_oTraderObject.m_fMarketEnterPrice + l_oTraderObject.m_fMarketEnterPrice * 0.01):  # Trail  On
            l_oTraderObject.m_iTrailFlag = 1
            l_oTraderObject.m_fTrailPrice = l_fTickClose
            #-------------------------------------------------------------------------------------------

        #---------------------Trailing Short position-------------
        if (l_oTraderObject.m_iPositionInMarket == -1 and l_oTraderObject.m_iTrailFlag == -1 and l_fTickClose <= l_oTraderObject.m_fTrailPrice):  # trail is ON and market is going down (Follow Trail)
            l_oTraderObject.m_fTrailPrice = l_fTickClose
        elif (l_oTraderObject.m_iPositionInMarket == -1 and l_oTraderObject.m_iTrailFlag == -1 and l_fTickClose >= l_oTraderObject.m_fTrailPrice + 0.0035 * l_oTraderObject.m_fTrailPrice):  # trail is ON and market moved up (Trail Hit)
            l_oTraderObject.m_iPositionInMarket = 0
            l_oTraderObject.m_iTrailFlag = 0
            l_oTraderObject.m_fTrailPrice = 0.0
            l_Cursor.execute("Insert into %s (Date, Time, Price, Tradetype, Qty, Remarks) values('%s', '%s', '%s', '%s', '%s', '%s');" % (l_oTraderObject.m_strSignalTableName, l_strTickDate, l_strTickTime, l_fTickClose, 'buy',l_oTraderObject.m_iShareQuantity, 'Short_TL_Hit'))  # Exit Trade
            l_DbHandle.commit()


        elif (l_oTraderObject.m_iPositionInMarket == -1 and l_oTraderObject.m_iTrailFlag == 0 and l_fTickClose <= l_oTraderObject.m_fMarketEnterPrice - l_oTraderObject.m_fMarketEnterPrice * 0.01):  # Trail On
            l_oTraderObject.m_iTrailFlag = -1
            l_oTraderObject.m_fTrailPrice = l_fTickClose

    #-----------------------------------------------------------------------------------------------------------------%

    #-------------------STOPLOSS------------------------%
    #---------------for Long position-------------------%
    if ((l_oTraderObject.m_iPositionInMarket == 1) and (l_fTickClose <= (l_oTraderObject.m_fMarketEnterPrice - 0.0025 * l_oTraderObject.m_fMarketEnterPrice))):  # Stop Loss
        l_oTraderObject.m_iPositionInMarket = 0
        l_oTraderObject.m_iTrailFlag = 0
        l_oTraderObject.m_fTrailPrice = 0.0
        l_Cursor.execute("Insert into %s (Date, Time, Price, Tradetype, Qty, Remarks) values('%s', '%s', '%s', '%s', '%s', '%s');" % (l_oTraderObject.m_strSignalTableName, l_strTickDate, l_strTickTime, l_fTickClose, 'sell',l_oTraderObject.m_iShareQuantity, 'Long_SL_Hit'))  # Exit Trade
        l_DbHandle.commit()

    #---------------for short position------------------%
    if ((l_oTraderObject.m_iPositionInMarket == -1) and (l_fTickClose >= (l_oTraderObject.m_fMarketEnterPrice + 0.0025 * l_oTraderObject.m_fMarketEnterPrice))):
        l_oTraderObject.m_iPositionInMarket = 0
        l_oTraderObject.m_iTrailFlag = 0
        l_oTraderObject.m_fTrailPrice = 0.0
        l_Cursor.execute("Insert into %s (Date, Time, Price, Tradetype, Qty, Remarks) values('%s', '%s', '%s', '%s', '%s', '%s');" % (l_oTraderObject.m_strSignalTableName, l_strTickDate, l_strTickTime, l_fTickClose, 'buy',l_oTraderObject.m_iShareQuantity, 'Short_SL_Hit'))  # Exit Trade
        l_DbHandle.commit()

    #------------------------------------------------------%

    if (l_strTickTime == l_oTraderObject.m_strSessionCloseTime):  # reached end of the day
        if (l_oTraderObject.m_iGenerateRina == 1):
            l_Cursor.execute("Select * from %s where date  = '%s';" % (l_oTraderObject.m_strSignalTableName, str(l_strTickDate)))  # get Today's signals
            l_SignalsRead = l_Cursor.fetchall()
            if (l_SignalsRead):  # if we have signals
                l_oTraderObject.Write2Rina(l_SignalsRead)  # write into Rina

l_Cursor.close()


