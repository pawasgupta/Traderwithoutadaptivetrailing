from __future__ import division
__author__ = 'pawasgupta'

#----------Modules Required---------
import MySQLdb as mariadb
import numpy as np
import math
import time

#-------------Class Defination Start---------
class Trader:
    def __init__(self):         #constructor to initialise the members

        self.m_str_dbname='test_db1'    # Name of Database
        self.m_str_DataTableName='data_tablePeriod2'    # name of Tick Data Table
        self.m_str_ResultTableName='tbl_TrainingResultsPeriod2' # name of Results Table (Contains Date, Time, Position)
        self.m_str_SignalTableName='tbl_TrainingSignalsPeriod2' # name of Signal Table
        self.m_str_OHLCTableName='tbl_OHLCPeriod2' # name of Signal Table

        self.m_str_UserName='pawasgupta'     # username
        self.m_str_Password='@1234' # Password
        self.m_i_BarTimeInterval=15 # Bar Size
        self.m_i_TotalTicks=0   # Counts the Total Ticks
        self.m_str_SessionCloseTime="17:00" # Session Close Time (Shoul have same format as Tick_Time in DataTable)
        self.m_i_t=0 # Time Instant or  Bar Count
        self.m_f_OHLCMatrix=[] # Contains Date,Time,O,H,L,C for Each bar

        #-----Parameters of my Algo------
        self.m_i_TradingWindowSize=80
        self.m_f_A=1000
        self.m_f_Alpha=100
        self.m_f_betaa=60
        self.m_f_gamma=5
        self.m_f_dell=0.5
        self.m_i_BarsBack=25
        self.m_f_Threshold=0.3

        self.m_i_Position=[] #vector storing Postions for each 't'
        self.m_f_Weights=[] #Matrix storing Weights for each 't'
        self.m_f_TempPosition=[]    # vector for TempPostion for each 't'
        self.m_f_Profit=[]  #Vector for Profit for each 't'
        self.m_f_CumulativeProfit=[]    #Vector for Cumulative Postion for each 't'
        self.m_f_FeatureMatrix=[]   #Matrix for Features
        self.m_f_Returns=[] #MVector Storing Returns for each 't'

        self.m_i_TradeType=0    # variable for remarks for trades in Tradesheet
        self.m_i_ShareQuantity=10000 # Share Quantity
        self.m_i_PositionInMarket=0 # variable that stores current position in market
        self.m_f_MarketEnterPrice=0.0   # trade Entry Price (used in Trailing and Stoploss)

        self.m_i_RinaInternalFlag=1 # Flag used internally by Write2Rina module
        self.m_i_GenerateRina=1 # Make 0 if u dont want to write 2 Rina
        self.m_str_RinaFileName="RinaCSVOutputPeriod2" # Rina File name
        self.m_i_TradeNum=1 # Trade number Count
        self.m_str_EnterType='' # used internally by Write2Rina module


    #========Member Functions=========================

    def m_login(self):     #login into the database
        db=mariadb.connect(user=self.m_str_UserName, passwd=self.m_str_Password, db=self.m_str_dbname)
        return db

    def m_MovingAverage(self,series,window):    #Computes Moving Average
        sz=len(series)
        average=np.zeros(sz,dtype='float64')
        n=np.repeat(1.0,window)/window
        average[0:window-1]=0
        average[window-1:sz]=np.convolve(series,n,'valid')
        return average

    def m_kernel(self,a,b): #Kernel Function (Using RBF kernel)
        nrm=np.linalg.norm(a-b)
        sigma=1
        return(math.exp((-1*(nrm**2))/(2*sigma*sigma)))


    def m_CreateTable(self): #Create Required Tables
        l_dbHandle=self.m_login()
        l_cur=l_dbHandle.cursor()
        l_cur.execute("show tables like '%s'"% (self.m_str_ResultTableName))
        l_QueryResult=l_cur.fetchall()
        if not l_QueryResult:   # Create table if it does not exist
            l_cur.execute("create table %s (Date text NOT NULL, Time text NOT NULL, Position int NOT NULL)"%(self.m_str_ResultTableName))
        else:          #If it Exits, Delete data present in table
            l_cur.execute("delete from %s;"% (self.m_str_ResultTableName))
            l_dbHandle.commit()


        l_cur.execute("show tables like '%s'"% (self.m_str_OHLCTableName))
        l_QueryResult=l_cur.fetchall()
        if not l_QueryResult:   # Create table if it does not exist
            l_cur.execute("create table %s (Date text NOT NULL, Time text NOT NULL, Open real NOT NULL, High real NOT NULL, Low real NOT NULL, Close real NOT NULL)"%(self.m_str_OHLCTableName))
        else:          #If it Exits, Delete data present in table
            l_cur.execute("delete from %s;"% (self.m_str_OHLCTableName))
            l_dbHandle.commit()



        l_cur.execute("show tables like '%s'"% (self.m_str_SignalTableName))
        l_QueryResult=l_cur.fetchall()
        if not l_QueryResult:   #Create table if it does not exist
            l_cur.execute("create table %s (Date text NOT NULL,Time text NOT NULL,Price Real NOT NULL,TradeType text NOT NULL,Qty int NOT NULL,Remarks text)"%(self.m_str_SignalTableName))
        else:          #Else Delete data present in table
            l_cur.execute("delete from %s;"% (self.m_str_SignalTableName))
            l_dbHandle.commit()
        l_cur.close()

    def m_CreateRinatFile(self):
        fileid=open(self.m_str_RinaFileName,"w") # create file if it does not exist else flush the content
        fileid.write('"Trade #","Date","Time","Signal","Price","Contracts","% Profit","Runup","Entry Eff","Total","System"\n')
        fileid.write('"Type","Date","Time","Signal","Price","Profit","Cum Profit","Drawdown","Exit Eff","Eff","Market"\n')
        fileid.close()


    def m_CreateOHLC(self,l_TickMatrix,l_NumberOfTicks):
        l_DbHandle=self.m_login() #login into database
        l_cur=l_DbHandle.cursor()


        Date=l_TickMatrix[l_NumberOfTicks-1][0] # date Stored in 1st column of TickMatrix
        Time=l_TickMatrix[l_NumberOfTicks-1][1] # time stored in 2nd Column of TickMatrix



        barOpen=(round((float(l_TickMatrix[0][2]))*100))/100 # Open in 3rd column of TickMatrix
        barClose=(round((float(l_TickMatrix[l_NumberOfTicks-1][5]))*100))/100


        RoundedHigh = [ (round((float(l_TickMatrix[temp][3]))*100))/100 for temp in range(0,l_NumberOfTicks) ] # High in 4th column of TickMatrix
        barHigh = max(RoundedHigh)

        RoundedLow = [ (round((float(l_TickMatrix[temp][4]))*100))/100 for temp in range(0,l_NumberOfTicks) ] #Close in 5th column of TickMatrix
        barLow = min(RoundedLow)

        self.m_i_t+=1 #increse Bar Count
        self.m_f_OHLCMatrix.append([])
        self.m_f_OHLCMatrix[self.m_i_t-1].extend([Date,Time,barOpen,barHigh,barLow,barClose])

        l_cur.execute("Insert into %s (Date,Time,Open,High,Low,Close) values('%s','%s','%s','%s','%s','%s');"%(TraderObject.m_str_OHLCTableName, str(Date), str(Time), barOpen, barHigh, barLow, barClose)) #Write into DB
        l_DbHandle.commit()
        l_cur.close()

        return Date,Time

    def m_One_SMO(self,l_f_phi_used):

        l_f_l=np.zeros(self.m_i_TradingWindowSize,dtype='float64')
        l_f_g=np.zeros(self.m_i_TradingWindowSize,dtype='float64')


    #----------Optimisation Start------------------------------------------------
        epsilon=0.1 # for stopping Criteria
        vecx=l_f_phi_used
        vecr=np.zeros(self.m_i_TradingWindowSize,dtype='float64')
        vecr[0:self.m_i_TradingWindowSize]=self.m_f_Returns[self.m_i_t-self.m_i_TradingWindowSize:self.m_i_t]

        lambda_new=1000*np.ones(self.m_i_TradingWindowSize,dtype='float64')  #initial Point
        g_new=1000*np.ones(self.m_i_TradingWindowSize,dtype='float64')
        l_old=1000*np.ones(self.m_i_TradingWindowSize,dtype='float64')
        g_old=1000*np.ones(self.m_i_TradingWindowSize,dtype='float64')
        iterate=1

        while(iterate!=0 and iterate<20): #start 1SMO algo
        #--------------update all lambda and get lamdanew
            for lk in range(0,self.m_i_TradingWindowSize,1):   #updating each lambda
                x_kminus1=vecx[:,lk]
                r_k=vecr[lk]
                var1=(r_k**2)*(self.m_kernel(x_kminus1,x_kminus1)+1/self.m_f_A)
                f_old=0
                for u in range(0,self.m_i_TradingWindowSize,1):
                    x_u_minus1=vecx[:,u]
                    x_u=vecx[:,u+1]

                    f_old=f_old+2*(l_old[u]*vecr[u]*(self.m_kernel(x_kminus1,x_u_minus1)+1/self.m_f_A))

                    f_old=f_old+2*(g_old[u]*self.m_f_dell*(self.m_kernel(x_kminus1,x_u)-self.m_kernel(x_kminus1,x_u_minus1)))
                f_old=f_old*r_k*(-0.5)

                if var1==0:
                    var1=var1+1
                lk_new=(f_old/var1)+l_old[lk]

                if lk_new>self.m_f_Alpha:
                    lk_new=self.m_f_Alpha
                elif lk_new<self.m_f_betaa:
                    lk_new=self.m_f_betaa

                lambda_new[lk]=lk_new

            for gk in range(0,self.m_i_TradingWindowSize,1): #update all G
                x_kminus1=vecx[:,gk]
                x_k=vecx[:,gk+1]

                var1=(self.m_f_dell**2)*(self.m_kernel(x_k,x_k)-self.m_kernel(x_k,x_kminus1)-self.m_kernel(x_kminus1,x_k)+self.m_kernel(x_kminus1,x_kminus1))

                f_old=0

                for u in range(0,self.m_i_TradingWindowSize,1):
                    x_u_minus1=vecx[:,u]
                    x_u=vecx[:,u+1]

                    f_old=f_old+2*(vecr[u]*l_old[u]*(self.m_kernel(x_u_minus1,x_k)-self.m_kernel(x_u_minus1,x_kminus1)))

                    f_old=f_old+2*(g_old[u]*(self.m_kernel(x_k,x_u)-self.m_kernel(x_k,x_u_minus1)-self.m_kernel(x_kminus1,x_u)+self.m_kernel(x_kminus1,x_u_minus1)))

                f_old=f_old*self.m_f_dell*(-0.5)

                if var1==0:
                    var1=1
                gk_new=(f_old/var1)+g_old[gk]


                if gk_new>self.m_f_gamma:
                    gk_new=self.m_f_gamma
                elif gk_new<-self.m_f_gamma:
                    gk_new=-self.m_f_gamma

                g_new[gk]=gk_new

		    # compare lambdaold and lambdanew & gold and gnew
            if ((np.linalg.norm(l_old-lambda_new))<=epsilon) and ((np.linalg.norm(g_old-g_new))<=epsilon):
                iterate=0   #Stop iterating
            else:
                iterate=iterate+1
                g_old[0:self.m_i_TradingWindowSize]=g_new[0:self.m_i_TradingWindowSize]
                l_old[0:self.m_i_TradingWindowSize]=lambda_new[0:self.m_i_TradingWindowSize]

	#--------------------Optimisation finished------------------------------------

        l_f_l[0:self.m_i_TradingWindowSize]=lambda_new[0:self.m_i_TradingWindowSize]
        l_f_g[0:self.m_i_TradingWindowSize]=g_new[0:self.m_i_TradingWindowSize]

        return l_f_l,l_f_g #return new values


    #--------My Algo------------------------
    def m_TradingAlgorithm(self):

        l_f_OpenPrice=np.matrix(self.m_f_OHLCMatrix)[:,2]
        l_f_OpenPrice=np.resize(l_f_OpenPrice,self.m_i_t)
        l_f_OpenPrice=np.asfarray(l_f_OpenPrice, dtype='float64')

        l_f_HighPrice=np.matrix(self.m_f_OHLCMatrix)[:,3]
        l_f_HighPrice=np.resize(l_f_HighPrice,self.m_i_t)
        l_f_HighPrice=np.asfarray(l_f_HighPrice, dtype='float64')

        l_f_LowPrice=np.matrix(self.m_f_OHLCMatrix)[:,4]
        l_f_LowPrice=np.resize(l_f_LowPrice,self.m_i_t)
        l_f_LowPrice=np.asfarray(l_f_LowPrice, dtype='float64')

        l_f_ClosePrice=np.matrix(self.m_f_OHLCMatrix)[:,5]
        l_f_ClosePrice=np.resize(l_f_ClosePrice,self.m_i_t)
        l_f_ClosePrice=np.asfarray(l_f_ClosePrice, dtype='float64')

        l_f_TypicalPrice=(l_f_ClosePrice+l_f_HighPrice+l_f_LowPrice)/3

        l_f_ShortMA=self.m_MovingAverage(l_f_ClosePrice,12)
        l_f_LongMA=self.m_MovingAverage(l_f_ClosePrice,20)

        l_f_SmoothPriceTempPosition=self.m_MovingAverage(l_f_TypicalPrice,7)     # compute Features
        l_f_FeatureVector=l_f_SmoothPriceTempPosition


        if (self.m_i_t==self.m_i_TradingWindowSize):    # Initialise during 1st call
            self.m_f_Profit=np.zeros(self.m_i_TradingWindowSize-1, dtype='float64')
            self.m_f_CumulativeProfit=np.zeros(self.m_i_TradingWindowSize-1, dtype='float64')
            self.m_f_TempPosition=np.zeros(self.m_i_TradingWindowSize-1, dtype='float64')

            self.m_f_FeatureMatrix=np.zeros((self.m_i_BarsBack,self.m_i_TradingWindowSize),dtype='float64')

            self.m_f_Returns=np.zeros(self.m_i_TradingWindowSize-1,dtype='float64')
            localvar=1
            while (localvar<self.m_i_TradingWindowSize-1):    #loop to assign all the values of r, cannot put abcd assignment here because it has to be in the same loop as the weight update
                self.m_f_Returns[localvar]=l_f_ClosePrice[localvar]-l_f_ClosePrice[localvar-1]
                localvar=localvar+1

            self.m_f_Weights=np.zeros((self.m_i_BarsBack,self.m_i_TradingWindowSize-1),dtype='float64')


        #---appending values correponding to each bar-------
        #---append in each call---------------------
        self.m_f_FeatureMatrix=np.hstack((self.m_f_FeatureMatrix,np.reshape(l_f_FeatureVector[self.m_i_t-self.m_i_BarsBack:self.m_i_t],[-1,1]))) #appending a column to Feature Matrix
        #tot=np.sum(self.m_f_FeatureMatrix,axis=0,dtype='float64')
        #self.m_f_FeatureMatrix[:,self.m_i_t]=self.m_f_FeatureMatrix[:,self.m_i_t]/tot[self.m_i_t]

        self.m_f_Returns=np.append(self.m_f_Returns,(l_f_ClosePrice[self.m_i_t-1]-l_f_ClosePrice[self.m_i_t-2]))

        self.m_f_Weights=np.hstack((self.m_f_Weights,np.zeros((self.m_i_BarsBack,1),dtype="float64")))
        self.m_f_TempPosition=np.append(self.m_f_TempPosition,0.0)
        self.m_i_Position=np.append(self.m_i_Position,0.0)
        self.m_f_Profit=np.append(self.m_f_Profit,0.0)
        self.m_f_CumulativeProfit=np.append(self.m_f_CumulativeProfit,0.0)

        l_f_phi_used=self.m_f_FeatureMatrix[:,self.m_i_t-self.m_i_TradingWindowSize:self.m_i_t+1]  # Phi used contains last WindowSize+1 Samples

        #---Make Phi 0 mean and unit Variance--------------
        l_f_tot1=np.sum(l_f_phi_used,axis=1,dtype='float64')
        l_f_tot1=l_f_tot1/(self.m_i_TradingWindowSize+1)
        l_f_std=np.std(l_f_phi_used,1)
        l_f_tot1=np.reshape(l_f_tot1,(-1,1))
        l_f_tot1=np.repeat(l_f_tot1,self.m_i_TradingWindowSize+1,axis=1)
        l_f_phi_used=l_f_phi_used-l_f_tot1
        for loopvar in range(0,len(l_f_phi_used)):
		    l_f_phi_used[loopvar,:]=l_f_phi_used[loopvar,:]/l_f_std[loopvar]

        l_f_Theta=0.0


        l_f_l,l_f_g=self.m_One_SMO(l_f_phi_used) # get optimum l and G

        for i in range(0,self.m_i_TradingWindowSize,1): # compute weights and Theta
            self.m_f_Weights[:,self.m_i_t-1]=self.m_f_Weights[:,self.m_i_t-1]+((l_f_l[i]*self.m_f_Returns[self.m_i_t-self.m_i_TradingWindowSize+i]*l_f_phi_used[:,i]) + (l_f_g[i]*self.m_f_dell*(l_f_phi_used[:,i+1]-l_f_phi_used[:,i])))
            l_f_Theta=l_f_Theta+(1/self.m_f_A)*self.m_f_Returns[self.m_i_t-self.m_i_TradingWindowSize+i]*l_f_l[i]

        tempr=(np.dot((np.transpose(self.m_f_Weights[:,[self.m_i_t-1]])),l_f_phi_used[:,self.m_i_TradingWindowSize])+l_f_Theta)/(np.linalg.norm(self.m_f_Weights[:,[self.m_i_t-1]]))    # Compute Temp Position
        self.m_f_TempPosition[self.m_i_t-1]=round(tempr,2)


        #----------Positions from TempPositions------------------
        if ((self.m_f_TempPosition[self.m_i_t-1]>self.m_f_Threshold) and (self.m_f_TempPosition[self.m_i_t-2]>self.m_f_Threshold) and (l_f_HighPrice[self.m_i_t-1]>l_f_HighPrice[self.m_i_t-2])  and l_f_ShortMA[self.m_i_t-1]>l_f_LongMA[self.m_i_t-1]):
            self.m_i_Position[self.m_i_t-1]=1
        elif ((self.m_f_TempPosition[self.m_i_t-1]>self.m_f_Threshold) and (self.m_i_Position[self.m_i_t-2]==1) and l_f_ShortMA[self.m_i_t-1]>l_f_LongMA[self.m_i_t-1]):
            self.m_i_Position[self.m_i_t-1]=1
        elif ((self.m_f_TempPosition[self.m_i_t-1]<-self.m_f_Threshold) and (self.m_f_TempPosition[self.m_i_t-2]<-self.m_f_Threshold) and (l_f_HighPrice[self.m_i_t-1]<l_f_HighPrice[self.m_i_t-2]) and l_f_ShortMA[self.m_i_t-1]<l_f_LongMA[self.m_i_t-1]):
            self.m_i_Position[self.m_i_t-1]=-1
        elif ((self.m_f_TempPosition[self.m_i_t-1]<-self.m_f_Threshold) and (self.m_i_Position[self.m_i_t-2]==-1) and l_f_ShortMA[self.m_i_t-1]<l_f_LongMA[self.m_i_t-1]):
            self.m_i_Position[self.m_i_t-1]=-1
        else:
            self.m_i_Position[self.m_i_t-1]=0



        self.m_f_Profit[self.m_i_t-1]=self.m_i_Position[self.m_i_t-2]*(l_f_ClosePrice[self.m_i_t-1]-l_f_ClosePrice[self.m_i_t-2])-0.0035*abs(self.m_i_Position[self.m_i_t-1]-self.m_i_Position[self.m_i_t-2])   #instantaneous profit

        self.m_f_CumulativeProfit[self.m_i_t-1]=self.m_f_CumulativeProfit[self.m_i_t-2]+self.m_f_Profit[self.m_i_t-1] #Cumulative Profit






    def m_SignalGeneration(self):
        l_str_BarDate=str(self.m_f_OHLCMatrix[self.m_i_t-1][0])
        l_str_BarTime=str(self.m_f_OHLCMatrix[self.m_i_t-1][1])

        l_f_BarClosePrice=float(self.m_f_OHLCMatrix[self.m_i_t-1][5])

        l_DbHandle=self.m_login() #login into database
        l_cur=l_DbHandle.cursor()



        if (self.m_i_Position[self.m_i_t-2]==0 and self.m_i_Position[self.m_i_t-1]==-1 and self.m_i_PositionInMarket==0): #Generate EnterShort1 Signal (Case 1)
    	    l_cur.execute("Insert into %s (Date,Time,Price,Tradetype,Qty,Remarks) values('%s','%s','%s','%s','%s','%s');"%(self.m_str_SignalTableName, l_str_BarDate,l_str_BarTime, l_f_BarClosePrice,'sell',self.m_i_ShareQuantity, 'Enter_Short1')) #Write into DB
            l_DbHandle.commit()
            self.m_i_TradeType=1
            self.m_f_MarketEnterPrice=l_f_BarClosePrice
            self.m_i_PositionInMarket=-1

        if (self.m_i_Position[self.m_i_t-2]==0 and self.m_i_Position[self.m_i_t-1]==1 and self.m_i_PositionInMarket==0): #Generate EnterLong1 Signal (Case 2)
    	    l_cur.execute("Insert into %s (Date,Time,Price,Tradetype,Qty,Remarks) values('%s','%s','%s','%s','%s','%s');"%(self.m_str_SignalTableName, l_str_BarDate,l_str_BarTime, l_f_BarClosePrice,'buy',self.m_i_ShareQuantity, 'Enter_Long1')) #Write into DB
            l_DbHandle.commit()
            self.m_i_TradeType=1
            self.m_f_MarketEnterPrice=l_f_BarClosePrice
            self.m_i_PositionInMarket=1

        if (self.m_i_Position[self.m_i_t-2]==1 and self.m_i_Position[self.m_i_t-1]==-1 and self.m_i_TradeType==1 and self.m_i_PositionInMarket==1):   #clear off your Long1 position (Case3)
            l_cur.execute("Insert into %s (Date,Time,Price,Tradetype,Qty,Remarks) values('%s','%s','%s','%s','%s','%s');"%(self.m_str_SignalTableName, l_str_BarDate,l_str_BarTime, l_f_BarClosePrice,'sell',self.m_i_ShareQuantity, 'Long_Exit1')) #Write into DB
            l_DbHandle.commit()
            self.m_i_PositionInMarket=0

        if (self.m_i_Position[self.m_i_t-2]==1 and self.m_i_Position[self.m_i_t-1]==-1 and self.m_i_TradeType==2 and self.m_i_PositionInMarket==1):   #clear off your Long2 position (Case 4)
            l_cur.execute("Insert into %s (Date,Time,Price,Tradetype,Qty,Remarks) values('%s','%s','%s','%s','%s','%s');"%(self.m_str_SignalTableName, l_str_BarDate,l_str_BarTime, l_f_BarClosePrice,'sell',self.m_i_ShareQuantity, 'Long_Exit2')) #Write into DB
            l_DbHandle.commit()
            self.m_i_PositionInMarket=0

        if (self.m_i_Position[self.m_i_t-2]==1 and self.m_i_Position[self.m_i_t-1]==-1 and self.m_i_PositionInMarket==0):    #Generate a Enter Short2 signal (Case 5)
            l_cur.execute("Insert into %s (Date,Time,Price,Tradetype,Qty,Remarks) values('%s','%s','%s','%s','%s','%s');"%(self.m_str_SignalTableName, l_str_BarDate,l_str_BarTime, l_f_BarClosePrice,'sell',self.m_i_ShareQuantity, 'Enter_Short2')) #Write into DB
            l_DbHandle.commit()
            self.m_i_TradeType==2
            self.m_f_MarketEnterPrice=l_f_BarClosePrice
            self.m_i_PositionInMarket=-1

        if (self.m_i_Position[self.m_i_t-2]==-1 and self.m_i_Position[self.m_i_t-1]==1 and self.m_i_TradeType==1 and self.m_i_PositionInMarket==-1):   #clear off your Short1 position (Case 6)
            l_cur.execute("Insert into %s (Date,Time,Price,Tradetype,Qty,Remarks) values('%s','%s','%s','%s','%s','%s');"%(self.m_str_SignalTableName, l_str_BarDate,l_str_BarTime, l_f_BarClosePrice,'buy',self.m_i_ShareQuantity, 'Short_Exit1')) #Write into DB
            l_DbHandle.commit()
            self.m_i_PositionInMarket=0

        if (self.m_i_Position[self.m_i_t-2]==-1 and self.m_i_Position[self.m_i_t-1]==1 and self.m_i_TradeType==2 and self.m_i_PositionInMarket==-1):   #clear off your Short2 position (Case 7)
            l_cur.execute("Insert into %s (Date,Time,Price,Tradetype,Qty,Remarks) values('%s','%s','%s','%s','%s','%s');"%(self.m_str_SignalTableName, l_str_BarDate,l_str_BarTime, l_f_BarClosePrice,'buy',self.m_i_ShareQuantity, 'Short_Exit2')) #Write into DB
            l_DbHandle.commit()
            self.m_i_PositionInMarket=0

        if (self.m_i_Position[self.m_i_t-2]==-1 and self.m_i_Position[self.m_i_t-1]==1 and self.m_i_PositionInMarket==0):    #Generate a EnterLong2 signal (Case 8)
            l_cur.execute("Insert into %s (Date,Time,Price,Tradetype,Qty,Remarks) values('%s','%s','%s','%s','%s','%s');"%(self.m_str_SignalTableName, l_str_BarDate,l_str_BarTime, l_f_BarClosePrice,'buy',self.m_i_ShareQuantity, 'Enter_Long2')) #Write into DB
            l_DbHandle.commit()
            self.m_i_TradeType==2
            self.m_f_MarketEnterPrice=l_f_BarClosePrice
            self.m_i_PositionInMarket=1

        if (self.m_i_Position[self.m_i_t-2]==-1 and self.m_i_Position[self.m_i_t-1]==0 and self.m_i_TradeType==1 and self.m_i_PositionInMarket==-1): #clear off your Short1 position (Case 9)
            l_cur.execute("Insert into %s (Date,Time,Price,Tradetype,Qty,Remarks) values('%s','%s','%s','%s','%s','%s');"%(self.m_str_SignalTableName, l_str_BarDate,l_str_BarTime, l_f_BarClosePrice,'buy',self.m_i_ShareQuantity, 'Short_Exit3')) #Write into DB
            l_DbHandle.commit()
            self.m_i_PositionInMarket=0

        if (self.m_i_Position[self.m_i_t-2]==-1 and self.m_i_Position[self.m_i_t-1]==0 and self.m_i_TradeType==2 and self.m_i_PositionInMarket==-1): #clear off your Short2 position(Case 10)
            l_cur.execute("Insert into %s (Date,Time,Price,Tradetype,Qty,Remarks) values('%s','%s','%s','%s','%s','%s');"%(self.m_str_SignalTableName, l_str_BarDate,l_str_BarTime, l_f_BarClosePrice,'buy',self.m_i_ShareQuantity, 'Short_Exit4')) #Write into DB
            l_DbHandle.commit()
            self.m_i_PositionInMarket=0

        if (self.m_i_Position[self.m_i_t-2]==1 and self.m_i_Position[self.m_i_t-1]==0 and self.m_i_TradeType==1 and self.m_i_PositionInMarket==1): #clear off your Long1 position (Case 11)
            l_cur.execute("Insert into %s (Date,Time,Price,Tradetype,Qty,Remarks) values('%s','%s','%s','%s','%s','%s');"%(self.m_str_SignalTableName, l_str_BarDate,l_str_BarTime, l_f_BarClosePrice,'sell',self.m_i_ShareQuantity, 'Long_Exit3')) #Write into DB
            l_DbHandle.commit()
            self.m_i_PositionInMarket=0

        if (self.m_i_Position[self.m_i_t-2]==1 and self.m_i_Position[self.m_i_t-1]==0 and self.m_i_TradeType==2 and self.m_i_PositionInMarket==1): #clear off Long2 your position (Case 12)
            l_cur.execute("Insert into %s (Date,Time,Price,Tradetype,Qty,Remarks) values('%s','%s','%s','%s','%s','%s');"%(self.m_str_SignalTableName, l_str_BarDate,l_str_BarTime, l_f_BarClosePrice,'sell',self.m_i_ShareQuantity, 'Long_Exit4')) #Write into DB
            l_DbHandle.commit()
            self.m_i_PositionInMarket=0
        l_cur.close()



    def m_Write2Rina(self,SignalsRead):
        #----------Entries in Rina file---------
        l_i_Profit=0
        l_i_PerProf=self.m_i_TradeNum
        l_i_CumProf=self.m_i_TradeNum
        l_i_EnterEff=0
        l_i_ExitEff=0
        l_i_DD=0
        l_i_RunUp=0
        l_i_Tot=0
        l_i_Eff=0
        l_str_System='portfolio_1'
        l_str_Market='USDINR1'

        l_i_DataSize=len(SignalsRead)   # number of Signals
        fileid=open(self.m_str_RinaFileName,"a")

        if (l_i_DataSize>0):
            l_i_Counter=0;
            while(l_i_Counter<l_i_DataSize):
                #Entries in Signal table alternate for Entry and Exit
                if (self.m_i_RinaInternalFlag==1):  # Entry  Signal
                    l_Enterdate=SignalsRead[l_i_Counter][0] # Date in Rina File Will be entered in same format as in data table
                    l_Entertime=SignalsRead[l_i_Counter][1] # Time in Rina File Will be entered in same format as in data table
                    l_Enterprice=SignalsRead[l_i_Counter][2]
                    self.m_str_EnterType=SignalsRead[l_i_Counter][3]
                    l_Entercontracts=SignalsRead[l_i_Counter][4]
                    l_Entersignalmame=SignalsRead[l_i_Counter][5]
                    #l_newenterdate=enterdate.replace('-','/')
                    temp='"'+str(self.m_i_TradeNum)+'","'+l_Enterdate+'","'+l_Entertime+'","'+l_Entersignalmame+'","'+str(l_Enterprice)+'","'+str(l_Entercontracts)+'","'+str(l_i_PerProf)+'","'+str(l_i_RunUp)+'","'+str(l_i_EnterEff)+'","'+'","'+str(l_i_Tot)+'","'+str(l_str_System)+"\n"
                    fileid.write(temp)
                    l_i_Counter=l_i_Counter+1
                    self.m_i_RinaInternalFlag=2 #Next Signal will correspond to Exit

                elif(self.m_i_RinaInternalFlag==2): #Exit Signal
                    l_Exitdate=SignalsRead[l_i_Counter][0]
                    l_Exittime=SignalsRead[l_i_Counter][1]
                    l_Exitprice=SignalsRead[l_i_Counter][2]
                    l_Exittype=SignalsRead[l_i_Counter][3]
                    l_Exitcontracts=SignalsRead[l_i_Counter][4]
                    l_Exitsignalname=SignalsRead[l_i_Counter][5]

                    #newexitdate=exitdate.replace('-','/')
                    temp='"'+str(self.m_str_EnterType)+'","'+l_Exitdate+'","'+l_Exittime+'","'+l_Exitsignalname+'","'+str(l_Exitprice)+'","'+str(l_i_Profit)+'","'+str(l_i_CumProf)+'","'+str(l_i_DD)+'","'+str(l_i_ExitEff)+'","'+str(l_i_Eff)+'","'+l_str_Market+'\n'
                    fileid.write(temp)
                    self.m_i_TradeNum=self.m_i_TradeNum+1 # Trade Complete
                    l_i_Perprof=self.m_i_TradeNum
                    l_i_CumProf=self.m_i_TradeNum
                    l_i_Counter=l_i_Counter+1
                    self.m_i_RinaInternalFlag=1 # Next Signal will correspond to Entry

        fileid.close()
        return


#------------------End Of Class-------------------------------

#=============Main Function===================================

TraderObject=Trader() # Trader Instance
#------------ local variables-----------
l_i_loopvar=1   # loop variable
l_i_TickNumber=0    # Count the ticks for Bar Creation
l_str_TickDate=''
l_str_TickTime=''
l_f_TickOpen=0.0
l_f_TickHigh=0.0
l_f_TickLow=0.0
l_f_TickClose=0.0
l_f_TrailPrice=0.0  # For Trailing
l_i_Trail=0 # for Trailing
l_TickDataMatrix=[[None for _ in range(6)] for _ in range (TraderObject.m_i_BarTimeInterval)] # Creating a matrix for tik data
l_DbHandle=TraderObject.m_login() #login into database
l_cur=l_DbHandle.cursor()

TraderObject.m_CreateTable() #Create tables
TraderObject.m_CreateRinatFile() # Create Rina file with header

print "Start date "  + time.strftime("%x")
print "Start time " + time.strftime("%X")



while(l_i_loopvar==1):
    #---------------Read Tick data and create OHLC matrix----------------------------------

    l_cur.execute("select TICK_DATE,TICK_TIME,OPEN,HIGH,LOW,CLOSE from %s LIMIT %s,1;"% (TraderObject.m_str_DataTableName,TraderObject.m_i_TotalTicks)) # read 1 line form database
    l_QueryResult=l_cur.fetchall()
    if not l_QueryResult:   # if no data then Exit form loop
        print "No data Present \n"
        print "End date "  + time.strftime("%x")
        print "End time " + time.strftime("%X")

        l_i_loopvar=0
        break

    else:
        l_i_TickNumber=l_i_TickNumber+1 # Increment tick number
        TraderObject.m_i_TotalTicks+=1  # Increment Total Tick number
        l_str_TickDate=l_QueryResult[0][0]
        l_str_TickTime=l_QueryResult[0][1]
        l_f_TickOpen=l_QueryResult[0][2]
        l_f_TickHigh=l_QueryResult[0][3]
        l_f_TickLow=l_QueryResult[0][4]
        l_f_TickClose=l_QueryResult[0][5]
        l_TickDataMatrix[l_i_TickNumber-1][0]=l_str_TickDate
        l_TickDataMatrix[l_i_TickNumber-1][1]=l_str_TickTime
        l_TickDataMatrix[l_i_TickNumber-1][2]=l_f_TickOpen
        l_TickDataMatrix[l_i_TickNumber-1][3]=l_f_TickHigh
        l_TickDataMatrix[l_i_TickNumber-1][4]=l_f_TickLow
        l_TickDataMatrix[l_i_TickNumber-1][5]=l_f_TickClose


        if ((l_i_TickNumber==TraderObject.m_i_BarTimeInterval) or (str(l_str_TickTime)==TraderObject.m_str_SessionCloseTime)):  # If BarTimeInterval ticks or Session closing Time then Create the Bar
            l_str_BarDate,l_str_BarTime=TraderObject.m_CreateOHLC(l_TickDataMatrix[0:l_i_TickNumber][:],l_i_TickNumber)
            l_i_TickNumber=0    # reaset number of Ticks for next bar
            if (TraderObject.m_i_t<TraderObject.m_i_TradingWindowSize):
                TraderObject.m_i_Position.append(0)
            else:
                TraderObject.m_TradingAlgorithm()


            l_cur.execute("Insert into %s (Date,Time,Position) values('%s','%s','%s');"%(TraderObject.m_str_ResultTableName, str(l_str_BarDate), str(l_str_BarTime), TraderObject.m_i_Position[TraderObject.m_i_t-1])) #Write into DB

            l_DbHandle.commit()
            #print str(l_str_BarDate)
            #print str(l_str_BarTime)
            #print TraderObject.m_i_Position[TraderObject.m_i_t-1]

            TraderObject.m_SignalGeneration()

        #-------------------TRAILING-------------------------------------------------------
        #--------------Trailing long position----------------------------------------------
        if (TraderObject.m_i_PositionInMarket==1 and l_i_Trail==1 and l_f_TickClose>=l_f_TrailPrice):    #trail is ON and market is going up (Follow Trail)
            l_f_TrailPrice=l_f_TickClose
        elif (TraderObject.m_i_PositionInMarket==1 and l_i_Trail==1 and l_f_TickClose<=l_f_TrailPrice-0.0035*l_f_TrailPrice): #trail is ON and market moved down (Trail Hit)
            TraderObject.m_i_PositionInMarket=0
            l_i_Trail=0
            l_f_TrailPrice=0.0
            l_cur.execute("Insert into %s (Date,Time,Price,Tradetype,Qty,Remarks) values('%s','%s','%s','%s','%s','%s');"%(TraderObject.m_str_SignalTableName, l_str_TickDate,l_str_TickTime, l_f_TickClose,'sell',TraderObject.m_i_ShareQuantity, 'Long_TL_Hit')) #Exit Trade
            l_DbHandle.commit()

        elif(TraderObject.m_i_PositionInMarket==1 and l_i_Trail==0 and l_f_TickClose>=TraderObject.m_f_MarketEnterPrice+TraderObject.m_f_MarketEnterPrice*0.01):    # Trail  On
            l_i_Trail=1
            l_f_TrailPrice=l_f_TickClose
    #-------------------------------------------------------------------------------------------

        #---------------------Trailing Short position-------------
        if (TraderObject.m_i_PositionInMarket==-1 and l_i_Trail==-1 and l_f_TickClose<=l_f_TrailPrice):    #trail is ON and market is going down (Follow Trail)
            l_f_TrailPrice=l_f_TickClose
        elif (TraderObject.m_i_PositionInMarket==-1 and l_i_Trail==-1 and l_f_TickClose>=l_f_TrailPrice+0.0035*l_f_TrailPrice): #trail is ON and market moved up (Trail Hit)
            TraderObject.m_i_PositionInMarket=0
            l_i_Trail=0
            l_f_TrailPrice=0.0
            l_cur.execute("Insert into %s (Date,Time,Price,Tradetype,Qty,Remarks) values('%s','%s','%s','%s','%s','%s');"%(TraderObject.m_str_SignalTableName, l_str_TickDate,l_str_TickTime, l_f_TickClose,'buy',TraderObject.m_i_ShareQuantity, 'Short_TL_Hit')) # Exit Trade
            l_DbHandle.commit()


        elif(TraderObject.m_i_PositionInMarket==-1 and l_i_Trail==0 and l_f_TickClose<=TraderObject.m_f_MarketEnterPrice-TraderObject.m_f_MarketEnterPrice*0.01):   # Trail On
            l_i_Trail=-1
            l_f_TrailPrice=l_f_TickClose

    #-----------------------------------------------------------------------------------------------------------------%

    #-------------------STOPLOSS------------------------%
    #---------------for Long position-------------------%
    if ((TraderObject.m_i_PositionInMarket==1) and (l_f_TickClose<=(TraderObject.m_f_MarketEnterPrice-0.0025*TraderObject.m_f_MarketEnterPrice))):  # Stop Loss
        TraderObject.m_i_PositionInMarket=0
        l_cur.execute("Insert into %s (Date,Time,Price,Tradetype,Qty,Remarks) values('%s','%s','%s','%s','%s','%s');"%(TraderObject.m_str_SignalTableName, l_str_TickDate,l_str_TickTime, l_f_TickClose,'sell',TraderObject.m_i_ShareQuantity, 'Long_SL_Hit')) # Exit Trade
        l_DbHandle.commit()

    #---------------for short position------------------%
    if ((TraderObject.m_i_PositionInMarket==-1) and (l_f_TickClose>=(TraderObject.m_f_MarketEnterPrice+0.0025*TraderObject.m_f_MarketEnterPrice))):
        TraderObject.m_i_PositionInMarket=0
        l_cur.execute("Insert into %s (Date,Time,Price,Tradetype,Qty,Remarks) values('%s','%s','%s','%s','%s','%s');"%(TraderObject.m_str_SignalTableName, l_str_TickDate,l_str_TickTime, l_f_TickClose,'buy',TraderObject.m_i_ShareQuantity, 'Short_SL_Hit')) # Exit Trade
        l_DbHandle.commit()

    #------------------------------------------------------%

    if (l_str_TickTime==TraderObject.m_str_SessionCloseTime): # reached end of the day
        if (TraderObject.m_i_GenerateRina==1):
            l_cur.execute("Select * from %s where date ='%s';"%(TraderObject.m_str_SignalTableName,str(l_str_TickDate))) # get Today's signals
            l_SignalsRead=l_cur.fetchall()
            if (l_SignalsRead): # if we have signals
                TraderObject.m_Write2Rina(l_SignalsRead)    # write into Rina


l_cur.close()


