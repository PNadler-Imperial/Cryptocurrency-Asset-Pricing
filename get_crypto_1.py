# -*- coding: utf-8 -*-
"""
F5
Created on Tue Mar 20 17:24:21 2018

@author: Philip Nadler

This file calls data of various frequencies via the Binance API. Run the get_crypto_poloniex_script first. This programme also merges
and synchronises the datasets of both exchanges.

It also converts the cryptocurrency values from Satoshi to approximate USD prices.

The only parameter of importance is "frequency" the frequency of your data.
 
Make sure that frequency/period is the same in both programmes, otherwise it will not synchronise.

(Note: Think about further expansion with this: CCXT – CryptoCurrency eXchange Trading Library)
"""
# In[]:

# Import packages

import os
import numpy as np
import pandas as pd #python data analysis library
import pickle #serialiying python objects.
import quandl #economic and financial time series
from datetime import datetime
#plotly stuff for data visualization.
#import plotly.offline as py
#import plotly.graph_objs as go
#import plotly.figure_factory as ff
#py.init_notebook_mode(connected=True)
#import qgrid
import dateparser 
import matplotlib
import pytz
from binance.client import Client
import time
import json
from IPython import get_ipython

# In[]:

""" Frequency?"""

frequency = Client.KLINE_INTERVAL_1DAY
#Client.KLINE_INTERVAL_30MINUTE  ; up to a 1 minute frequency
#Client.KLINE_INTERVAL_5MINUTE
#Client.KLINE_INTERVAL_1DAY
#Client.KLINE_INTERVAL_1WEEK
"""Start and End date?"""

#note that binance opened on the 14th of July 2017
start_date = ("1 Dec, 2014")
#end_date = ("1 day ago UTC") 
end_date = ("6 August, 2018")

# In[]:

#Binance API only works with millisecond values, thus translate date to ms

#add additional binance data
#https://github.com/binance-exchange/binance-api-node

def date_to_milliseconds(date_str):
    """Convert UTC date to milliseconds
    If using offset strings add "UTC" to date string e.g. "now UTC", "11 hours ago UTC"
    See dateparse docs for formats http://dateparser.readthedocs.io/en/latest/
    :param date_str: date in readable format, i.e. "January 01, 2018", "11 hours ago UTC", "now UTC"
    :type date_str: str
    """
    # get epoch value in UTC
    epoch = datetime.utcfromtimestamp(0).replace(tzinfo=pytz.utc)
    # parse our date string
    d = dateparser.parse(date_str)
    # if the date is not timezone aware apply UTC timezone
    if d.tzinfo is None or d.tzinfo.utcoffset(d) is None:
        d = d.replace(tzinfo=pytz.utc)

    # return the difference in time
    return int((d - epoch).total_seconds() * 1000.0) # *1000 to make it readable for java script
print(date_to_milliseconds("now UTC"))
print(date_to_milliseconds("January 01, 2018"))


# In[]:

#Frequency/Period needs to be translated to milliseconds
def interval_to_milliseconds(interval):
    """Convert a Binance interval string to milliseconds
    :param interval: Binance interval string 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d, 1w
    :type interval: str
    :return:
         None if unit not one of m, h, d or w
         None if string not in correct format
         int value of interval in milliseconds
    """
    ms = None
    seconds_per_unit = {
        "m": 60,
        "h": 60 * 60,
        "d": 24 * 60 * 60,
        "w": 7 * 24 * 60 * 60
    }

    unit = interval[-1]
    if unit in seconds_per_unit:
        try:
            ms = int(interval[:-1]) * seconds_per_unit[unit] * 1000
        except ValueError:
            pass
    return ms

# Just to test 
print(interval_to_milliseconds(Client.KLINE_INTERVAL_1MINUTE))
print(interval_to_milliseconds(Client.KLINE_INTERVAL_30MINUTE))
print(interval_to_milliseconds(Client.KLINE_INTERVAL_1WEEK))


# In[]:

#Call data

"""
Additional Bittrex data can be added later on
#additional bittrex 
#https://bittrex.com/Api/v2.0/pub/market/GetTicks?marketName=BTC-CVC&tickInterval=thirtyMin&_=1500915289433
#bittrex_base_url='https://bittrex.com/Api/v2.0/pub/market/GetTicks?marketName={}&tickInterval=thirtyMin'
#print(date_to_milliseconds("January 01, 2018"))
#print(dateparser.parse('12/12/12'))
"""

def get_historical_klines(symbol, interval, start_str, end_str=None):
    """Get Historical Klines from Binance
    See dateparse docs for valid start and end string formats http://dateparser.readthedocs.io/en/latest/
    If using offset strings for dates add "UTC" to date string e.g. "now UTC", "11 hours ago UTC"
    :param symbol: Name of symbol pair e.g BNBBTC
    :type symbol: str
    :param interval: Biannce Kline interval
    :type interval: str
    :param start_str: Start date string in UTC format
    :type start_str: str
    :param end_str: optional - end date string in UTC format
    :type end_str: str
    :return: list of OHLCV values
    """
    # create the Binance client, no need for api key
    client = Client("", "")

    # init our list
    output_data = []

    # setup the max limit; max=500
    limit = 495

    # convert interval to useful value in seconds
    timeframe = interval_to_milliseconds(interval)

    # convert our date strings to milliseconds
    start_ts = date_to_milliseconds(start_str)

    # if an end time was passed convert it
    end_ts = None
    if end_str:
        end_ts = date_to_milliseconds(end_str)

    idx = 0
    # it can be difficult to know when a symbol was listed on Binance so allow start time to be before list date
    symbol_existed = False
    while True:
        # fetch the klines from start_ts up to max 500 entries or the end_ts if set
        temp_data = client.get_klines(
            symbol=symbol,
            interval=interval,
            limit=limit,
            startTime=start_ts,
            endTime=end_ts
        )

        # handle the case where our start date is before the symbol pair listed on Binance
        if not symbol_existed and len(temp_data):
            symbol_existed = True

        if symbol_existed:
            # append this loops data to our output data
            output_data += temp_data

            # update our start timestamp using the last value in the array and add the interval timeframe
            start_ts = temp_data[len(temp_data) - 1][0] + timeframe
        else:
            # it wasn't listed yet, increment our start date
            start_ts += timeframe

        idx += 1
        # check if we received less than the required limit and exit the loop
        if len(temp_data) < limit:
            # exit the while loop
            break

        # sleep after every 3rd call to be kind to the API
        if idx % 3 == 0:
            time.sleep(1)

    return output_data


# fetch 1 minute klines for the last day up until now
#klines = get_historical_klines("BNBBTC", Client.KLINE_INTERVAL_1MINUTE, "1 day ago UTC")

# fetch 30 minute klines for the last month of 2017
#klines = get_historical_klines("ETHBTC", Client.KLINE_INTERVAL_30MINUTE, "1 Dec, 2017", "1 Jan, 2018")

# fetch weekly klines since it listed
#klines = get_historical_klines("NEOBTC", Client.KLINE_INTERVAL_1WEEK, "1 Jan, 2017")    
 

#klines1 = client.get_historical_klines("ETHBTC", Client.KLINE_INTERVAL_30MINUTE, "1 Dec, 2017", "1 Jan, 2018")

# In[]

#This is to set the parameters of the Binance API    

# https://min-api.cryptocompare.com/data/all/exchanges

client = Client("", "")
##note that binance opened on the 14th of July 2017
#start_date = ("1 Dec, 2014")
##end_date = ("1 day ago UTC") 
#end_date = ("17 April, 2018")
#
#frequency = Client.KLINE_INTERVAL_5MINUTE
##Client.KLINE_INTERVAL_30MINUTE  ; up to a 1 minute frequency

   
klines_data={}

binance_altcoins=['ADA','ADX','AE','AION','AMB','APPC','ARK','ARN','AST','BAT','BCC','BCD','BCPT','BLZ','BNB','BNT','BQX','BRD','BTG','BTS','CDT','CHAT',
                  'CMT','CND','CTR','DGD','DLT','DNT','EDO','ELF','ENG','ENJ','EOS','ETC','ETH','EVX','FUEL','FUN','GTO','GVT','GXS','HSR','ICN','ICX','INS',
                  'IOST','IOTA','KMD','KNC','LEND','LINK','LRC','LTC','LUN','MANA','MCO','MDA','MOD','MTH','MTL','NANO','NCASH','NEBL','NEO','NULS','OAX',
                  'ONT','OST','PIVX','POA','POE','POWR','PPT','QSP','QTUM','RCN','RDN','REQ','RLC','RPX','SALT','SNGLS','SNM','SNT','STORM','SUB',
                  'TNB','TNT','TRIG','TRX','VEN','VIB','VIBE','WABI','WAVES','WINGS','WTC','XLM','XVG','XZC','YOYO','ZIL']

#116  Cryptocurrencies in total
for binance_altcoin in binance_altcoins:
    binance_coinpair= '{}BTC'.format(binance_altcoin)
    try:#remove this if it doesnt work, load data if already localy safed
        klines_data[binance_altcoin] = json.load(open(
            "Binance_{}_{}_{}-{}.json".format(
                    binance_coinpair,
                    frequency,
                    date_to_milliseconds(start_date),
                    date_to_milliseconds(end_date)
                    )))
        print( "We're locally loading %s" % (binance_altcoin))
    except:   #remove until here, load data 
        klines = client.get_historical_klines(binance_coinpair, frequency, start_date, end_date)
        print( "We're downloading %s" % (binance_altcoin))
        klines_data[binance_altcoin] = klines   

        with open(
                "Binance_{}_{}_{}-{}.json".format(
                        binance_coinpair,
                        frequency,
                        date_to_milliseconds(start_date),
                        date_to_milliseconds(end_date)
                        ),
                        'w' #write mode
                )   as f:
                    f.write(json.dumps(klines))
  

# In[]
    
def get_binance_to_poloniex_format(klines):
    new_list=pd.DataFrame([]) #creates an empty dataframe
    
    for index in range(len(klines)): # for every element in the list
        new_list[index] = klines[index] # put the element into a row of the dataframe
    
    new_list = pd.DataFrame.transpose(new_list) #transpose to match and name the columns
    new_list.columns = ['Open Time','Open','High','Low','Close','Volume','Close time','Quote Asset Volume','Number of Trades',
                                     'Taker buy base asset volume','Taker buy quote asset volume', 'ignore']
    
    for index in range (len(new_list)):
        new_list.at[index,'date'] = datetime.datetime.fromtimestamp(new_list.at[index,'Open Time']/1000).strftime('%Y-%m-%d %H:%M:%S') # java time different than UNIX thus /1000

    new_list=new_list.set_index('Open Time') #change this for higher frequency to 'Open Time' instead of 'date'
    return new_list
    
# In[]    

#Convert the dictionary to a datagrame
    
import datetime   #make sure to load this, might cause a problem somewhere
klines_data_df = {}

for index in binance_altcoins:
    klines_data_df[index] = get_binance_to_poloniex_format(klines_data[index])

#Try to generate an average of prices 
#for index in binance_altcoins:
#    klines_data_df[index]['Mid Price'] = klines_data_df[index]['Open']+klines_data_df[index]['Close'])
#    klines_data_df[index]['Mid Price'] = klines_data_df[index][['Open','Close']].sum(axis=1)
            
    
# In[]

#We keep this for now. This is computationally quicker, but causes an error on higher frequencies
def merge_dfs_on_column(dataframes, labels, col):
    '''Merge a single column of each dataframe into a new combined dataframe'''
    series_dict = {}
    for index in range(len(dataframes)):
        series_dict[labels[index]] = dataframes[index][col]
        
    return pd.DataFrame(series_dict)    #this line causes problems for high frequency sets
# In[] 

#dateframes needs to be klines_df
""" This takes a given variable  col and selects it and puts it into a 2 dimensional dataframe for all given coins. I.e. NxP N=n_obs, P=n_coins, each element is the variable chosen in col"""
    
def merge_dfs_on_column2(dataframes, labels, col):
    series_dict = {}
    for index in range(len(dataframes)):
        series_dict[labels[index]] = dataframes[index][col]
    
    x = pd.DataFrame(series_dict)
    x = x.set_index(x.index/1000)

    dates = {}
    for index in range(len(x.index)):
        dates[index] = datetime.datetime.fromtimestamp(x.index[index]).strftime('%Y-%m-%d %H:%M:%S')
    x['date'] = dates.values()
    x = x.set_index('date')
    return x
        

# In[]   

#This runs the previously defined function, putting the required variable e.g. "opening price" in a 2 dimensional dataframe
#Usually runs with df2, df is faster but crashes on higher frequencies, needs to be fixed later
    
klines_final_df = merge_dfs_on_column(list(klines_data_df.values()),list(klines_data_df.keys()),'Open')   
klines_final_df2 =  merge_dfs_on_column2(list(klines_data_df.values()),list(klines_data_df.keys()),'Open') #if you want to use this for high frequency data, change  line 283+X get_binance_to_poloniex_format(klines): from 'Open Times' to Date

klines_final_df2.index = pd.to_datetime(klines_final_df2.index) #This step is necessary to sync both datasets, binance differs due to daylight saving time. 
klines_final_df2 = klines_final_df2[~klines_final_df2.index.duplicated(keep='first')] # this for 30min higher frequ data
klines_final_df2 = klines_final_df2.reindex(combined_df.index, method='nearest')

# In[] 

#Construct USD prices
  
for index in klines_data_df.keys():
    klines_data_df[index] = klines_data_df[index].set_index('date')
    klines_data_df[index]['Open'] = klines_data_df[index]['Open'].convert_objects(convert_numeric=True)
#    klines_data_df[index]['price_usd'] = klines_data_df[index]['Open'] * btc_usd_datasets['avg_btc_price_usd']   

temp = {}    
for index in klines_data_df.keys():
    try:
        x = klines_data_df[index]['Open']
        x.index = x.index.to_datetime()#new
        y = btc_usd_datasets['avg_btc_price_usd'] 
        
        xxl = x.reindex(y.index, method='nearest', limit=1) #new
        xxl = xxl.dropna() #new
        
        x  = xxl #new
#        x.index = xxl[0] #new
        #print(x*y)
        klines_data_df[index]['price_usd'] =  x*y
        temp[index] = x*y
    except:
        pass

temp2 = pd.DataFrame (temp) 
temp2.plot()
klines_final_df2 = temp2
#klines_data_df[index]['price_usd'].plot()
klines_final_df2.index = pd.to_datetime(klines_final_df2.index) #This step is necessary to sync both datasets, binance differs due to daylight saving time. 
klines_final_df2 = klines_final_df2.reindex(combined_df.index, method='nearest')


poloniex_binance_df2 = combined_df.combine_first(klines_final_df2) #df1.combine_first(df2) df1’s values prioritized, use values from df2 to fill holes:



# In[]

# combine both dataframes  
  
#Merge Satoshi Dataframe    
poloniex_binance_satoshi_df = satoshi_df.combine_first(klines_final_df2) #df1.combine_first(df2) df1’s values prioritized, use values from df2 to fill holes:

#Merge USD Dataframe
#klines_final_df2=klines_final_df.convert_objects(convert_numeric=True)   
klines_final_df2 = klines_final_df2.convert_objects(convert_numeric=True)   
poloniex_binance_df = pd.merge(satoshi_df, klines_final_df2, left_index=True, right_index=True, how='inner') #'outer' and 'inner'

poloniex_binance_df2 = combined_df.combine_first(klines_final_df2)
#poloniex_binance_df3 = pd.merge_asof(satoshi_df, klines_final_df2, left_index=True, right_index=True,tolerance=pd.Timedelta('3m'),allow_exact_matches=False).fillna('NAN')
#matches = satoshi_df.reindex(klines_final_df2.index, method='nearest', tolerance=pd.Timedelta('15m'))


#use combined_df instead of satoshi_df if you want usd instead of satshi
poloniex_binance_df2.tail(10000).plot()
# In[] 
matplotlib.pyplot.show()
"""
#Outlier Detection

