"""
F5
Created on Fri Apr  6 17:30:17 2018

@author: Philip Nadler

This file calls data of various frequencies via the Poloniex API and also calls BITCOIN/USD data based on exchanges which actually
allow trade BTC/USD pairs. 

Run this BEFORE get_crypto_binance. Can also be run without it for a smaller number of cryptos.

"""

import os
import numpy as np
import pandas as pd #python data analysis library
import pickle #serialiying python objects.
import quandl #economic and financial time series
from datetime import datetime
#plotly stuff for data visualization.
import plotly.offline as py
import plotly.graph_objs as go
import plotly.figure_factory as ff
import matplotlib
#py.init_notebook_mode(connected=True)
#import qgrid
# In[]:

"""Generate Plots?"""
plots_var = 1

"""Frequency?"""
period = 86400 #this is daily frequency valid values are 300, 900, 1800, 7200, 14400, and 86400)
#period = 7200 #2h
#period = 1800 #30minutes
#period = 300 5min

"""Start and End date"""
start_date = datetime.strptime('2015-01-01','%Y-%m-%d')
#This can be looked up at https://poloniex.com/support/api/
end_date = datetime.now()

"""Set path if required"""
os.getcwd()
os.chdir( "/Users\Phili\OneDrive - Imperial College London\Python\CryptoTS")

# In[]:

# We're using pickle to serialize and save the downloaded data as a file, which will prevent our script from re-downloading the same data each time we run the script. The function will return the data as a Pandas dataframe. If you're not familiar with dataframes, you can think of them as super-powered Python spreadsheets.

def get_quandl_data(quandl_id):
    #download and cache quandl data
    cache_path='{}.pkl'.format(quandl_id).replace('/','-')
    try:
        f = open(cache_path, 'rb')
        df= pickle.load(f)
        print('Loaded {} from cache'.format(quandl_id))
    except (OSError, IOError) as e:
        print('Downloading {} from Quandl'.format(quandl_id))
        df = quandl.get(quandl_id)
        df.to_pickle(cache_path)
        print('Cached {} at {}'.format(quandl_id, cache_path))
    return df

        

# In[]:

# Merge a single column of each dataframe into a new combined dataframe
def merge_dfs_on_column(dataframes, labels, col):
    series_dict = {}
    for index in range(len(dataframes)):
        series_dict[labels[index]] = dataframes[index][col]
        
    return pd.DataFrame(series_dict)


# In[]: 

# fetch daily krakenusd data via  quandl
btc_usd_price_kraken = get_quandl_data('BCHARTS/KRAKENUSD')

# In[]:


exchange_data = {}

exchange_data['KRAKEN'] = btc_usd_price_kraken

for exchange in exchanges:
    exchange_code = 'BCHARTS/{}USD'.format(exchange)
    btc_exchange_df = get_quandl_data(exchange_code)
    exchange_data[exchange] = btc_exchange_df

btc_usd_datasets = merge_dfs_on_column(list(exchange_data.values()), list(exchange_data.keys()), 'Weighted Price')

btc_usd_datasets.plot()
# In[]: 

# Clean and Aggregate data
# Remove "0" values
btc_usd_datasets.replace(0, np.nan, inplace=True)
btc_usd_datasets.plot()

# Calculate the average BTC price as a new column
btc_usd_datasets['avg_btc_price_usd'] = btc_usd_datasets.mean(axis=1)
btc_usd_datasets['avg_btc_price_usd'].plot()


# In[]:

def get_json_data(json_url, cache_path):
    try:
        f=open(cache_path, 'rb')
        df = pickle.load(f)
        print('Loaded {} from cache'.format(json_url))
    except (OSError, IOError) as e:
        print('Downloading{}'.format(json_url))
        df= pd.read_json(json_url)
        df.to_pickle(cache_path)
        print('cached response at {}'.format(json_url, cache_path))
    return df


# In[]:

#Define a function to format Poloniex API HTTP requests and call get_json_data to save resulting data.
    
base_polo_url = 'https://poloniex.com/public?command=returnChartData&currencyPair={}&start={}&end={}&period={}'
#start_date = datetime.strptime('2015-01-01','%Y-%m-%d')
#This can be looked up at https://poloniex.com/support/api/
#end_date = datetime.now()

#period = 86400 #this is daily frequency valid values are 300, 900, 1800, 7200, 14400, and 86400)
#period = 7200 #2h
#period = 1800 #30minutes
#period = 3600
#period = 300

def get_crypto_data(poloniex_pair):
    'Retrieve cryptocurrency data from poloniex'
    json_url = base_polo_url.format(poloniex_pair, start_date.timestamp(),end_date.timestamp(),period)
    data_df = get_json_data(json_url,poloniex_pair)
    data_df = data_df.set_index('date')
    return data_df

# In[]:

#all poloniex coins sorted by volume
altcoins = ['ETH','LTC','XRP','ETC','STR','DASH','SC','XMR','XEM','LSK','BCH','ZEC','EMC2','BTS','FCT','DOGE','XPM','SYS','OMG','DGB','ARDR','STRAT','STEEM','SC',
           'NXT','ZRX','VIA','REP','STORJ','VTC','DCR','BCN','MAID','GNT','GAS','GNO','NAV','AMP','GAME','BURST','XCP','VRC','LBC',
           'PPC','CVC','RIC','CLAM','XVC','POT','PASC','PINK','EXP','HUC','BELA','XBC','OMNI','BCY','FLO','BLK','NXC','BTM','RADS','SBD',
           'FLDC','NEOS','GRC','NMC','BTCD']


altcoins = ['ETH','LTC','XRP','ETC','STR','DASH','SC','XMR','XEM','LSK','BCH','ZEC','EMC2','BTS','FCT','DOGE','XPM','SYS','OMG','DGB','ARDR','STRAT','STEEM','SC',
           'NXT','ZRX','VIA','REP','STORJ','VTC','DCR','BCN','MAID','GNT','GAS','GNO','NAV','AMP','GAME','BURST','XCP','VRC','LBC',
           'PPC','CVC','CLAM','POT','PASC','EXP','HUC','XBC','OMNI','BTM','SBD',
           'NEOS','GRC','NMC','BTCD']

#altcoins=['XVC']

#sore each coin in dictionary
altcoin_data = {}
for altcoin in altcoins:
    print(altcoin)
    coinpair = 'BTC_{}'.format(altcoin)
    crypto_price_df = get_crypto_data(coinpair)
    altcoin_data[altcoin] = crypto_price_df
    altcoin_data_satoshi = {} #removable
    altcoin_data_satoshi[altcoin] = crypto_price_df

# In[]:

""" Quandl API only provides daily BTC/USD data from various exchanges. As workaround the .krakenUSD.csv from http://api.bitcoincharts.com/v1/csv/ is used """
    
if period != 86400:
    #read the file
    print('Obtain high frequency data from http://api.bitcoincharts.com/v1/csv/')
    x = pd.DataFrame.from_csv('.krakenUSD.csv')
    
    #set index to date, drop the seconds do make synchronisising easier
    dates = {}
    for index in range(len(x.index)):
        dates[index] = datetime.fromtimestamp(x.index[index]).strftime('%Y-%m-%d %H:%M')#'%Y-%m-%d %H:%M:%S'
    x['date'] = dates.values()
    x = x.set_index('date')
    
    #label columns
    x.columns=['price','amount'] #doublecheck amount
    #unixtime,price,amount
    
    #reindex by removing duplicates(happens for high frequency)
    x3 = x[~x.index.duplicated(keep='first')]
    x3 = x3.sort_index()
    x3.index = pd.to_datetime(x3.index)
    # reindex BTC data with poloniex Altcoins
    x3 = x3.reindex(altcoin_data['ETH'].index, method='nearest')
    
    #btc_usd_datasets = pd.DataFrame(size=(x3.shape[0],x3.shape[1]))
    #btc_usd_datasets = pd.DataFrame(np.zeros((x3.shape[0],x3.shape[1])))
    btc_usd_datasets = x3
    btc_usd_datasets['avg_btc_price_usd'] = x3 ['price']


# In[]:

#Calculate USD prices for altcoins
# Instead of weightedAverage Open or Closing Prices can be used
for altcoin in altcoin_data.keys():
    altcoin_data[altcoin]['price_usd'] = altcoin_data[altcoin]['weightedAverage'] * btc_usd_datasets['avg_btc_price_usd']
#we've created a new column in each altcoin dataframe with the USD prices for that coin.



# In[]:

# This contains prices of altcoins in satoshi levels
# Instead of weightedAverage Open or Closing Prices or any other metric can be used
satoshi_df = merge_dfs_on_column(list(altcoin_data.values()),list(altcoin_data.keys()),'weightedAverage')
satoshi_df = merge_dfs_on_column(list(altcoin_data.values()),list(altcoin_data.keys()),'weightedAverage')

# This cointains prices in USD levels
combined_df = merge_dfs_on_column(list(altcoin_data.values()),list(altcoin_data.keys()),'price_usd')

# Add the Bitcoin dollar level to it:
combined_df['BTC'] = btc_usd_datasets['avg_btc_price_usd']

# In[]:

# This is only relevant if used in Jupyter, same goes for loaded plotly packages
#copypaste helperfunction
def df_scatter(df, title, seperate_y_axis=False, y_axis_label='', scale='linear', initial_hide=False):
    '''Generate a scatter plot of the entire dataframe'''
    label_arr = list(df)
    series_arr = list(map(lambda col: df[col], label_arr))
    
    layout = go.Layout(
        title=title,
        legend=dict(orientation="h"),
        xaxis=dict(type='date'),
        yaxis=dict(
            title=y_axis_label,
            showticklabels= not seperate_y_axis,
            type=scale
        )
    )
    
    y_axis_config = dict(
        overlaying='y',
        showticklabels=False,
        type=scale )
    
    visibility = 'visible'
    if initial_hide:
        visibility = 'legendonly'
        
    # Form Trace For Each Series
    trace_arr = []
    for index, series in enumerate(series_arr):
        trace = go.Scatter(
            x=series.index, 
            y=series, 
            name=label_arr[index],
            visible=visibility
        )
        
        # Add seperate axis for the series
        if seperate_y_axis:
            trace['yaxis'] = 'y{}'.format(index + 1)
            layout['yaxis{}'.format(index + 1)] = y_axis_config    
        trace_arr.append(trace)

    fig = go.Figure(data=trace_arr, layout=layout)
    py.iplot(fig)



# If required, only check 2017 data
    
combined_df_2017 = combined_df[combined_df.index.year == 2017]
combined_df_2017.pct_change().corr(method='pearson')

# In[]:

#Eyecandy for Jupyter

def correlation_heatmap(df, title, absolute_bounds=True):
    '''Plot a correlation heatmap for the entire dataframe'''
    heatmap = go.Heatmap(
        z=df.corr(method='pearson').as_matrix(),
        x=df.columns,
        y=df.columns,
        colorbar=dict(title='Pearson Coefficient'),
    )
    
    layout = go.Layout(title=title)
    
    if absolute_bounds:
        heatmap['zmax'] = 1.0
        heatmap['zmin'] = -1.0
        
    fig = go.Figure(data=[heatmap], layout=layout)
    py.iplot(fig)

