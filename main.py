#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from functions import *

#    This script scrapes token metrics from coingecko.com
#    for the top 100 crypto-currencies and saves them to csv.
#    The script is easy to automate using crontab.


# Specify paths to where data files will be created / updated

datafile = 'daily_top_100.csv'
logfile = 'logging.txt'


# Specify the preferred order how the variables should be stored in the csv file
# Don't change once first data has been written to datafile

var_order = ['rank', 'mcUSD', 'mcBTC', 'priceUSD', 'priceBTC', 'vol24h',
            'percChange1h', 'percChange24h', 'percChange7d']


# Scrape Coingecko data for the top 100 coins
top100Dict = dailyTop100Snapshot(logfile=logfile)


# Save to csv as specified in datafile
updateCSV(top100Dict, datafile, order=var_order, verbose=False, logfile=logfile)
