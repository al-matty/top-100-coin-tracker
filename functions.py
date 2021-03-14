#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, inspect, sys
import random
import pandas as pd
from time import sleep
import urllib.request
from bs4 import BeautifulSoup
from datetime import datetime


# Helper function: Removes any '$', '%', and ',' from target string and converts to float
def clean(string):
    # Abort if scraped metric is empty or None
    assert string not in {None, ''}, \
        f"""
        Coingecko seems to have restructured their website.
        One of the metrics couldn't be scraped. Check {funcName}().
        """
    return float(string.replace(',','').replace('$','').replace('%',''))

# Appends a new row to csv as specified in fileName
def appendToCsv(fileName, varList, varNames, verbose=True):
    '''
    Appends each value in varList as a new row to a file as specified in fileName.
    Creates new file with header if not found in working dir.
    Aborts with error message if it would change shape[1] of csv (= number of vars per row).

    Format of header:    id,time,[varNames]
    Example for row:     0,2021 Feb 18 16:24,0.03,72,NaN,Yes,...

    1st value: Successive id (=first value in last row of file + 1).
    2nd value: The current time in format "2021 Feb 18 17:34"
    If there is no file yet: Creates file with header = id, timestamp, [varNames]
    '''

    # Get name of function for error messages (depends on inspect, sys)
    funcName = inspect.currentframe().f_code.co_name

    # Abort if number of variables and names don't add up.
    assert len(varList) == len(varNames), \
        f"{funcName}(): The number of variables and names to append to csv must be the same."

    rowsAdded = []

    # Get current time.
    timestamp = datetime.now()
    parsedTime = timestamp.strftime('%Y %b %d %H:%M')

    # Possibility: fileName doesn't exist yet. Create file with header and data.
    if not os.path.isfile(fileName):
        header = 'id,' + 'time,' + str(','.join(varNames))

        with open(fileName, 'a') as wfile:
            wfile.write(header)
            varList = [str(var) for var in varList]
            row = '\n' + '0' + ',' + parsedTime + ',' + str(','.join(varList))
            wfile.write(row)
            rowsAdded.append(row)

        if verbose:
            print(
            '''
            No file called "%s" has been found, so it has been created.
            Header:
            %s
            ''' % (fileName, header))
            print('Added new row to data: \t', row[1:])

    # Possibility: fileName exists. Only append new data.
    else:

        # Abort if number of variables to append differs from number of elements in csv header.
        with open(fileName, 'r') as infile:
            header = infile.readlines()[0]
            n_header = len(header.split(','))

        assert len(varList) + 2 == n_header, \
            f"""
            {funcName}(): You're trying to append a row of {len(varList)} variables to csv.
            In the csv header there are {n_header}. To be imported as pandas dataframe for analytics,
            the number of variables per row in the csv needs to stay consistent throughout all rows.
            """

        # Determine new id value based on most recent line of file.
        with open(fileName, 'r') as rfile:
            rows = rfile.readlines()
            try:
                # Write id, time, data to file.
                id_ = str(int(rows[-1].split(',')[0]) + 1)
                with open(fileName, 'a') as wfile:
                    varList = [str(var) for var in varList]
                    row = '\n' + str(id_) + ',' + parsedTime + ',' + str(','.join(varList))
                    wfile.write(row)
                    rowsAdded.append(row)

                    if verbose:
                        print('Added new row to data: \t', row[1:])

            # Possibility: id can't be determined from file. Abort.
            except ValueError:
                print('''
                The last line of "%s" doesn't start with a valid id value (int).
                Something is wrong with your data file.
                No data has been written to the file.''' % fileName)

# Calls appendToCsv(). Values in d (nested dict) per pool/token become veriables per row in csv
def updateCSV(d, fileName, order=None, verbose=True, logfile=None):
    '''
    Appends current pool data from nested dict to csv file to keep track of
    asset ratios over time.
    Order can be specified as list of variable names.
    logfile: If a textfile is specified, appends datetime & #rows to logfile.
    '''
    outDf = pd.DataFrame(d)

    # Possibility: Reorder data as specified in order
    if order:
        outDf = outDf.reindex(order)

    # Count rows before appending data
    if os.path.isfile(fileName):
        with open(fileName, 'r') as file:
            rowsBefore = len(file.readlines())
    else:
        rowsBefore = 0

    # Append data
    for pair in outDf:
        name = pair
        varNames = outDf[pair].index.tolist()
        varNames.insert(0, 'token')
        varList = outDf[pair].values.tolist()
        varList.insert(0, name)
        appendToCsv(fileName, varList, varNames, verbose=verbose)

    # Count rows after appending, prepare labeled sample row for printing
    with open(fileName, 'r') as file:
        lines = file.readlines()
        rowsAfter = len(lines)
        difference = rowsAfter - rowsBefore
        headerList = lines[0].strip().split(',')
        sampleRow = random.choice(lines[-difference:])
        sampleList = sampleRow.strip().split(',')

    printDf = pd.DataFrame(sampleList, index=headerList, columns=['Sample Row'])

    print(f'Appended {difference} rows to {fileName}.\nRandom sample:\n')
    print(printDf)

    # Option: Write summary to logfile
    if logfile:
        log(logfile, f'Appended {difference} rows to {fileName}.')

# Appends a row (datetime + log message) to a logfile.
def log(logfile, _str):
    '''
    Appends current date, time, and _str as row to logfile (i.e. logging.txt).
    '''
    # Get current time.
    timestamp = datetime.now()
    parsedTime = timestamp.strftime('%Y %b %d %H:%M')

    row = '\n' + parsedTime + '\t' + _str

    with open(logfile, 'a') as file:
        file.write(row)

# Helper function to scrape all metrics from a row out of the top 100 table on Coingecko.com
def metricsFromRow(row, BTCprice, logfile=None):
    '''
    Gets a row from the live coin table of coingecko.com.
    Returns dict {symbol: {metrics}} for this row / this coin.
    '''
    funcName = inspect.currentframe().f_code.co_name
    row = list(row)
    tokenDict = {}
    valDict = {}

    # Try to scrape metrics from row in table
    try:
        symbol = list(row[5])[1].get_text().split('\n')[-4]
        valDict['rank'] = int(clean(row[3].get_text()))
        valDict['priceUSD'] = clean(row[7].get_text())
        valDict['priceBTC'] = valDict['priceUSD'] / BTCprice
        valDict['percChange1h'] = clean(row[9].get_text())
        valDict['percChange24h'] = clean(row[11].get_text())
        valDict['percChange7d'] = clean(row[13].get_text())
        valDict['vol24h'] = clean(row[15].get_text())
        valDict['mcUSD'] = clean(row[17].get_text())
        valDict['mcBTC'] = valDict['mcUSD'] / BTCprice

    # Possibility: Metric can't be scraped from website
    except IndexError:
        message = f"{funcName}(): Couldn't scrape all metrics for {symbol}. Maybe the website changed?"
        print(message)

        # Option: Write to logfile
        if logfile:
            log(logfile, message)

    tokenDict[symbol] = valDict

    return tokenDict

# Scrapes metrics for the top 100 crytos from coingecko and returns them in a dictionary
def dailyTop100Snapshot(logfile=None):
    '''
    Visits Coingecko's main page and scrapes the table data (top 100 coins).
    Returns a nested dict of shape {'symbol': {'metric1': val, 'metric2': val, ...}}
    where the keys are the coin symbols, i.e. 'BTC', 'ETH'.
    '''
    top100Dict = {}

    # Scrape coingecko main page and parse to bs object
    url = 'https://www.coingecko.com/en'
    userAgent = 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko)' + \
        ' Chrome/41.0.2228.0 Safari/537.36'
    req = urllib.request.Request(url, headers= {'User-Agent' : userAgent})
    html = urllib.request.urlopen(req)
    bs = BeautifulSoup(html.read(), 'html.parser')

    # Get all table rows from website except for header row
    tableRows = bs.findAll('tr')[1:]

    # Get BTC price to calculate BTC-denominated metrics
    BTCprice = clean(list(tableRows[0])[7].get_text())

    # For every row (= coin) in table: Get metrics and append to top100Dict
    for row in tableRows:
        tokenDict = metricsFromRow(row, BTCprice, logfile=logfile)
        top100Dict.update(tokenDict)

    return top100Dict
