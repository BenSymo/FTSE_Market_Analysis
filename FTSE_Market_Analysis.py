#!/usr/bin/env python
# coding: utf-8

# In[81]:


import requests
import pandas as pd
import yfinance as yf
from bs4 import BeautifulSoup
import yahoo_fin.stock_info as si

class Market_Analysis():
    def __init__(self, data_from, data_to, pull_dividends):
        """
        This method is called when the Market_Analysis object is initialised
        
        It takes in the following user inputs:
        pull_dividends, bool: controls whether the script fetches dividens from dividenddata
        """
        
        print('Initialising object')
        
        # Take in user inputs
        self.data_from = data_from
        self.data_to = data_to
        self.pull_dividends = pull_dividends
        
        # Get all ftse 100 & 250 tickets in one list
        self.tickers = sorted([f'{f}.L' for f in si.tickers_ftse100()                        + si.tickers_ftse250() if '.' not in f])
        
    def get_dividends(self):
        """
        This method scrapes www.dividenddata.co.uk to get declared dividend data 
        for the ftse100 and ftse250
        
        The output is a dataframe - self.dividends
        """
        
        if self.pull_dividends:
        
            print('Scraping declared dividends from www.dividenddata.co.uk')

            # specify the url of the webpage
            url_list = ['https://www.dividenddata.co.uk/exdividenddate.py?m=ftse100'
                        ,'https://www.dividenddata.co.uk/exdividenddate.py?m=ftse250']

            # Create an empty dataframe that we will append to
            self.dividends = pd.DataFrame()

            # Loop through each URL
            for url in url_list:

                # Set up the user agent otherwise this pull doesn't work
                with requests.Session() as se:
                    se.headers = {
                        "User-Agent": """Mozilla/5.0 (Windows NT 10.0; Win64; x64)""",
                    }

                # make a request to the webpage and store the response
                response = se.get(url)

                # parse the response using Beautiful Soup
                soup = BeautifulSoup(response.text, 'html.parser')

                # find the table with the specified class
                table = soup.find('table', {'class': 'table table-striped'})

                # extract the headers
                headers = [header.text for header in table.find_all('th')]

                # extract the data from the table
                data = []
                rows = table.find_all('tr')[1:]
                for row in rows:
                    cols = row.find_all('td')
                    cols = [col.text for col in cols]
                    data.append(cols)

                # create a dataframe from the extracted data then add it to the self.dividends df
                self.dividends = pd.concat([self.dividends, pd.DataFrame(data, columns=headers)]
                                           , ignore_index = True)
            
    def format_dividends(self):
        """
        This method applies some formatting to the dividends data pulled from self.get_dividends()
        """
        
        if self.pull_dividends:
        
            print('Formatting dividend data')

            # Rename columns
            self.dividends.columns = ['TICKER','NAME','MARKET','SHARE_PRICE','DIVIDEND','TYPE'
                                      ,'DIV_IMPACT','DECLARATION_DATE','EX-DIVIDEND_DATE','PAYMENT_DATE']

            # Add '.L' to the end of each tag
            self.dividends['TICKER'] += '.L'

            # Convert SHARE_PRICE to float
            self.dividends['SHARE_PRICE'] = self.dividends['SHARE_PRICE'].apply(lambda x: float(x[:-1]))

            # Convert DIV_IMPACT to a decimal
            self.dividends['DIV_IMPACT'] = self.dividends['DIV_IMPACT'].apply(lambda x: float(x[:-1]) / 100)

            # Recalc DIVIDEND
            self.dividends['DIVIDEND'] = self.dividends['SHARE_PRICE'] * self.dividends['DIV_IMPACT']

            # Set dates
            today = datetime.now().date()
            start = today - timedelta(days=365)
            end = today + timedelta(days=365)

            # Create a dataframe of dates and add the string format that dividenddata uses
            dates_df = pd.DataFrame(pd.date_range(start = start
                                                  , end = end
                                                  , freq = '1D')
                                    , columns = ['DATE'])
            dates_df['DATE_STR'] = dates_df['DATE'].apply(lambda x: x.strftime("%d-%b"))

            # Convert dates to the right format
            self.dividends['DECLARATION_DATE'] = self.dividends['DECLARATION_DATE'].apply(
                lambda x: dates_df.loc[(dates_df['DATE_STR'] == x), 'DATE'].min())
            self.dividends['EX-DIVIDEND_DATE'] = self.dividends['EX-DIVIDEND_DATE'].apply(
                lambda x: dates_df.loc[(dates_df['DATE_STR'] == x), 'DATE'].max())
            self.dividends['PAYMENT_DATE'] = self.dividends['PAYMENT_DATE'].apply(
                lambda x: dates_df.loc[(dates_df['DATE_STR'] == x), 'DATE'].max())
            
    def download_market_data(self):
        """
        This method scrapes market data from yahoo finance using the yfinance package
        """
        
        print('Downloading market data from yahoo finance')
        
        # Here we use yf.download function
        self.market_data = yf.download(
            tickers=self.tickers
            , threads=True
            , group_by='ticker'
            , start = self.data_from
            , end = self.data_to
            , interval='1d'
            , auto_adjust = False
            , actions = True)
        
    def reformat_market_data(self):
        """
        This method reformats the data that was scraped from yahoo finance
        """
        
        print('Reformatting Market Data')
        
        if len(self.tickers) > 1:
            # Reformat the data so the there is only one
            self.market_data = pd.DataFrame(self.market_data.T.stack()).reset_index()
            self.market_data.columns = ['Ticker','Type','Date','Value']
            self.market_data = self.market_data.set_index(['Ticker','Date','Type']).unstack().reset_index()

            # Collapse the columns into a 1D array
            self.market_data.columns = [c[0] if c[0] != 'Value' else c[1] for c in self.market_data.columns]
            
        else:
            self.market_data.reset_index(inplace=True)
            self.market_data.insert(0,'Ticker',self.tickers[0])
            
        self.market_data.columns = [c.upper() for c in self.market_data.columns]
        self.market_data.drop(columns = 'ADJ CLOSE', inplace=True)
        self.market_data.set_index('TICKER', inplace=True)
            
self = Market_Analysis(data_from = datetime(2022,1,1)
                       , data_to = datetime.now()
                       , pull_dividends = False)
self.get_dividends()
self.format_dividends()

if self.pull_dividends:
    display(self.dividends.head())

self.download_market_data()
self.reformat_market_data()

self.market_data

