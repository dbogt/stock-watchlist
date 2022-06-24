#%% Import Packages
import streamlit as st
import requests 
import pandas as pd
from datetime import datetime
from time import mktime
from urllib.request import Request, urlopen 
import json
import re  #regular expressions
import time
from google.oauth2 import service_account
import plotly.express as px
from googleapiclient.discovery import build
from io import StringIO, BytesIO
from bs4 import BeautifulSoup
from datetime import datetime

#%% Streamlit App Config
if 'lastRefresh' not in st.session_state:
    st.session_state['lastRefresh'] = datetime.now().strftime("%m-%d-%Y, %H:%M:%S")

st.set_page_config(layout="wide",page_title='Stock Watchlists')

#%% Google Sheets API
sheet_url = st.secrets["private_gsheets_url"]
spreadsheet_id = st.secrets['spreadsheet_id']

#%% Google Sheets Functions
@st.experimental_singleton()
def connect():
    # Create a connection object.
    credentials = service_account.Credentials.from_service_account_info(
        st.secrets["gcp_service_account"],
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )

    service = build("sheets", "v4", credentials=credentials)
    gsheet_connector = service.spreadsheets()
    return gsheet_connector


def collect(gsheet_connector, userTab='test') -> pd.DataFrame:
    #Grabs the saved watchlist from a the user's tab in the google sheets file
    values = (
        gsheet_connector.values()
        .get(
            spreadsheetId= spreadsheet_id,
            # range="{}!A:C".format(userTab),
            range=userTab,
        )
        .execute()
    )

    df = pd.DataFrame(values["values"])
    df.columns = df.iloc[0]
    df = df[1:]
    return df


def insert(gsheet_connector, df, userTab='test') -> None:
    #Saves the current watchlist back into Google Sheets
    gsheet_connector.values().append(
        spreadsheetId=spreadsheet_id,
        range=userTab,
        body=dict(values=[df.columns.values.tolist()] + df.values.tolist()),
        valueInputOption="USER_ENTERED",
    ).execute()

def clear(gsheet_connector, userTab='test') -> None:
    #Clears the old watchlist on a user's tab
    gsheet_connector.values().batchClear(
        spreadsheetId=spreadsheet_id,
        body=dict(ranges=userTab)
    ).execute()

def addTab(gsheet_connector, tabName) -> None :
    #Create a mew tab in Google Sheets with the user's email as the tab name
    data = {'requests': [
        {
            'addSheet':{
                'properties':{'title': tabName}
            }
        }
    ]}
    gsheet_connector.batchUpdate(spreadsheetId=spreadsheet_id, 
            body=data).execute()

def sheetNames(gsheet_connector):
    #Grab all existing tab names to check if user already exists
    sheet_metadata = gsheet_connector.get(spreadsheetId=spreadsheet_id).execute()
    sheets = sheet_metadata.get('sheets', '')
    # title = sheets[0].get("properties", {}).get("title", "Sheet1")
    titles = [sh.get("properties", {}).get("title", "Sheet1") for sh in sheets]
    return titles

#%% Yahoo Finance Functions
#Source: https://maikros.github.io/yahoo-finance-python/

def get_crumbs_and_cookies(stock):
    """
    get crumb and cookies for historical data csv download from yahoo finance
    parameters: stock - short-handle identifier of the company 
    returns a tuple of header, crumb and cookie
    """
    
    url = 'https://finance.yahoo.com/quote/{}/history'.format(stock)
    with requests.session():
        header = {'Connection': 'keep-alive',
                   'Expires': '-1',
                   'Upgrade-Insecure-Requests': '1',
                   'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) \
                   AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.99 Safari/537.36'
                   }
        
        website = requests.get(url, headers=header)
        # soup = BeautifulSoup(website.text, 'lxml')
        soup = BeautifulSoup(website.text)
        crumb = re.findall('"CrumbStore":{"crumb":"(.+?)"}', str(soup))

        return (header, crumb[0], website.cookies)
    
def convert_to_unix(date):
    """
    converts date to unix timestamp
    parameters: date - in format (dd-mm-yyyy)
    returns integer unix timestamp
    """
    datum = datetime.strptime(date, '%d-%m-%Y')
    
    return int(mktime(datum.timetuple())) + 86400 #adding 1 day due to timezone issue

def fnYFinHist(stock, interval='1d', day_begin='01-01-2013', day_end='17-11-2021'):
    """
    queries yahoo finance api to receive historical data in csv file format
    
    parameters: 
        stock - short-handle identifier of the company
        interval - 1d, 1wk, 1mo - daily, weekly monthly data
        day_begin - starting date for the historical data (format: dd-mm-yyyy)
        day_end - final date of the data (format: dd-mm-yyyy)
    
    returns a list of comma seperated value lines
    """
    
    day_begin_unix = convert_to_unix(day_begin)
    day_end_unix = convert_to_unix(day_end)
    header, crumb, cookies = get_crumbs_and_cookies(stock)
    
    with requests.session():
        url = 'https://query1.finance.yahoo.com/v7/finance/download/' \
              '{stock}?period1={day_begin}&period2={day_end}&interval={interval}&events=history&crumb={crumb}' \
              .format(stock=stock, 
                      day_begin=day_begin_unix, day_end=day_end_unix,
                      interval=interval, crumb=crumb)
                
        website = requests.get(url, headers=header, cookies=cookies)

    data = pd.read_csv(StringIO(website.text), parse_dates=['Date'], index_col=['Date'])
    data['Returns'] = data['Close'].pct_change()
    return data

def fnYFinJSON(stock, field):
    if not stock:
        return "enter a ticker"
    else:
    	urlData = "https://query2.finance.yahoo.com/v7/finance/quote?symbols="+stock
    	webUrl = urlopen(urlData)
    	if (webUrl.getcode() == 200):
    		data = webUrl.read()
    	else:
    	    print ("Received an error from server, cannot retrieve results " + str(webUrl.getcode()))
    	yFinJSON = json.loads(data)
        
    try:
        tickerData = yFinJSON["quoteResponse"]["result"][0]
    except:
        return "N/A"
    if field in tickerData:
        return tickerData[field]
    else:
        return "N/A"

def updateDate():
    st.session_state['lastRefresh'] = datetime.now().strftime("%m-%d-%Y, %H:%M:%S")

@st.cache
def grabPricing(ticker, field, lastUpdate=st.session_state['lastRefresh']):
    fieldValue = fnYFinJSON(ticker, field)
    updateDate()
    return fieldValue

@st.cache
def grabPricingAll(ticker, interval, start, end, lastUpdate=st.session_state['lastRefresh']):
    df = fnYFinHist(ticker, interval, start, end)
    updateDate()
    return df

#%% Main App Functions
def cleanDF(df):
    cols = ['Price', 'Price Change', '% Change', 'Buy Target', 'Sell Target']
    for col in cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col])
    return df 

@st.cache(allow_output_mutation=True)
def initialGrab(activeUser):
    grabSavedList()

def grabSavedList():
    if activeUser in allUsers:
        # st.write("DEBUG: grabbin" + activeUser)
        userDF = collect(db, activeUser)
        userDF = cleanDF(userDF)
        # st.write("Your saved watchlist is:")
        # st.dataframe(userDF)
        userDF.set_index('index', inplace=True)
        dict1 = userDF.to_dict('index')
        # st.write(dict1)
        st.session_state[activeUser] = dict1
    else:
        placeholder.text("No watchlist saved yet under your profile. Creating profile now.")
        # st.write("No watchlist saved yet under your profile. Creating profile now.")
        addTab(db, activeUser)
        time.sleep(1)
        placeholder.text("New empty profile created")

def add_ticker():
    ticker = st.session_state.tickerBox
    buy = float(st.session_state.buyTarget)
    sell = float(st.session_state.sellTarget)
    stockName = fnYFinJSON(ticker, "displayName")
    if stockName == 'N/A':
        stockName = fnYFinJSON(ticker, "shortName")
    lastPrice = fnYFinJSON(ticker, "regularMarketPrice")
    stockPriceChg = fnYFinJSON(ticker, 'regularMarketChange')
    pctChg = fnYFinJSON(ticker, 'regularMarketChangePercent')
    currency = fnYFinJSON(ticker, 'currency')
    lastUpdate = datetime.now().strftime("%m-%d-%Y, %H:%M:%S")
    st.session_state[activeUser][ticker] = {'Company':stockName, 'Price':lastPrice, 'Price Change':stockPriceChg, "% Change":pctChg,'Buy Target':buy, 'Sell Target':sell, 'Currency':currency, 'Last Update':lastUpdate}

def targetHighlight(df):
    numCols = df.shape[0]
    highlightSell='color: green; font-weight: bold'
    highlightBuy='color: red; font-weight: bold'
    default = ''
    priceCol = "Price"
    if (df['Buy Target'] > 0) and (df[priceCol] <= (df["Buy Target"] * (1 + buyPercent/100))): 
        return [highlightBuy]*numCols
    elif (df['Sell Target'] > 0) and (df[priceCol] >= (df["Sell Target"] * (1 - sellPercent/100))): 
        return [highlightSell]*numCols
    else:
        return [default]*numCols

def checkAlerts(df):
    s = "<h3><font color='green'>{}</font></h3>"
    b = "<h3><font color='red'>{}</font></h3>"
    
    buyAlertsDF = df[df['Buy Target']>df['Price']]
    buyAlertsTickers = list(buyAlertsDF['index'])
    buyAlertsDF.set_index('index', inplace=True)

    sellAlertsDF = df[df['Sell Target']<df['Price']]
    sellAlertsTickers = list(sellAlertsDF['index'])
    sellAlertsDF.set_index('index', inplace=True)

    buyAlertOutput = "**<font color='red'>{}</font>**: current price of **{:.2f}** is **{:.1%}** lower than buy target of **{:.2f}** </br>"
    allBuyText = ""
    
    sellAlertOutput = "**<font color='green'>{}</font>**: current price of **{:.2f}** is **{:.1%}** higher than sell target of **{:.2f}** </br>"
    allSellText = ""
    with alertsContainer:
        if len(sellAlertsTickers)>0:
            st.markdown(s.format("ALERT! Following stocks have met sell target"), unsafe_allow_html=True)
            #st.write(sellAlertsTickers)
            for ticker in sellAlertsTickers:
                sellTarget = sellAlertsDF.loc[ticker]['Sell Target']
                price = sellAlertsDF.loc[ticker]['Price']
                percentOver = price / sellTarget - 1
                allSellText += sellAlertOutput.format(ticker, price, percentOver, sellTarget)
            st.markdown(allSellText, unsafe_allow_html=True)
        if len(buyAlertsTickers)>0:
            st.markdown(b.format("ALERT! Following stocks have met buy target"), unsafe_allow_html=True)
            #st.write(buyAlertsTickers)
            for ticker in buyAlertsTickers:
                buyTarget = buyAlertsDF.loc[ticker]['Buy Target']
                price = buyAlertsDF.loc[ticker]['Price']
                percentOver = (price / buyTarget - 1) * -1
                allBuyText += buyAlertOutput.format(ticker, price, percentOver, buyTarget)
            st.markdown(allBuyText, unsafe_allow_html=True)

def delete_ticker():
    ticker = st.session_state.tickerBox
    del st.session_state[activeUser][ticker]

@st.cache(allow_output_mutation=True)
def createExcel():
    #Download current watchlist
    buffer = BytesIO()
    with pd.ExcelWriter(buffer) as writer:
        df.to_excel(writer, sheet_name="MyWatchlist", index=True)
    return buffer



#%% Connect to Google Sheets
db = connect()
testDF = collect(db) #dummy watchlist

#%% Main App
allUsers = sheetNames(db)
activeUser = st.user['email']
st.title("Stock Watch List")
# initialGrab(str(activeUser))

appDetails = """
Created by: Bogdan Tudose, bogdan.tudose@marqueegroup.ca \n
Date: Feb 10, 2022 \n
Purpose: Streamlit February App-a-thon to test upcoming Streamlit features \n
App Details: The app checks the authenticated email of the user and allows them to store a list of tickers as watchlist.
The app connects to a private Google Sheets and creates a new tab for each user to save their watchlist.
When a user adds a new ticker to their watch list, they are asked to also enter a Buy and Sell target.
Live market data scraped from Yahoo Finance is also added to the watchlist table.
If any of the stocks meet the buy or sell target, they are added to the alerts section at top of the dashboard. \n
\n
Instructions:
- if you have previously created and saved a watchlist, click on the "Grab Saved List" button
- you can add new tickers and buy/sell targets in the sidebar menu
- you can update the targets or delete old tickers using the same sidebar form
- don't forget to hit "Save Data" to store your watchlist for future retrieval
- you can look up tickers live pricing by checking off "Lookup Stock" below
- you can also compare your watchlist with other past users by checking off "Compare watchlist to another user"
\n
Other notes: follow the ticker conventions of Yahoo Finance, e.g. XYZ.TO for Canadian tickers, etc. 
"""
with st.expander("See app info"):
    st.write(appDetails)

st.write("Current user is:")
st.write(activeUser)
st.write("**Don't forget to press Save Data to store your watchlist for future runs!**")
alertsContainer = st.container()

if activeUser == "bogdan.a.tudose@gmail.com":
    with st.expander("Admin Only:"):
        st.write("Spreadsheet ID:" + spreadsheet_id)
        st.write("Sheet URL:" + sheet_url)
        st.write("All users:")
        st.write(allUsers)
        if st.checkbox("Test another user"):
            adminUsersDrop = st.selectbox("Pick another user watchlist:", allUsers)
            activeUser = adminUsersDrop
            initialGrab(activeUser)

    

with st.form(key='grab_sheets'):
    grab_button = st.form_submit_button(label='Grab Saved List', on_click=grabSavedList)

placeholder = st.empty()

if activeUser not in st.session_state:
    st.write("Dummy watchlist:")
    testDF = cleanDF(testDF)
    testDF.set_index('index', inplace=True)
    dict1 = testDF.to_dict('index')
    st.session_state[activeUser] = dict1

st.sidebar.write("Add, delete or update a ticker from your watchlist")
with st.sidebar.form(key='my_form'):
    ticker_input = st.text_input('Enter a Ticker', key='tickerBox')
    buy_target = st.text_input('Buy Target', key='buyTarget')
    sell_target = st.text_input('Sell Target', key='sellTarget')

    submit_button = st.form_submit_button(label='Add', on_click=add_ticker)
    delete_button = st.form_submit_button(label='Delete', on_click=delete_ticker)



st.sidebar.write("Current ticker list")
st.sidebar.write(st.session_state[activeUser])

#%% Test dynamic tables
df = pd.DataFrame.from_dict(st.session_state[activeUser], orient='index').reset_index()

tableFormat = st.radio("Pick a table format for the watchlist", ('Dynamic Table','Conditional Formatted Table'))
st.write("Note: dataframe styling does not currently work with dynamic tables")
if tableFormat == 'Dynamic Table':
    st.dataframe(df, key='my_df')
elif tableFormat == 'Conditional Formatted Table':
    buyPercent = st.number_input("% within buy target", value=0)
    sellPercent = st.number_input("% within sell target", value=0)
    st.write("Bolded green rows denote securities with a price within {:.0%} of target sell price.".format(sellPercent/100))
    st.write("Bolded red rows denote securities with a price within {:.0%} of target buy price.".format(buyPercent/100))
    st.dataframe(df.style.apply(targetHighlight, axis=1))

saveBtn = st.button('Save Data')
checkAlerts(df)

if saveBtn:
    if activeUser not in allUsers:
        addTab(db, activeUser)
    clear(db, activeUser)    
    insert(db, df, activeUser)

st.download_button(
    label="Download Watchlist as Excel",
    data=createExcel(),
    file_name='watchlist.xlsx') 

if st.checkbox("Compare watchlist to another user"):
    usersDrop = st.selectbox("Pick another user to compare watchlists:", allUsers)
    otherUserDF = collect(db, usersDrop)
    st.header("{} Watchlist".format(usersDrop))
    st.dataframe(otherUserDF, key='other_df')


currencyMap = {'GBp':'GBp','USD':'US$','CAD':'C$','JPY':'¥'}

if st.checkbox("Lookup Stock"):
    #Current Price Charts
    stockDrop = st.text_input('Stock Ticker', value="DIS")
    startDate = st.date_input('Start Date', pd.to_datetime('2019-01-01'))
    endDate = st.date_input('End Date', datetime.now())
    #dates formatted for the YFin API
    dayStart = '{:%d-%m-%Y}'.format(startDate)
    dayEnd = '{:%d-%m-%Y}'.format(endDate)
    updateDate()
    stockDF = grabPricingAll(stockDrop, '1d', dayStart, dayEnd)
    stockName = grabPricing(stockDrop, 'displayName')
    if stockName == 'N/A':
        stockName = grabPricing(stockDrop, 'shortName')
    stockPrice = grabPricing(stockDrop, 'regularMarketPrice')
    stockPriceChg = grabPricing(stockDrop, 'regularMarketChange')
    stockPctChg = grabPricing(stockDrop, 'regularMarketChangePercent')
    stockCurrency = grabPricing(stockDrop,'currency')
    currency = currencyMap[stockCurrency]

    figStock = px.line(stockDF, x=stockDF.index, y='Close', title="{} - {}".format(stockDrop, stockName))

    st.header('Market Data')
    liveLinks = '''
    <a href="https://finance.yahoo.com/quote/{}/key-statistics?p={}" target="_blank">{} - YFinance Profile</a><br>
    '''.format(stockDrop,stockDrop, stockDrop)
    st.markdown(liveLinks, unsafe_allow_html=True)

    st.metric("{} - {}".format(stockDrop, stockName),
                "{}{:,.2f}".format(currency,stockPrice),
                "{:,.2f} ({:.2%})".format(stockPriceChg,stockPctChg/100))
    st.write("Last Refresh: " + st.session_state['lastRefresh'])
    st.plotly_chart(figStock)

 

# clearBtn = st.button('Clear Old Data')
# if clearBtn:
#     clear(db, activeUser)

# newTabBtn = st.button("Add new tab")
# if newTabBtn:
#     addTab(db, "newTest")
