import requests
import json
import pyodbc
import time
import numpy as np
from datetime import datetime, timedelta
from td_ameritrade_variables import (
    accountid
    ,access_token
    ,access_token_expiry
    ,refresh_token, apikey
    ,driver
    ,server
    ,database
    ,username
    ,password
)

def APICallFx(request_method, url, payload, data, json, headers):
    #s = requests.Session()
    #a = requests.adapters.HTTPAdapter(max_retries=1)
    #s.mount('http://', a)
    
    r = requests.request(
        method = request_method
        ,url = url
        ,params = payload
        ,data = data
        ,json = json
        ,headers = headers
    )
    #convert content json to dictionary and return
    if not(r.ok):
        raise Exception('API call error: '+str(r.status_code)+' '+r.reason)        
    return r

def RefreshTokenFx():
    global access_token, access_token_expiry
    if datetime.now() >= access_token_expiry: # check to see if our exipiry has passed and if we need the API call
        url = 'https://api.tdameritrade.com/v1/oauth2/token'
        data = {
            'grant_type': 'refresh_token',
            'refresh_token': refresh_token,
            'client_id': apikey+'@AMER.OAUTHAP'
        }
        r = requests.post(url = url,data = data)
        if r.ok:
            access_token = r.json()['access_token']
            access_token_expiry = datetime.now() + timedelta(seconds=int(r.json()['expires_in']))
            print(str(datetime.now().strftime('%H:%M:%S')) + ' -- New access token received: '+str(r.status_code)+' '+r.reason)
        else:
            raise Exception(str(datetime.now().strftime('%H:%M:%S')) + ' -- Error recieving new access token: '+str(r.status_code)+' '+r.reason)
        return access_token
    else:
        return access_token

def ConnectSQLFx():
    global driver, server, database, username, password
    conn = pyodbc.connect(
        'DRIVER='+driver+';SERVER='+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password, autocommit=True
        )
    return conn

def GetQuoteFx(quote):
    # 1: Fist set global variables and check to see if a new access token is needed
    global apikey, access_token
    access_token = RefreshTokenFx()
    # 2: Continue witht he API call
    request_method = 'get'
    url = "https://api.tdameritrade.com/v1/marketdata/{}/quotes?apikey={}".format(quote, apikey)
    payload = {}
    data = {}
    json = {}
    headers = {
        "Authorization":"Bearer {}".format(access_token)
    }
    #Make API Call
    quote_data = APICallFx(request_method, url, payload, data, json, headers)
    return quote_data.json()

def GetOrderFx(orderid):
    # 1: Fist set global variables and check to see if a new access token is needed
    global accountid, access_token
    access_token = RefreshTokenFx()
    # 2: Continue witht he API call
    request_method = 'get'
    url = "https://api.tdameritrade.com/v1/accounts/{}/orders/{}".format(accountid, orderid)
    payload = {}
    data = {}
    json = {}
    headers = {
        "Authorization":"Bearer {}".format(access_token)
    }
    #Make API Call
    order_data = APICallFx(
        request_method=request_method
        ,url=url
        ,payload=payload
        ,data=data
        ,json=json
        ,headers=headers
    )
    return order_data

def PlaceOTAFx(symbol, assetType, buy_price, sell_price, buy_quantity, sell_quantity): # Needs some work to fill in the details
    # 1: Fist set global variables and check to see if a new access token is needed
    global accountid, access_token
    access_token = RefreshTokenFx()
    # 2: Continue witht he API call
    request_method = 'post'
    url = 'https://api.tdameritrade.com/v1/accounts/{}/orders'.format(accountid)
    payload = {}
    data = {}
    json = {
        "orderType": "LIMIT",
        "session": "NORMAL",
        "price": "{}".format(buy_price),
        "duration": "GOOD_TILL_CANCEL", #"DAY" "GOOD_TILL_CANCEL"
        "orderStrategyType": "TRIGGER",
        "orderLegCollection": [
            {
            "instruction": "BUY",
            "quantity": "{}".format(buy_quantity),
            "instrument": {
                "symbol": "{}".format(symbol),
                "assetType": "{}".format(assetType)
            }
            }
        ],
        "childOrderStrategies": [
            {
            "orderType": "LIMIT",
            "session": "NORMAL",
            "price": "{}".format(sell_price),
            "taxLotMethod":"LOW_COST", #'FIFO' or 'LIFO' or 'HIGH_COST' or 'LOW_COST' or 'AVERAGE_COST' or 'SPECIFIC_LOT'
            "duration": "GOOD_TILL_CANCEL", #"DAY" "GOOD_TILL_CANCEL"
            "orderStrategyType": "SINGLE",
            "orderLegCollection": [
                {
                "instruction": "SELL",
                "quantity": "{}".format(sell_quantity),
                "instrument": {
                    "symbol": "{}".format(symbol),
                    "assetType": "{}".format(assetType)
                }
                }
            ]
            }
        ]
    }
    headers = {
        "Authorization":"Bearer {}".format(access_token)
        ,"Content-Type":"application/json"
    }
    r = APICallFx(request_method, url, payload, data, json, headers)

    if r.ok:
        print('Purchased 1 OTA for TNA: BUY @ {} and SELL @ {}'.format(buy_price, sell_price)) #Maybe move this to order retreival part
    else:
        raise Exception('Error placing OTA order: '+str(r.status_code)+' '+r.reason)
    return r

def ExecuteSQL(conn, sql, params, print_true_false, returnList_true_false): 
    try:
        cursor = conn.cursor()
        cursor.execute(sql, params)
        rows = cursor.fetchall()
    except Exception as e:
        if str(e) == 'No results.  Previous SQL was not a query.':
            pass
        else:
            print('***ERROR STOPPING PROGRAM NOW----->' + str(e))
            raise
    else:
        if print_true_false:
            print(rows)
        if returnList_true_false:
            return rows

def GetOrderByQueryFx(fromEnteredTime, toEnteredTime, maxResults, status):
    # 1: Fist set global variables and check to see if a new access token is needed
    global accountid, access_token
    access_token = RefreshTokenFx()
    # 2: Continue witht he API call
    request_method = 'get'
    url = "https://api.tdameritrade.com/v1/orders"
    payload = {
        "accountId" : accountid,
        "maxResults" : maxResults,
        "fromEnteredTime" : fromEnteredTime,
        "toEnteredTime" : toEnteredTime,
        "status" : status
    }
    data = {}
    json = {}
    headers = {
        "Authorization":"Bearer {}".format(access_token)
    }
    #Make API Call
    order_data = APICallFx(request_method, url, payload, data, json, headers)
    return order_data

def OrderParser(dict_value, parent_orderId):
    dv = dict_value
    closeTime = datetime.strptime(dv['closeTime'], "%Y-%m-%dT%H:%M:%S%z") if 'closeTime' in dv.keys() else None
    poid = parent_orderId if parent_orderId else dv['orderId']
    relationship = 'PARENT' if parent_orderId is None else 'CHILD'
    
    if dv['orderType'] == 'LIMIT':
        price = dv['price']
    elif dv['orderType'] == 'MARKET':
        price = dv['orderActivityCollection'][0]['executionLegs'][0]['price']
    elif dv['orderType'] == 'TRAILING_STOP':
        price = dv['activationPrice']
    else:
        price = 9999
    y = (
        poid
        ,dv['orderId']
        ,relationship
        ,price
        ,dv['quantity']
        ,dv['orderLegCollection'][0]['instruction']
        ,dv['status']
        ,dv['orderType']
        ,datetime.strptime(dv['enteredTime'], "%Y-%m-%dT%H:%M:%S%z")
        ,closeTime
    )
    return y

def ParseOrderJson(order_list):
    fields = []
    for i in order_list:
        fields.append(OrderParser(dict_value=i, parent_orderId=None))
        if 'childOrderStrategies' in i.keys():
            fields.append(OrderParser(dict_value=i['childOrderStrategies'][0], parent_orderId=i['orderId']))
        else:
            print('Order Type - '+str(i['orderType']+' - not defined in OrderParser function. See TD order: '+str(i['orderId'])))
    return fields

def InsertNewOrdersIntoSQL(fromEnteredTime, toEnteredTime, maxResults, status, conn):
    orders = GetOrderByQueryFx(
        fromEnteredTime=fromEnteredTime
        ,toEnteredTime=toEnteredTime
        , maxResults=maxResults
        ,status=status
    )
    orders = orders.json()
    params = ParseOrderJson(order_list=orders)
    #print(params)
    sql = """
        MERGE [dbo].[orders2] AS TARGET
        USING (SELECT
                ? AS [parentOrderID]
                ,? AS [orderId]
                ,? AS [relationship]
                ,? AS [price]
                ,? AS [quantity]
                ,? AS [instruction]
                ,? AS [status]
                ,? AS [orderType]
                ,? AS [enteredTime]
                ,? AS [closeTime]
                ) AS SOURCE 
        ON (TARGET.orderId = SOURCE.orderID) 
        --When records are matched, update the records if there is any change
        WHEN MATCHED
        THEN UPDATE SET
            TARGET.[parentOrderID] = SOURCE.[parentOrderID]
            ,TARGET.[orderId] = SOURCE.[orderId]
            ,TARGET.[relationship] = SOURCE.[relationship]
            ,TARGET.[price] = SOURCE.[price]
            ,TARGET.[quantity] = SOURCE.[quantity]
            ,TARGET.[instruction] = SOURCE.[instruction]
            ,TARGET.[status] = SOURCE.[status]
            ,TARGET.[orderType] = SOURCE.[orderType]
            ,TARGET.[enteredTime] = SOURCE.[enteredTime]
            ,TARGET.[closeTime] = SOURCE.[closeTime] 
        --When no records are matched, insert the incoming records from source table to target table
        WHEN NOT MATCHED BY TARGET 
        THEN INSERT (
            [parentOrderID]
            ,[orderId]
            ,[relationship]
            ,[price]
            ,[quantity]
            ,[instruction]
            ,[status]
            ,[orderType]
            ,[enteredTime]
            ,[closeTime]
        ) VALUES (
            SOURCE.[parentOrderID]
            ,SOURCE.[orderId]
            ,SOURCE.[relationship]
            ,SOURCE.[price]
            ,SOURCE.[quantity]
            ,SOURCE.[instruction]
            ,SOURCE.[status]
            ,SOURCE.[orderType]
            ,SOURCE.[enteredTime]
            ,SOURCE.[closeTime]
        )
        ;
    """

    for p in params:
        ExecuteSQL(conn=conn, sql=sql, params=p, print_true_false=False, returnList_true_false = False)

def log(x):
    log_num = np.log(x)/np.log(108.5) # 105 NEEDS TO BE PUT IN HERE DYNAMICALLY#
    log_num_multiplied = (1-log_num)*1+1 #2 IS THE MULTIPLIER MORE AGGRESSIVE = HIGH NUMBER
    return log_num_multiplied

def BuyOrWait(last_buy_price, bidPrice, conn):
    if bidPrice <= (last_buy_price * .99) or bidPrice >= (last_buy_price * 1.01):
        PlaceOTAFx(
            symbol = "TNA"
            ,assetType = "EQUITY"
            ,buy_price = bidPrice
            #,sell_price = round(bidPrice*log(bidPrice),2)
            ,sell_price = round(bidPrice*1.01,2)
            ,buy_quantity = 335
            ,sell_quantity = 335
        )
        print(str(datetime.now().strftime('%H:%M:%S')) + ' NEW RANGE (MIN, LAST, MAX): ' + str(round(bidPrice * .98,2)) + ' --- '+str(bidPrice) +' --- ' + str(round(bidPrice * 1.02, 2)))
        
        today = datetime.now().strftime('%Y-%m-%d')
        InsertNewOrdersIntoSQL(fromEnteredTime=today, toEnteredTime=today, maxResults=None, status=None, conn=conn)
        return bidPrice
    else:
        return last_buy_price

def GetMinMaxFromOrder(conn):
    sql = "SELECT TOP 1 [price] FROM [dbo].[orders2] WHERE status = 'FILLED' ORDER BY [enteredTime] DESC, relationship DESC;"
    params = ()
    r = ExecuteSQL(conn=conn, sql=sql, params=params,print_true_false=False,returnList_true_false=True)
    last_order = round(float(r[0][0]), 2)
    return last_order

def GetAccountDetails(conn):
    # 1: Fist set global variables and check to see if a new access token is needed
    global access_token, accountid
    access_token = RefreshTokenFx()
    # 2: Continue witht he API call
    url = "https://api.tdameritrade.com/v1/accounts/{}".format(accountid)
    headers = {
        "Authorization":"Bearer {}".format(access_token)
    }
    #Make API Call
    order_data = APICallFx(request_method='get', url=url, payload={}, data={}, json={}, headers=headers)
    params = (
        str(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        ,order_data.json()['securitiesAccount']['currentBalances']['liquidationValue']
        ,order_data.json()['securitiesAccount']['currentBalances']['unsettledCash']
        ,order_data.json()['securitiesAccount']['currentBalances']['cashAvailableForTrading']
        ,order_data.json()['securitiesAccount']['currentBalances']['cashAvailableForWithdrawal']
    )
    #print(params)
    sql = """
        INSERT INTO account_values (date ,accountValue ,unsettledCash ,cashAvailableForTrading ,cashAvailableForWithdrawal)
        VALUES (?,?,?,?,?)
        ;
    """
    ExecuteSQL(conn=conn, sql=sql, params=params, print_true_false=False, returnList_true_false = False)

def Begin():
    #1 Connect to Azure SQL Server
    conn = ConnectSQLFx()
    last_buy_price = GetMinMaxFromOrder(conn)
    print('RANGE (MIN, LAST, MAX): ' + str(round(last_buy_price * .98,2)) + ' --- '+str(last_buy_price) +' --- ' + str(round(last_buy_price * 1.02, 2)))

    x = datetime(datetime.now().year, datetime.now().month, datetime.now().day, 14)
    while datetime.now() < x:
        try:
            bidPrice = round(float(GetQuoteFx('TNA')['TNA']['bidPrice']),2)
            last_buy_price = BuyOrWait(last_buy_price, bidPrice, conn)
        except:
            print(str(datetime.now().strftime('%H:%M:%S')) + ' Connection Issue. Waiting 10 seconds')
            time.sleep(10)
            pass
        time.sleep(1)
    print('All Done :)')