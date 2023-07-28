from threading import Thread
import http.client
import time
import json
import pyodbc
import datetime
import sqlite3
import pandas as pd
from finta import TA
from sqlalchemy import create_engine
import pandas_ta as ta
import urllib
import configparser
import datetime
from datetime import timedelta

config_obj = configparser.ConfigParser()
config_obj.read(".\configfile.ini")

dbparam = config_obj["mssql"]
server = dbparam["Server"]
db = dbparam["db"]

indexparam = config_obj["symbol"]
indexsymbol = indexparam["indexsymbol"]
cesymbol = indexparam["cesymbol"]
pesymbol = indexparam["pesymbol"]


# Populating OCS DB tables for Index
def populate_nifty_1m_table():
    while (True):
        try:
            time.sleep(59)
            # time.sleep(5)
            to_db_dict = {}

            swipe_in = datetime.datetime.today()
            new_swipe_in = (swipe_in - timedelta(minutes=1))
            s = new_swipe_in.replace(second=0, microsecond=0)
            from_epoch_time = int(s.timestamp())
            to_epoch_time = int(from_epoch_time) + 59

            # current_epoch_time = int(time.time())
            # from_epoch_time = current_epoch_time - 59
            # from_epoch_time = 1689824700
            # current_epoch_time = 1689824759

            print("from_epoch_time -", from_epoch_time)
            print("current_epoch_time -", to_epoch_time)

            conn = http.client.HTTPSConnection("nimblerest.lisuns.com", 4532)
            payload = ''
            headers = {}
            # conn.request("GET",
            #              "/GetHistory/?accessKey=b44bbc1d-f995-416e-9aa4-d8741fc68006&exchange=NFO&instrumentIdentifier=" + indexsymbol + "&periodicity=MINUTE&period=1&from=" + str(
            #                  from_epoch_time) + "&to=" + str(to_epoch_time), payload, headers)

            conn.request("GET","/GetHistory/?accessKey=b44bbc1d-f995-416e-9aa4-d8741fc68006&exchange=NFO&periodicity=MINUTE&period=1&max=2&instrumentIdentifier=" + indexsymbol, payload, headers)



            res = conn.getresponse()
            data = res.read()
            print(data)
            json_object = json.loads(data.decode("utf-8"))

            out_dict = json_object["OHLC"][1]

            to_db_dict["close"] = out_dict.get('CLOSE')
            to_db_dict["high"] = out_dict.get('HIGH')
            to_db_dict["last_trade_time"] = out_dict.get('LASTTRADETIME')
            to_db_dict["low"] = out_dict.get('LOW')
            to_db_dict["open"] = out_dict.get('OPEN')
            to_db_dict["open_interest"] = out_dict.get('OPENINTEREST')
            to_db_dict["quotation_lot"] = out_dict.get('QUOTATIONLOT')
            to_db_dict["traded_qty"] = out_dict.get('TRADEDQTY')
            to_db_dict["epoch"] = to_epoch_time
            to_db_dict["datetime"] = datetime.datetime.fromtimestamp(to_epoch_time).strftime('%d-%m-%Y %H:%M:%S')
            update_db(to_db_dict, "NIFTY_1m_Ticker")
            populate_NIFTY_1m_indicators()
            is_hammer("NIFTY_1m_Ticker")
            update_db_color("NIFTY_1m_Ticker")
            update_db_doji("NIFTY_1m_Ticker")
            # update_db_buy("NIFTY_1m_Ticker")
        except Exception as e:
            print(e)
            continue


def populate_nifty_2m_table():
    while (True):
        try:
            time.sleep(119)
            #             time.sleep(5)
            to_db_dict = {}

            swipe_in = datetime.datetime.today()
            new_swipe_in = (swipe_in - timedelta(minutes=2))
            s = new_swipe_in.replace(second=0, microsecond=0)
            from_epoch_time = int(s.timestamp())
            to_epoch_time = int(from_epoch_time) + 119

            # from_epoch_time = 1689824700
            # current_epoch_time = 1689824819
            # current_epoch_time = int(time.time())
            # from_epoch_time = current_epoch_time - 119

            print("from_epoch_time -", from_epoch_time)
            print("current_epoch_time -", to_epoch_time)



            conn = http.client.HTTPSConnection("nimblerest.lisuns.com", 4532)
            payload = ''
            headers = {}
            conn.request("GET",
                         "/GetHistory/?accessKey=b44bbc1d-f995-416e-9aa4-d8741fc68006&exchange=NFO&instrumentIdentifier=" + indexsymbol + "&periodicity=MINUTE&period=2&from=" + str(
                             from_epoch_time) + "&to=" + str(to_epoch_time), payload, headers)
            res = conn.getresponse()
            data = res.read()
            json_object = json.loads(data.decode("utf-8"))

            out_dict = json_object["OHLC"][0]

            to_db_dict["close"] = out_dict.get('CLOSE')
            to_db_dict["high"] = out_dict.get('HIGH')
            to_db_dict["last_trade_time"] = out_dict.get('LASTTRADETIME')
            to_db_dict["low"] = out_dict.get('LOW')
            to_db_dict["open"] = out_dict.get('OPEN')
            to_db_dict["open_interest"] = out_dict.get('OPENINTEREST')
            to_db_dict["quotation_lot"] = out_dict.get('QUOTATIONLOT')
            to_db_dict["traded_qty"] = out_dict.get('TRADEDQTY')
            to_db_dict["epoch"] = to_epoch_time
            to_db_dict["datetime"] = datetime.datetime.fromtimestamp(to_epoch_time).strftime('%d-%m-%Y %H:%M:%S')
            update_db(to_db_dict, "NIFTY_2m_Ticker")
            populate_NIFTY_2m_indicators()
            is_hammer("NIFTY_2m_Ticker")
        except Exception as e:
            print(e)
            continue


def populate_nifty_3m_table():
    while (True):
        try:
            time.sleep(179)
            #time.sleep(5)
            to_db_dict = {}

            swipe_in = datetime.datetime.today()
            new_swipe_in = (swipe_in - timedelta(minutes=3))
            s = new_swipe_in.replace(second=0, microsecond=0)
            from_epoch_time = int(s.timestamp())
            to_epoch_time = int(from_epoch_time) + 179

            # from_epoch_time = 1689824700
            # current_epoch_time = 1689824879
            # current_epoch_time = int(time.time())
            # from_epoch_time = current_epoch_time - 179

            print("from_epoch_time -", from_epoch_time)
            print("current_epoch_time -", to_epoch_time)



            conn = http.client.HTTPSConnection("nimblerest.lisuns.com", 4532)
            payload = ''
            headers = {}
            conn.request("GET",
                         "/GetHistory/?accessKey=b44bbc1d-f995-416e-9aa4-d8741fc68006&exchange=NFO&instrumentIdentifier=" + indexsymbol + "&periodicity=MINUTE&period=3&from=" + str(
                             from_epoch_time) + "&to=" + str(to_epoch_time), payload, headers)
            res = conn.getresponse()
            data = res.read()
            json_object = json.loads(data.decode("utf-8"))

            out_dict = json_object["OHLC"][0]

            to_db_dict["close"] = out_dict.get('CLOSE')
            to_db_dict["high"] = out_dict.get('HIGH')
            to_db_dict["last_trade_time"] = out_dict.get('LASTTRADETIME')
            to_db_dict["low"] = out_dict.get('LOW')
            to_db_dict["open"] = out_dict.get('OPEN')
            to_db_dict["open_interest"] = out_dict.get('OPENINTEREST')
            to_db_dict["quotation_lot"] = out_dict.get('QUOTATIONLOT')
            to_db_dict["traded_qty"] = out_dict.get('TRADEDQTY')
            to_db_dict["epoch"] = to_epoch_time
            to_db_dict["datetime"] = datetime.datetime.fromtimestamp(to_epoch_time).strftime('%d-%m-%Y %H:%M:%S')
            update_db(to_db_dict, "NIFTY_3m_Ticker")
            populate_NIFTY_3m_indicators()
            is_hammer("NIFTY_3m_Ticker")
        except Exception as e:
            print(e)
            continue


def populate_nifty_5m_table():
    while (True):
        try:
            time.sleep(299)
            #time.sleep(5)
            to_db_dict = {}

            swipe_in = datetime.datetime.today()
            new_swipe_in = (swipe_in - timedelta(minutes=5))
            s = new_swipe_in.replace(second=0, microsecond=0)
            from_epoch_time = int(s.timestamp())
            to_epoch_time = int(from_epoch_time) + 299

            # from_epoch_time = 1689824700
            # current_epoch_time = 1689824999
            # current_epoch_time = int(time.time())
            # from_epoch_time = current_epoch_time - 299

            print("from_epoch_time -", from_epoch_time)
            print("current_epoch_time -", to_epoch_time)



            conn = http.client.HTTPSConnection("nimblerest.lisuns.com", 4532)
            payload = ''
            headers = {}
            conn.request("GET",
                         "/GetHistory/?accessKey=b44bbc1d-f995-416e-9aa4-d8741fc68006&exchange=NFO&instrumentIdentifier=" + indexsymbol + "&periodicity=MINUTE&period=5&from=" + str(
                             from_epoch_time) + "&to=" + str(to_epoch_time), payload, headers)
            res = conn.getresponse()
            data = res.read()
            json_object = json.loads(data.decode("utf-8"))

            out_dict = json_object["OHLC"][0]

            to_db_dict["close"] = out_dict.get('CLOSE')
            to_db_dict["high"] = out_dict.get('HIGH')
            to_db_dict["last_trade_time"] = out_dict.get('LASTTRADETIME')
            to_db_dict["low"] = out_dict.get('LOW')
            to_db_dict["open"] = out_dict.get('OPEN')
            to_db_dict["open_interest"] = out_dict.get('OPENINTEREST')
            to_db_dict["quotation_lot"] = out_dict.get('QUOTATIONLOT')
            to_db_dict["traded_qty"] = out_dict.get('TRADEDQTY')
            to_db_dict["epoch"] = to_epoch_time
            to_db_dict["datetime"] = datetime.datetime.fromtimestamp(to_epoch_time).strftime('%d-%m-%Y %H:%M:%S')
            update_db(to_db_dict, "NIFTY_5m_Ticker")
            populate_NIFTY_5m_indicators()
            is_hammer("NIFTY_5m_Ticker")
        except Exception as e:
            print(e)
            continue


def populate_nifty_10m_table():
    while (True):
        try:
            time.sleep(599)
            #time.sleep(5)
            to_db_dict = {}

            swipe_in = datetime.datetime.today()
            new_swipe_in = (swipe_in - timedelta(minutes=10))
            s = new_swipe_in.replace(second=0, microsecond=0)
            from_epoch_time = int(s.timestamp())
            to_epoch_time = int(from_epoch_time) + 599

            # from_epoch_time = 1689824700
            # current_epoch_time = 1689825299
            # current_epoch_time = int(time.time())
            # from_epoch_time = current_epoch_time - 599

            print("from_epoch_time -", from_epoch_time)
            print("to_epoch_time -", to_epoch_time)



            conn = http.client.HTTPSConnection("nimblerest.lisuns.com", 4532)
            payload = ''
            headers = {}
            conn.request("GET",
                         "/GetHistory/?accessKey=b44bbc1d-f995-416e-9aa4-d8741fc68006&exchange=NFO&instrumentIdentifier=" + indexsymbol + "&periodicity=MINUTE&period=10&from=" + str(
                             from_epoch_time) + "&to=" + str(to_epoch_time), payload, headers)
            res = conn.getresponse()
            data = res.read()
            json_object = json.loads(data.decode("utf-8"))

            out_dict = json_object["OHLC"][0]

            to_db_dict["close"] = out_dict.get('CLOSE')
            to_db_dict["high"] = out_dict.get('HIGH')
            to_db_dict["last_trade_time"] = out_dict.get('LASTTRADETIME')
            to_db_dict["low"] = out_dict.get('LOW')
            to_db_dict["open"] = out_dict.get('OPEN')
            to_db_dict["open_interest"] = out_dict.get('OPENINTEREST')
            to_db_dict["quotation_lot"] = out_dict.get('QUOTATIONLOT')
            to_db_dict["traded_qty"] = out_dict.get('TRADEDQTY')
            to_db_dict["epoch"] = to_epoch_time
            to_db_dict["datetime"] = datetime.datetime.fromtimestamp(to_epoch_time).strftime('%d-%m-%Y %H:%M:%S')
            update_db(to_db_dict, "NIFTY_10m_Ticker")
            populate_NIFTY_10m_indicators()
            is_hammer("NIFTY_10m_Ticker")
        except Exception as e:
            print(e)
            continue


def populate_NIFTY_1m_indicators():

    conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                          r'Server=' + server + ';'
                          'Database=' + db + ';'
                          'Trusted_Connection=yes;')  # integrated security

    cur = conn.cursor()
    cur.execute('''SELECT * FROM NIFTY_1m_Ticker Order By datetime Asc''')
    myresult = cur.fetchall()

    df = pd.DataFrame((tuple(t) for t in myresult))
    df.columns = ['open', 'close', 'high', 'low', 'last_trade_time', 'open_interest', 'quotation_lot', 'traded_qty',
                  'epoch', 'datetime', 'sma', 'smma', 'adx', 'ema9', 'hammer', 'color', 'doji', 'buy']
    convert_dict = {'open': float,
                    'close': float,
                    'high': float,
                    'low': float
                    }

    df = df.astype(convert_dict)
    # print(df.dtypes)
    smma_series = TA.SMMA(df, 7)
    sma_series = TA.SMA(df, 20)
    adx_series = TA.ADX(df, 14)
    ema9_series = TA.EMA(df, 9)
    df = df.assign(smma=smma_series, sma=sma_series, adx=adx_series, ema9=ema9_series)

    rownum = 0
    for index, rows in df.iterrows():
        sma = df['sma'][rownum]
        smma = df['smma'][rownum]
        adx = df['adx'][rownum]
        ema9 = df['ema9'][rownum]
        epoch = df['epoch'][rownum]
        cur.execute("""update NIFTY_1m_Ticker SET sma = ?, smma = ?, adx = ?, ema9 = ? WHERE epoch = ?""", str(sma),
                    str(smma), str(adx), str(ema9), str(epoch))
        rownum += 1
    conn.commit()
    print("Data Successfully Inserted")
    conn.close()


def populate_NIFTY_2m_indicators():

    conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                          r'Server=' + server + ';'
                          'Database=' + db + ';'
                          'Trusted_Connection=yes;')  # integrated security

    cur = conn.cursor()
    cur.execute('''SELECT * FROM NIFTY_2m_Ticker''')
    myresult = cur.fetchall()

    df = pd.DataFrame((tuple(t) for t in myresult))
    df.columns = ['open', 'close', 'high', 'low', 'last_trade_time', 'open_interest', 'quotation_lot', 'traded_qty',
                  'epoch', 'datetime', 'sma', 'smma', 'adx', 'ema9', 'hammer']
    convert_dict = {'open': float,
                    'close': float,
                    'high': float,
                    'low': float
                    }

    df = df.astype(convert_dict)
    # print(df.dtypes)
    smma_series = TA.SMMA(df, 7)
    sma_series = TA.SMA(df, 20)
    adx_series = TA.ADX(df, 14)
    ema9_series = TA.EMA(df, 9)
    df = df.assign(smma=smma_series, sma=sma_series, adx=adx_series, ema9=ema9_series)

    rownum = 0
    for index, rows in df.iterrows():
        sma = df['sma'][rownum]
        smma = df['smma'][rownum]
        adx = df['adx'][rownum]
        ema9 = df['ema9'][rownum]
        epoch = df['epoch'][rownum]
        cur.execute("""update NIFTY_2m_Ticker SET sma = ?, smma = ?, adx = ?, ema9 = ? WHERE epoch = ?""", str(sma),
                    str(smma), str(adx), str(ema9), str(epoch))
        rownum += 1
    conn.commit()
    print("Data Successfully Inserted")
    conn.close()


def populate_NIFTY_3m_indicators():

    conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                          r'Server=' + server + ';'
                          'Database=' + db + ';'
                          'Trusted_Connection=yes;')  # integrated security

    cur = conn.cursor()
    cur.execute('''SELECT * FROM NIFTY_3m_Ticker''')
    myresult = cur.fetchall()

    df = pd.DataFrame((tuple(t) for t in myresult))
    df.columns = ['open', 'close', 'high', 'low', 'last_trade_time', 'open_interest', 'quotation_lot', 'traded_qty',
                  'epoch', 'datetime', 'sma', 'smma', 'adx', 'ema9', 'hammer']
    convert_dict = {'open': float,
                    'close': float,
                    'high': float,
                    'low': float
                    }

    df = df.astype(convert_dict)
    # print(df.dtypes)
    smma_series = TA.SMMA(df, 7)
    sma_series = TA.SMA(df, 20)
    adx_series = TA.ADX(df, 14)
    ema9_series = TA.EMA(df, 9)
    df = df.assign(smma=smma_series, sma=sma_series, adx=adx_series, ema9=ema9_series)

    rownum = 0
    for index, rows in df.iterrows():
        sma = df['sma'][rownum]
        smma = df['smma'][rownum]
        adx = df['adx'][rownum]
        ema9 = df['ema9'][rownum]
        epoch = df['epoch'][rownum]
        cur.execute("""update NIFTY_3m_Ticker SET sma = ?, smma = ?, adx = ?, ema9 = ? WHERE epoch = ?""", str(sma),
                    str(smma), str(adx), str(ema9), str(epoch))
        rownum += 1
    conn.commit()
    print("Data Successfully Inserted")
    conn.close()


def populate_NIFTY_5m_indicators():

    conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                          r'Server=' + server + ';'
                          'Database=' + db + ';'
                          'Trusted_Connection=yes;')  # integrated security

    cur = conn.cursor()
    cur.execute('''SELECT * FROM NIFTY_5m_Ticker''')
    myresult = cur.fetchall()

    df = pd.DataFrame((tuple(t) for t in myresult))
    df.columns = ['open', 'close', 'high', 'low', 'last_trade_time', 'open_interest', 'quotation_lot', 'traded_qty',
                  'epoch', 'datetime', 'sma', 'smma', 'adx', 'ema9', 'hammer']
    convert_dict = {'open': float,
                    'close': float,
                    'high': float,
                    'low': float
                    }

    df = df.astype(convert_dict)
    # print(df.dtypes)
    smma_series = TA.SMMA(df, 7)
    sma_series = TA.SMA(df, 20)
    adx_series = TA.ADX(df, 14)
    ema9_series = TA.EMA(df, 9)
    df = df.assign(smma=smma_series, sma=sma_series, adx=adx_series, ema9=ema9_series)

    rownum = 0
    for index, rows in df.iterrows():
        sma = df['sma'][rownum]
        smma = df['smma'][rownum]
        adx = df['adx'][rownum]
        ema9 = df['ema9'][rownum]
        epoch = df['epoch'][rownum]
        cur.execute("""update NIFTY_5m_Ticker SET sma = ?, smma = ?, adx = ?, ema9 = ? WHERE epoch = ?""", str(sma),
                    str(smma), str(adx), str(ema9), str(epoch))
        rownum += 1
    conn.commit()
    print("Data Successfully Inserted")
    conn.close()


def populate_NIFTY_10m_indicators():

    conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                          r'Server=' + server + ';'
                          'Database=' + db + ';'
                          'Trusted_Connection=yes;')  # integrated security

    cur = conn.cursor()
    cur.execute('''SELECT * FROM NIFTY_10m_Ticker''')
    myresult = cur.fetchall()

    df = pd.DataFrame((tuple(t) for t in myresult))
    df.columns = ['open', 'close', 'high', 'low', 'last_trade_time', 'open_interest', 'quotation_lot', 'traded_qty',
                  'epoch', 'datetime', 'sma', 'smma', 'adx', 'ema9', 'hammer']
    convert_dict = {'open': float,
                    'close': float,
                    'high': float,
                    'low': float
                    }

    df = df.astype(convert_dict)
    # print(df.dtypes)
    smma_series = TA.SMMA(df, 7)
    sma_series = TA.SMA(df, 20)
    adx_series = TA.ADX(df, 14)
    ema9_series = TA.EMA(df, 9)
    df = df.assign(smma=smma_series, sma=sma_series, adx=adx_series, ema9=ema9_series)

    rownum = 0
    for index, rows in df.iterrows():
        sma = df['sma'][rownum]
        smma = df['smma'][rownum]
        adx = df['adx'][rownum]
        ema9 = df['ema9'][rownum]
        epoch = df['epoch'][rownum]
        cur.execute("""update NIFTY_10m_Ticker SET sma = ?, smma = ?, adx = ?, ema9 = ? WHERE epoch = ?""", str(sma),
                    str(smma), str(adx), str(ema9), str(epoch))
        rownum += 1
    conn.commit()
    print("Data Successfully Inserted")
    conn.close()




# Populating OCS DB tables for Symbol - CE
def populate_CE_1m_table():
    while (True):
        try:
            time.sleep(59)
            # time.sleep(5)
            to_db_dict = {}

            swipe_in = datetime.datetime.today()
            new_swipe_in = (swipe_in - timedelta(minutes=1))
            s = new_swipe_in.replace(second=0, microsecond=0)
            from_epoch_time = int(s.timestamp())
            to_epoch_time = int(from_epoch_time) + 59

            # current_epoch_time = int(time.time())
            # from_epoch_time = current_epoch_time - 59
            # from_epoch_time = 1689824700
            # current_epoch_time = 1689824759

            print("from_epoch_time -", from_epoch_time)
            print("to_epoch_time -", to_epoch_time)

            conn = http.client.HTTPSConnection("nimblerest.lisuns.com", 4532)
            payload = ''
            headers = {}
            # conn.request("GET",
            #              "/GetHistory/?accessKey=b44bbc1d-f995-416e-9aa4-d8741fc68006&exchange=NFO&instrumentIdentifier=" + cesymbol + "&periodicity=MINUTE&period=1&from=" + str(
            #                  from_epoch_time) + "&to=" + str(to_epoch_time), payload, headers)

            conn.request("GET",
                         "/GetHistory/?accessKey=b44bbc1d-f995-416e-9aa4-d8741fc68006&exchange=NFO&periodicity=MINUTE&period=1&max=2&instrumentIdentifier=" + cesymbol,
                         payload, headers)

            res = conn.getresponse()
            data = res.read()
            print(data)
            json_object = json.loads(data.decode("utf-8"))

            out_dict = json_object["OHLC"][1]

            to_db_dict["close"] = out_dict.get('CLOSE')
            to_db_dict["high"] = out_dict.get('HIGH')
            to_db_dict["last_trade_time"] = out_dict.get('LASTTRADETIME')
            to_db_dict["low"] = out_dict.get('LOW')
            to_db_dict["open"] = out_dict.get('OPEN')
            to_db_dict["open_interest"] = out_dict.get('OPENINTEREST')
            to_db_dict["quotation_lot"] = out_dict.get('QUOTATIONLOT')
            to_db_dict["traded_qty"] = out_dict.get('TRADEDQTY')
            to_db_dict["epoch"] = to_epoch_time
            to_db_dict["datetime"] = datetime.datetime.fromtimestamp(to_epoch_time).strftime('%d-%m-%Y %H:%M:%S')
            update_db(to_db_dict, "CE_STRIKE_1m_Ticker")
            populate_CE_1m_indicators()
            is_hammer("CE_STRIKE_1m_Ticker")
            update_db_color("CE_STRIKE_1m_Ticker")
            update_db_doji("CE_STRIKE_1m_Ticker")
            # update_db_buy("CE_STRIKE_1m_Ticker")
        except Exception as e:
            print(e)
            continue


def populate_CE_2m_table():
    while (True):
        try:
            time.sleep(119)
            #             time.sleep(5)
            to_db_dict = {}

            swipe_in = datetime.datetime.today()
            new_swipe_in = (swipe_in - timedelta(minutes=2))
            s = new_swipe_in.replace(second=0, microsecond=0)
            from_epoch_time = int(s.timestamp())
            to_epoch_time = int(from_epoch_time) + 119

            # from_epoch_time = 1689824700
            # current_epoch_time = 1689824819
            # current_epoch_time = int(time.time())
            # from_epoch_time = current_epoch_time - 119

            print("from_epoch_time -", from_epoch_time)
            print("to_epoch_time -", to_epoch_time)



            conn = http.client.HTTPSConnection("nimblerest.lisuns.com", 4532)
            payload = ''
            headers = {}
            conn.request("GET",
                         "/GetHistory/?accessKey=b44bbc1d-f995-416e-9aa4-d8741fc68006&exchange=NFO&instrumentIdentifier=" + cesymbol + "&periodicity=MINUTE&period=2&from=" + str(
                             from_epoch_time) + "&to=" + str(to_epoch_time), payload, headers)
            res = conn.getresponse()
            data = res.read()
            json_object = json.loads(data.decode("utf-8"))

            out_dict = json_object["OHLC"][0]

            to_db_dict["close"] = out_dict.get('CLOSE')
            to_db_dict["high"] = out_dict.get('HIGH')
            to_db_dict["last_trade_time"] = out_dict.get('LASTTRADETIME')
            to_db_dict["low"] = out_dict.get('LOW')
            to_db_dict["open"] = out_dict.get('OPEN')
            to_db_dict["open_interest"] = out_dict.get('OPENINTEREST')
            to_db_dict["quotation_lot"] = out_dict.get('QUOTATIONLOT')
            to_db_dict["traded_qty"] = out_dict.get('TRADEDQTY')
            to_db_dict["epoch"] = to_epoch_time
            to_db_dict["datetime"] = datetime.datetime.fromtimestamp(to_epoch_time).strftime('%d-%m-%Y %H:%M:%S')
            update_db(to_db_dict, "CE_STRIKE_2m_Ticker")
            populate_CE_2m_indicators()
            is_hammer("CE_STRIKE_2m_Ticker")
        except Exception as e:
            print(e)
            continue


def populate_CE_3m_table():
    while (True):
        try:
            time.sleep(179)
            #time.sleep(5)
            to_db_dict = {}

            swipe_in = datetime.datetime.today()
            new_swipe_in = (swipe_in - timedelta(minutes=3))
            s = new_swipe_in.replace(second=0, microsecond=0)
            from_epoch_time = int(s.timestamp())
            to_epoch_time = int(from_epoch_time) + 179

            # from_epoch_time = 1689824700
            # current_epoch_time = 1689824879
            # current_epoch_time = int(time.time())
            # from_epoch_time = current_epoch_time - 179

            print("from_epoch_time -", from_epoch_time)
            print("to_epoch_time -", to_epoch_time)



            conn = http.client.HTTPSConnection("nimblerest.lisuns.com", 4532)
            payload = ''
            headers = {}
            conn.request("GET",
                         "/GetHistory/?accessKey=b44bbc1d-f995-416e-9aa4-d8741fc68006&exchange=NFO&instrumentIdentifier=" + cesymbol + "&periodicity=MINUTE&period=3&from=" + str(
                             from_epoch_time) + "&to=" + str(to_epoch_time), payload, headers)
            res = conn.getresponse()
            data = res.read()
            json_object = json.loads(data.decode("utf-8"))

            out_dict = json_object["OHLC"][0]

            to_db_dict["close"] = out_dict.get('CLOSE')
            to_db_dict["high"] = out_dict.get('HIGH')
            to_db_dict["last_trade_time"] = out_dict.get('LASTTRADETIME')
            to_db_dict["low"] = out_dict.get('LOW')
            to_db_dict["open"] = out_dict.get('OPEN')
            to_db_dict["open_interest"] = out_dict.get('OPENINTEREST')
            to_db_dict["quotation_lot"] = out_dict.get('QUOTATIONLOT')
            to_db_dict["traded_qty"] = out_dict.get('TRADEDQTY')
            to_db_dict["epoch"] = to_epoch_time
            to_db_dict["datetime"] = datetime.datetime.fromtimestamp(to_epoch_time).strftime('%d-%m-%Y %H:%M:%S')
            update_db(to_db_dict, "CE_STRIKE_3m_Ticker")
            populate_CE_3m_indicators()
            is_hammer("CE_STRIKE_3m_Ticker")
        except Exception as e:
            print(e)
            continue


def populate_CE_5m_table():
    while (True):
        try:
            time.sleep(299)
            #time.sleep(5)
            to_db_dict = {}

            swipe_in = datetime.datetime.today()
            new_swipe_in = (swipe_in - timedelta(minutes=5))
            s = new_swipe_in.replace(second=0, microsecond=0)
            from_epoch_time = int(s.timestamp())
            to_epoch_time = int(from_epoch_time) + 299

            # from_epoch_time = 1689824700
            # current_epoch_time = 1689824999
            # current_epoch_time = int(time.time())
            # from_epoch_time = current_epoch_time - 299

            print("from_epoch_time -", from_epoch_time)
            print("to_epoch_time -", to_epoch_time)



            conn = http.client.HTTPSConnection("nimblerest.lisuns.com", 4532)
            payload = ''
            headers = {}
            conn.request("GET",
                         "/GetHistory/?accessKey=b44bbc1d-f995-416e-9aa4-d8741fc68006&exchange=NFO&instrumentIdentifier=" + cesymbol + "&periodicity=MINUTE&period=5&from=" + str(
                             from_epoch_time) + "&to=" + str(to_epoch_time), payload, headers)
            res = conn.getresponse()
            data = res.read()
            json_object = json.loads(data.decode("utf-8"))

            out_dict = json_object["OHLC"][0]

            to_db_dict["close"] = out_dict.get('CLOSE')
            to_db_dict["high"] = out_dict.get('HIGH')
            to_db_dict["last_trade_time"] = out_dict.get('LASTTRADETIME')
            to_db_dict["low"] = out_dict.get('LOW')
            to_db_dict["open"] = out_dict.get('OPEN')
            to_db_dict["open_interest"] = out_dict.get('OPENINTEREST')
            to_db_dict["quotation_lot"] = out_dict.get('QUOTATIONLOT')
            to_db_dict["traded_qty"] = out_dict.get('TRADEDQTY')
            to_db_dict["epoch"] = to_epoch_time
            to_db_dict["datetime"] = datetime.datetime.fromtimestamp(to_epoch_time).strftime('%d-%m-%Y %H:%M:%S')
            update_db(to_db_dict, "CE_STRIKE_5m_Ticker")
            populate_CE_5m_indicators()
            is_hammer("CE_STRIKE_5m_Ticker")
        except Exception as e:
            print(e)
            continue


def populate_CE_10m_table():
    while (True):
        try:
            time.sleep(599)
            #time.sleep(5)
            to_db_dict = {}

            swipe_in = datetime.datetime.today()
            new_swipe_in = (swipe_in - timedelta(minutes=10))
            s = new_swipe_in.replace(second=0, microsecond=0)
            from_epoch_time = int(s.timestamp())
            to_epoch_time = int(from_epoch_time) + 599

            # from_epoch_time = 1689824700
            # current_epoch_time = 1689825299
            # current_epoch_time = int(time.time())
            # from_epoch_time = current_epoch_time - 599

            print("from_epoch_time -", from_epoch_time)
            print("to_epoch_time -", to_epoch_time)



            conn = http.client.HTTPSConnection("nimblerest.lisuns.com", 4532)
            payload = ''
            headers = {}
            conn.request("GET",
                         "/GetHistory/?accessKey=b44bbc1d-f995-416e-9aa4-d8741fc68006&exchange=NFO&instrumentIdentifier=" + cesymbol + "&periodicity=MINUTE&period=10&from=" + str(
                             from_epoch_time) + "&to=" + str(to_epoch_time), payload, headers)
            res = conn.getresponse()
            data = res.read()
            json_object = json.loads(data.decode("utf-8"))

            out_dict = json_object["OHLC"][0]

            to_db_dict["close"] = out_dict.get('CLOSE')
            to_db_dict["high"] = out_dict.get('HIGH')
            to_db_dict["last_trade_time"] = out_dict.get('LASTTRADETIME')
            to_db_dict["low"] = out_dict.get('LOW')
            to_db_dict["open"] = out_dict.get('OPEN')
            to_db_dict["open_interest"] = out_dict.get('OPENINTEREST')
            to_db_dict["quotation_lot"] = out_dict.get('QUOTATIONLOT')
            to_db_dict["traded_qty"] = out_dict.get('TRADEDQTY')
            to_db_dict["epoch"] = to_epoch_time
            to_db_dict["datetime"] = datetime.datetime.fromtimestamp(to_epoch_time).strftime('%d-%m-%Y %H:%M:%S')
            update_db(to_db_dict, "CE_STRIKE_10m_Ticker")
            populate_CE_10m_indicators()
            is_hammer("CE_STRIKE_10m_Ticker")
        except Exception as e:
            print(e)
            continue


def populate_CE_1m_indicators():

    conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                          r'Server=' + server + ';'
                          'Database=' + db + ';'
                          'Trusted_Connection=yes;')  # integrated security

    cur = conn.cursor()
    cur.execute('''SELECT * FROM CE_STRIKE_1m_Ticker Order By datetime Asc''')
    myresult = cur.fetchall()

    df = pd.DataFrame((tuple(t) for t in myresult))
    df.columns = ['open', 'close', 'high', 'low', 'last_trade_time', 'open_interest', 'quotation_lot', 'traded_qty',
                  'epoch', 'datetime', 'sma', 'smma', 'adx', 'ema9', 'hammer', 'color', 'doji', 'buy', 'reason']
    convert_dict = {'open': float,
                    'close': float,
                    'high': float,
                    'low': float
                    }

    df = df.astype(convert_dict)
    # print(df.dtypes)
    smma_series = TA.SMMA(df, 7)
    sma_series = TA.SMA(df, 20)
    adx_series = TA.ADX(df, 14)
    ema9_series = TA.EMA(df, 9)
    df = df.assign(smma=smma_series, sma=sma_series, adx=adx_series, ema9=ema9_series)

    rownum = 0
    for index, rows in df.iterrows():
        sma = df['sma'][rownum]
        smma = df['smma'][rownum]
        adx = df['adx'][rownum]
        ema9 = df['ema9'][rownum]
        epoch = df['epoch'][rownum]
        cur.execute("""update CE_STRIKE_1m_Ticker SET sma = ?, smma = ?, adx = ?, ema9 = ? WHERE epoch = ?""", str(sma),
                    str(smma), str(adx), str(ema9), str(epoch))
        rownum += 1
    conn.commit()
    print("Data Successfully Inserted")
    conn.close()


def populate_CE_2m_indicators():

    conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                          r'Server=' + server + ';'
                          'Database=' + db + ';'
                          'Trusted_Connection=yes;')  # integrated security

    cur = conn.cursor()
    cur.execute('''SELECT * FROM CE_STRIKE_2m_Ticker''')
    myresult = cur.fetchall()

    df = pd.DataFrame((tuple(t) for t in myresult))
    df.columns = ['open', 'close', 'high', 'low', 'last_trade_time', 'open_interest', 'quotation_lot', 'traded_qty',
                  'epoch', 'datetime', 'sma', 'smma', 'adx', 'ema9', 'hammer']
    convert_dict = {'open': float,
                    'close': float,
                    'high': float,
                    'low': float
                    }

    df = df.astype(convert_dict)
    # print(df.dtypes)
    smma_series = TA.SMMA(df, 7)
    sma_series = TA.SMA(df, 20)
    adx_series = TA.ADX(df, 14)
    ema9_series = TA.EMA(df, 9)
    df = df.assign(smma=smma_series, sma=sma_series, adx=adx_series, ema9=ema9_series)

    rownum = 0
    for index, rows in df.iterrows():
        sma = df['sma'][rownum]
        smma = df['smma'][rownum]
        adx = df['adx'][rownum]
        ema9 = df['ema9'][rownum]
        epoch = df['epoch'][rownum]
        cur.execute("""update CE_STRIKE_2m_Ticker SET sma = ?, smma = ?, adx = ?, ema9 = ? WHERE epoch = ?""", str(sma),
                    str(smma), str(adx), str(ema9), str(epoch))
        rownum += 1
    conn.commit()
    print("Data Successfully Inserted")
    conn.close()


def populate_CE_3m_indicators():

    conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                          r'Server=' + server + ';'
                          'Database=' + db + ';'
                          'Trusted_Connection=yes;')  # integrated security

    cur = conn.cursor()
    cur.execute('''SELECT * FROM CE_STRIKE_3m_Ticker''')
    myresult = cur.fetchall()

    df = pd.DataFrame((tuple(t) for t in myresult))
    df.columns = ['open', 'close', 'high', 'low', 'last_trade_time', 'open_interest', 'quotation_lot', 'traded_qty',
                  'epoch', 'datetime', 'sma', 'smma', 'adx', 'ema9', 'hammer']
    convert_dict = {'open': float,
                    'close': float,
                    'high': float,
                    'low': float
                    }

    df = df.astype(convert_dict)
    # print(df.dtypes)
    smma_series = TA.SMMA(df, 7)
    sma_series = TA.SMA(df, 20)
    adx_series = TA.ADX(df, 14)
    ema9_series = TA.EMA(df, 9)
    df = df.assign(smma=smma_series, sma=sma_series, adx=adx_series, ema9=ema9_series)

    rownum = 0
    for index, rows in df.iterrows():
        sma = df['sma'][rownum]
        smma = df['smma'][rownum]
        adx = df['adx'][rownum]
        ema9 = df['ema9'][rownum]
        epoch = df['epoch'][rownum]
        cur.execute("""update CE_STRIKE_3m_Ticker SET sma = ?, smma = ?, adx = ?, ema9 = ? WHERE epoch = ?""", str(sma),
                    str(smma), str(adx), str(ema9), str(epoch))
        rownum += 1
    conn.commit()
    print("Data Successfully Inserted")
    conn.close()


def populate_CE_5m_indicators():

    conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                          r'Server=' + server + ';'
                          'Database=' + db + ';'
                          'Trusted_Connection=yes;')  # integrated security

    cur = conn.cursor()
    cur.execute('''SELECT * FROM CE_STRIKE_5m_Ticker''')
    myresult = cur.fetchall()

    df = pd.DataFrame((tuple(t) for t in myresult))
    df.columns = ['open', 'close', 'high', 'low', 'last_trade_time', 'open_interest', 'quotation_lot', 'traded_qty',
                  'epoch', 'datetime', 'sma', 'smma', 'adx', 'ema9', 'hammer']
    convert_dict = {'open': float,
                    'close': float,
                    'high': float,
                    'low': float
                    }

    df = df.astype(convert_dict)
    # print(df.dtypes)
    smma_series = TA.SMMA(df, 7)
    sma_series = TA.SMA(df, 20)
    adx_series = TA.ADX(df, 14)
    ema9_series = TA.EMA(df, 9)
    df = df.assign(smma=smma_series, sma=sma_series, adx=adx_series, ema9=ema9_series)

    rownum = 0
    for index, rows in df.iterrows():
        sma = df['sma'][rownum]
        smma = df['smma'][rownum]
        adx = df['adx'][rownum]
        ema9 = df['ema9'][rownum]
        epoch = df['epoch'][rownum]
        cur.execute("""update CE_STRIKE_5m_Ticker SET sma = ?, smma = ?, adx = ?, ema9 = ? WHERE epoch = ?""", str(sma),
                    str(smma), str(adx), str(ema9), str(epoch))
        rownum += 1
    conn.commit()
    print("Data Successfully Inserted")
    conn.close()


def populate_CE_10m_indicators():

    conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                          r'Server=' + server + ';'
                          'Database=' + db + ';'
                          'Trusted_Connection=yes;')  # integrated security

    cur = conn.cursor()
    cur.execute('''SELECT * FROM CE_STRIKE_10m_Ticker''')
    myresult = cur.fetchall()

    df = pd.DataFrame((tuple(t) for t in myresult))
    df.columns = ['open', 'close', 'high', 'low', 'last_trade_time', 'open_interest', 'quotation_lot', 'traded_qty',
                  'epoch', 'datetime', 'sma', 'smma', 'adx', 'ema9', 'hammer']
    convert_dict = {'open': float,
                    'close': float,
                    'high': float,
                    'low': float
                    }

    df = df.astype(convert_dict)
    # print(df.dtypes)
    smma_series = TA.SMMA(df, 7)
    sma_series = TA.SMA(df, 20)
    adx_series = TA.ADX(df, 14)
    ema9_series = TA.EMA(df, 9)
    df = df.assign(smma=smma_series, sma=sma_series, adx=adx_series, ema9=ema9_series)

    rownum = 0
    for index, rows in df.iterrows():
        sma = df['sma'][rownum]
        smma = df['smma'][rownum]
        adx = df['adx'][rownum]
        ema9 = df['ema9'][rownum]
        epoch = df['epoch'][rownum]
        cur.execute("""update CE_STRIKE_10m_Ticker SET sma = ?, smma = ?, adx = ?, ema9 = ? WHERE epoch = ?""", str(sma),
                    str(smma), str(adx), str(ema9), str(epoch))
        rownum += 1
    conn.commit()
    print("Data Successfully Inserted")
    conn.close()




# Populating OCS DB tables for Symbol - PE
def populate_PE_1m_table():
    while (True):
        try:
            time.sleep(59)
            #time.sleep(5)
            to_db_dict = {}

            swipe_in = datetime.datetime.today()
            new_swipe_in = (swipe_in - timedelta(minutes=1))
            s = new_swipe_in.replace(second=0, microsecond=0)
            from_epoch_time = int(s.timestamp())
            to_epoch_time = int(from_epoch_time) + 59


            # current_epoch_time = int(time.time())
            # from_epoch_time = current_epoch_time - 59
            # from_epoch_time = 1689824700
            # current_epoch_time = 1689824759

            print("from_epoch_time -", from_epoch_time)
            print("to_epoch_time -", to_epoch_time)

            conn = http.client.HTTPSConnection("nimblerest.lisuns.com", 4532)
            payload = ''
            headers = {}
            # conn.request("GET",
            #              "/GetHistory/?accessKey=b44bbc1d-f995-416e-9aa4-d8741fc68006&exchange=NFO&instrumentIdentifier=" + pesymbol + "&periodicity=MINUTE&period=1&from=" + str(
            #                  from_epoch_time) + "&to=" + str(to_epoch_time), payload, headers)

            conn.request("GET",
                         "/GetHistory/?accessKey=b44bbc1d-f995-416e-9aa4-d8741fc68006&exchange=NFO&periodicity=MINUTE&period=1&max=2&instrumentIdentifier=" + pesymbol,
                         payload, headers)

            res = conn.getresponse()
            data = res.read()
            # print(data)
            json_object = json.loads(data.decode("utf-8"))

            out_dict = json_object["OHLC"][1]

            to_db_dict["close"] = out_dict.get('CLOSE')
            to_db_dict["high"] = out_dict.get('HIGH')
            to_db_dict["last_trade_time"] = out_dict.get('LASTTRADETIME')
            to_db_dict["low"] = out_dict.get('LOW')
            to_db_dict["open"] = out_dict.get('OPEN')
            to_db_dict["open_interest"] = out_dict.get('OPENINTEREST')
            to_db_dict["quotation_lot"] = out_dict.get('QUOTATIONLOT')
            to_db_dict["traded_qty"] = out_dict.get('TRADEDQTY')
            to_db_dict["epoch"] = to_epoch_time
            to_db_dict["datetime"] = datetime.datetime.fromtimestamp(to_epoch_time).strftime('%d-%m-%Y %H:%M:%S')
            update_db(to_db_dict, "PE_STRIKE_1m_Ticker")
            populate_PE_1m_indicators()
            is_hammer("PE_STRIKE_1m_Ticker")
            update_db_color("PE_STRIKE_1m_Ticker")
            update_db_doji("PE_STRIKE_1m_Ticker")
            # update_db_buy("PE_STRIKE_1m_Ticker")
        except Exception as e:
            print(e)
            continue


def populate_PE_2m_table():
    while (True):
        try:
            time.sleep(119)
            #             time.sleep(5)
            to_db_dict = {}

            swipe_in = datetime.datetime.today()
            new_swipe_in = (swipe_in - timedelta(minutes=2))
            s = new_swipe_in.replace(second=0, microsecond=0)
            from_epoch_time = int(s.timestamp())
            to_epoch_time = int(from_epoch_time) + 119

            # from_epoch_time = 1689824700
            # current_epoch_time = 1689824819
            # current_epoch_time = int(time.time())
            # from_epoch_time = current_epoch_time - 119

            print("from_epoch_time -", from_epoch_time)
            print("to_epoch_time -", to_epoch_time)



            conn = http.client.HTTPSConnection("nimblerest.lisuns.com", 4532)
            payload = ''
            headers = {}
            conn.request("GET",
                         "/GetHistory/?accessKey=b44bbc1d-f995-416e-9aa4-d8741fc68006&exchange=NFO&instrumentIdentifier=" + pesymbol + "&periodicity=MINUTE&period=2&from=" + str(
                             from_epoch_time) + "&to=" + str(to_epoch_time), payload, headers)
            res = conn.getresponse()
            data = res.read()
            json_object = json.loads(data.decode("utf-8"))

            out_dict = json_object["OHLC"][0]

            to_db_dict["close"] = out_dict.get('CLOSE')
            to_db_dict["high"] = out_dict.get('HIGH')
            to_db_dict["last_trade_time"] = out_dict.get('LASTTRADETIME')
            to_db_dict["low"] = out_dict.get('LOW')
            to_db_dict["open"] = out_dict.get('OPEN')
            to_db_dict["open_interest"] = out_dict.get('OPENINTEREST')
            to_db_dict["quotation_lot"] = out_dict.get('QUOTATIONLOT')
            to_db_dict["traded_qty"] = out_dict.get('TRADEDQTY')
            to_db_dict["epoch"] = to_epoch_time
            to_db_dict["datetime"] = datetime.datetime.fromtimestamp(to_epoch_time).strftime('%d-%m-%Y %H:%M:%S')
            update_db(to_db_dict, "PE_STRIKE_2m_Ticker")
            populate_PE_2m_indicators()
            is_hammer("PE_STRIKE_2m_Ticker")
        except Exception as e:
            print(e)
            continue


def populate_PE_3m_table():
    while (True):
        try:
            time.sleep(179)
            #time.sleep(5)
            to_db_dict = {}

            swipe_in = datetime.datetime.today()
            new_swipe_in = (swipe_in - timedelta(minutes=3))
            s = new_swipe_in.replace(second=0, microsecond=0)
            from_epoch_time = int(s.timestamp())
            to_epoch_time = int(from_epoch_time) + 179

            # from_epoch_time = 1689824700
            # current_epoch_time = 1689824879
            # current_epoch_time = int(time.time())
            # from_epoch_time = current_epoch_time - 179

            print("from_epoch_time -", from_epoch_time)
            print("to_epoch_time -", to_epoch_time)



            conn = http.client.HTTPSConnection("nimblerest.lisuns.com", 4532)
            payload = ''
            headers = {}
            conn.request("GET",
                         "/GetHistory/?accessKey=b44bbc1d-f995-416e-9aa4-d8741fc68006&exchange=NFO&instrumentIdentifier=" + pesymbol + "&periodicity=MINUTE&period=3&from=" + str(
                             from_epoch_time) + "&to=" + str(to_epoch_time), payload, headers)
            res = conn.getresponse()
            data = res.read()
            json_object = json.loads(data.decode("utf-8"))

            out_dict = json_object["OHLC"][0]

            to_db_dict["close"] = out_dict.get('CLOSE')
            to_db_dict["high"] = out_dict.get('HIGH')
            to_db_dict["last_trade_time"] = out_dict.get('LASTTRADETIME')
            to_db_dict["low"] = out_dict.get('LOW')
            to_db_dict["open"] = out_dict.get('OPEN')
            to_db_dict["open_interest"] = out_dict.get('OPENINTEREST')
            to_db_dict["quotation_lot"] = out_dict.get('QUOTATIONLOT')
            to_db_dict["traded_qty"] = out_dict.get('TRADEDQTY')
            to_db_dict["epoch"] = to_epoch_time
            to_db_dict["datetime"] = datetime.datetime.fromtimestamp(to_epoch_time).strftime('%d-%m-%Y %H:%M:%S')
            update_db(to_db_dict, "PE_STRIKE_3m_Ticker")
            populate_PE_3m_indicators()
            is_hammer("PE_STRIKE_3m_Ticker")
        except Exception as e:
            print(e)
            continue


def populate_PE_5m_table():
    while (True):
        try:
            time.sleep(299)
            #time.sleep(5)
            to_db_dict = {}

            swipe_in = datetime.datetime.today()
            new_swipe_in = (swipe_in - timedelta(minutes=5))
            s = new_swipe_in.replace(second=0, microsecond=0)
            from_epoch_time = int(s.timestamp())
            to_epoch_time = int(from_epoch_time) + 299

            # from_epoch_time = 1689824700
            # current_epoch_time = 1689824999
            # current_epoch_time = int(time.time())
            # from_epoch_time = current_epoch_time - 299

            print("from_epoch_time -", from_epoch_time)
            print("to_epoch_time -", to_epoch_time)



            conn = http.client.HTTPSConnection("nimblerest.lisuns.com", 4532)
            payload = ''
            headers = {}
            conn.request("GET",
                         "/GetHistory/?accessKey=b44bbc1d-f995-416e-9aa4-d8741fc68006&exchange=NFO&instrumentIdentifier=" + pesymbol + "&periodicity=MINUTE&period=5&from=" + str(
                             from_epoch_time) + "&to=" + str(to_epoch_time), payload, headers)
            res = conn.getresponse()
            data = res.read()
            json_object = json.loads(data.decode("utf-8"))

            out_dict = json_object["OHLC"][0]

            to_db_dict["close"] = out_dict.get('CLOSE')
            to_db_dict["high"] = out_dict.get('HIGH')
            to_db_dict["last_trade_time"] = out_dict.get('LASTTRADETIME')
            to_db_dict["low"] = out_dict.get('LOW')
            to_db_dict["open"] = out_dict.get('OPEN')
            to_db_dict["open_interest"] = out_dict.get('OPENINTEREST')
            to_db_dict["quotation_lot"] = out_dict.get('QUOTATIONLOT')
            to_db_dict["traded_qty"] = out_dict.get('TRADEDQTY')
            to_db_dict["epoch"] = to_epoch_time
            to_db_dict["datetime"] = datetime.datetime.fromtimestamp(to_epoch_time).strftime('%d-%m-%Y %H:%M:%S')
            update_db(to_db_dict, "PE_STRIKE_5m_Ticker")
            populate_PE_5m_indicators()
            is_hammer("PE_STRIKE_5m_Ticker")
        except Exception as e:
            print(e)
            continue


def populate_PE_10m_table():
    while (True):
        try:
            time.sleep(599)
            #time.sleep(5)
            to_db_dict = {}

            swipe_in = datetime.datetime.today()
            new_swipe_in = (swipe_in - timedelta(minutes=10))
            s = new_swipe_in.replace(second=0, microsecond=0)
            from_epoch_time = int(s.timestamp())
            to_epoch_time = int(from_epoch_time) + 599

            # from_epoch_time = 1689824700
            # current_epoch_time = 1689825299
            # current_epoch_time = int(time.time())
            # from_epoch_time = current_epoch_time - 599

            print("from_epoch_time -", from_epoch_time)
            print("to_epoch_time -", to_epoch_time)



            conn = http.client.HTTPSConnection("nimblerest.lisuns.com", 4532)
            payload = ''
            headers = {}
            conn.request("GET",
                         "/GetHistory/?accessKey=b44bbc1d-f995-416e-9aa4-d8741fc68006&exchange=NFO&instrumentIdentifier=" + pesymbol + "&periodicity=MINUTE&period=10&from=" + str(
                             from_epoch_time) + "&to=" + str(to_epoch_time), payload, headers)
            res = conn.getresponse()
            data = res.read()
            json_object = json.loads(data.decode("utf-8"))

            out_dict = json_object["OHLC"][0]

            to_db_dict["close"] = out_dict.get('CLOSE')
            to_db_dict["high"] = out_dict.get('HIGH')
            to_db_dict["last_trade_time"] = out_dict.get('LASTTRADETIME')
            to_db_dict["low"] = out_dict.get('LOW')
            to_db_dict["open"] = out_dict.get('OPEN')
            to_db_dict["open_interest"] = out_dict.get('OPENINTEREST')
            to_db_dict["quotation_lot"] = out_dict.get('QUOTATIONLOT')
            to_db_dict["traded_qty"] = out_dict.get('TRADEDQTY')
            to_db_dict["epoch"] = to_epoch_time
            to_db_dict["datetime"] = datetime.datetime.fromtimestamp(to_epoch_time).strftime('%d-%m-%Y %H:%M:%S')
            update_db(to_db_dict, "PE_STRIKE_10m_Ticker")
            populate_PE_10m_indicators()
            is_hammer("PE_STRIKE_10m_Ticker")
        except Exception as e:
            print(e)
            continue


def populate_PE_1m_indicators():

    conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                          r'Server=' + server + ';'
                          'Database=' + db + ';'
                          'Trusted_Connection=yes;')  # integrated security

    cur = conn.cursor()
    cur.execute('''SELECT * FROM PE_STRIKE_1m_Ticker Order By datetime Asc''')
    myresult = cur.fetchall()

    df = pd.DataFrame((tuple(t) for t in myresult))
    df.columns = ['open', 'close', 'high', 'low', 'last_trade_time', 'open_interest', 'quotation_lot', 'traded_qty',
                  'epoch', 'datetime', 'sma', 'smma', 'adx', 'ema9', 'hammer', 'color', 'doji', 'buy', 'reason']
    convert_dict = {'open': float,
                    'close': float,
                    'high': float,
                    'low': float
                    }

    df = df.astype(convert_dict)
    # print(df.dtypes)
    smma_series = TA.SMMA(df, 7)
    sma_series = TA.SMA(df, 20)
    adx_series = TA.ADX(df, 14)
    ema9_series = TA.EMA(df, 9)
    df = df.assign(smma=smma_series, sma=sma_series, adx=adx_series, ema9=ema9_series)

    # df.ta.ema(13, append=True)
    # df.ta.sma(20, append=True)
    # df.ta.adx(14, append=True)
    # df.ta.ema(9, append=True)
    #
    # print(df)
    # print(df.columns)


    rownum = 0
    for index, rows in df.iterrows():
        sma = df['sma'][rownum]
        smma = df['smma'][rownum]
        adx = df['adx'][rownum]
        ema9 = df['ema9'][rownum]
        epoch = df['epoch'][rownum]
        cur.execute("""update PE_STRIKE_1m_Ticker SET sma = ?, smma = ?, adx = ?, ema9 = ? WHERE epoch = ?""", str(sma),
                    str(smma), str(adx), str(ema9), str(epoch))
        rownum += 1
    conn.commit()
    print("Data Successfully Inserted")
    conn.close()


def populate_PE_2m_indicators():

    conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                          r'Server=' + server + ';'
                          'Database=' + db + ';'
                          'Trusted_Connection=yes;')  # integrated security

    cur = conn.cursor()
    cur.execute('''SELECT * FROM PE_STRIKE_2m_Ticker''')
    myresult = cur.fetchall()

    df = pd.DataFrame((tuple(t) for t in myresult))
    df.columns = ['open', 'close', 'high', 'low', 'last_trade_time', 'open_interest', 'quotation_lot', 'traded_qty',
                  'epoch', 'datetime', 'sma', 'smma', 'adx', 'ema9', 'hammer']
    convert_dict = {'open': float,
                    'close': float,
                    'high': float,
                    'low': float
                    }

    df = df.astype(convert_dict)
    # print(df.dtypes)
    smma_series = TA.SMMA(df, 7)
    sma_series = TA.SMA(df, 20)
    adx_series = TA.ADX(df, 14)
    ema9_series = TA.EMA(df, 9)
    df = df.assign(smma=smma_series, sma=sma_series, adx=adx_series, ema9=ema9_series)

    rownum = 0
    for index, rows in df.iterrows():
        sma = df['sma'][rownum]
        smma = df['smma'][rownum]
        adx = df['adx'][rownum]
        ema9 = df['ema9'][rownum]
        epoch = df['epoch'][rownum]
        cur.execute("""update PE_STRIKE_2m_Ticker SET sma = ?, smma = ?, adx = ?, ema9 = ? WHERE epoch = ?""", str(sma),
                    str(smma), str(adx), str(ema9), str(epoch))
        rownum += 1
    conn.commit()
    print("Data Successfully Inserted")
    conn.close()


def populate_PE_3m_indicators():

    conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                          r'Server=' + server + ';'
                          'Database=' + db + ';'
                          'Trusted_Connection=yes;')  # integrated security

    cur = conn.cursor()
    cur.execute('''SELECT * FROM PE_STRIKE_3m_Ticker''')
    myresult = cur.fetchall()

    df = pd.DataFrame((tuple(t) for t in myresult))
    df.columns = ['open', 'close', 'high', 'low', 'last_trade_time', 'open_interest', 'quotation_lot', 'traded_qty',
                  'epoch', 'datetime', 'sma', 'smma', 'adx', 'ema9', 'hammer']
    convert_dict = {'open': float,
                    'close': float,
                    'high': float,
                    'low': float
                    }

    df = df.astype(convert_dict)
    # print(df.dtypes)
    smma_series = TA.SMMA(df, 7)
    sma_series = TA.SMA(df, 20)
    adx_series = TA.ADX(df, 14)
    ema9_series = TA.EMA(df, 9)
    df = df.assign(smma=smma_series, sma=sma_series, adx=adx_series, ema9=ema9_series)

    rownum = 0
    for index, rows in df.iterrows():
        sma = df['sma'][rownum]
        smma = df['smma'][rownum]
        adx = df['adx'][rownum]
        ema9 = df['ema9'][rownum]
        epoch = df['epoch'][rownum]
        cur.execute("""update PE_STRIKE_3m_Ticker SET sma = ?, smma = ?, adx = ?, ema9 = ? WHERE epoch = ?""", str(sma),
                    str(smma), str(adx), str(ema9), str(epoch))
        rownum += 1
    conn.commit()
    print("Data Successfully Inserted")
    conn.close()


def populate_PE_5m_indicators():

    conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                          r'Server=' + server + ';'
                          'Database=' + db + ';'
                          'Trusted_Connection=yes;')  # integrated security

    cur = conn.cursor()
    cur.execute('''SELECT * FROM PE_STRIKE_5m_Ticker''')
    myresult = cur.fetchall()

    df = pd.DataFrame((tuple(t) for t in myresult))
    df.columns = ['open', 'close', 'high', 'low', 'last_trade_time', 'open_interest', 'quotation_lot', 'traded_qty',
                  'epoch', 'datetime', 'sma', 'smma', 'adx', 'ema9', 'hammer']
    convert_dict = {'open': float,
                    'close': float,
                    'high': float,
                    'low': float
                    }

    df = df.astype(convert_dict)
    # print(df.dtypes)
    smma_series = TA.SMMA(df, 7)
    sma_series = TA.SMA(df, 20)
    adx_series = TA.ADX(df, 14)
    ema9_series = TA.EMA(df, 9)
    df = df.assign(smma=smma_series, sma=sma_series, adx=adx_series, ema9=ema9_series)

    rownum = 0
    for index, rows in df.iterrows():
        sma = df['sma'][rownum]
        smma = df['smma'][rownum]
        adx = df['adx'][rownum]
        ema9 = df['ema9'][rownum]
        epoch = df['epoch'][rownum]
        cur.execute("""update PE_STRIKE_5m_Ticker SET sma = ?, smma = ?, adx = ?, ema9 = ? WHERE epoch = ?""", str(sma),
                    str(smma), str(adx), str(ema9), str(epoch))
        rownum += 1
    conn.commit()
    print("Data Successfully Inserted")
    conn.close()


def populate_PE_10m_indicators():

    conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                          r'Server=' + server + ';'
                          'Database=' + db + ';'
                          'Trusted_Connection=yes;')  # integrated security

    cur = conn.cursor()
    cur.execute('''SELECT * FROM PE_STRIKE_10m_Ticker''')
    myresult = cur.fetchall()

    df = pd.DataFrame((tuple(t) for t in myresult))
    df.columns = ['open', 'close', 'high', 'low', 'last_trade_time', 'open_interest', 'quotation_lot', 'traded_qty',
                  'epoch', 'datetime', 'sma', 'smma', 'adx', 'ema9', 'hammer']
    convert_dict = {'open': float,
                    'close': float,
                    'high': float,
                    'low': float
                    }

    df = df.astype(convert_dict)
    # print(df.dtypes)
    smma_series = TA.SMMA(df, 7)
    sma_series = TA.SMA(df, 20)
    adx_series = TA.ADX(df, 14)
    ema9_series = TA.EMA(df, 9)
    df = df.assign(smma=smma_series, sma=sma_series, adx=adx_series, ema9=ema9_series)

    rownum = 0
    for index, rows in df.iterrows():
        sma = df['sma'][rownum]
        smma = df['smma'][rownum]
        adx = df['adx'][rownum]
        ema9 = df['ema9'][rownum]
        epoch = df['epoch'][rownum]
        cur.execute("""update PE_STRIKE_10m_Ticker SET sma = ?, smma = ?, adx = ?, ema9 = ? WHERE epoch = ?""", str(sma),
                    str(smma), str(adx), str(ema9), str(epoch))
        rownum += 1
    conn.commit()
    print("Data Successfully Inserted")
    conn.close()





def is_hammer(table):
    conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                          r'Server=' + server + ';'
                          'Database=' + db + ';'
                          'Trusted_Connection=yes;')  # integrated security

    cur = conn.cursor()
    cur.execute("SELECT * FROM " + table + " Order By datetime Desc")
    full_row = cur.fetchall()

    open = float(full_row[0][0])
    close = float(full_row[0][1])
    high = float(full_row[0][2])
    low = float(full_row[0][3])
    sma = float(full_row[0][10])
    smma = float(full_row[0][11])
    adx = float(full_row[0][12])
    ema9 = float(full_row[0][13])

    epoch = int(full_row[0][8])

    upper_shadow = high - max(open, close)

    # if (float(upper_shadow) <= 1.5) and (float(low) < float(open)) and (
    #         float(close) > float(open)) and (
    #         float(close) > float(smma) and (
    #         float(high) - float(low) <= 12)):
    if (upper_shadow <= 1.5) and (low < open) and (close > open) and (close > smma) and (high - low <= 12) and (low - smma <= 4):
        cur.execute("update " + table + " SET hammer = 'Y' where epoch = '" + str(epoch) + "'")
        conn.commit()
        print("Hammer Successfully Inserted - Y")
        conn.close()
    else:
        cur.execute("update " + table + " SET hammer = 'N' where epoch = '" + str(epoch) + "'")
        conn.commit()
        print("Hammer Successfully Inserted - N")
        conn.close()


def update_db(dict_to_db, table):

    close = str(dict_to_db.get('close'))
    high = str(dict_to_db.get('high'))
    last_trade_time = str(dict_to_db.get('last_trade_time'))
    low = str(dict_to_db.get('low'))
    open_price = str(dict_to_db.get('open'))
    open_interest = str(dict_to_db.get('open_interest'))
    quotation_lot = str(dict_to_db.get('quotation_lot'))
    traded_qty = str(dict_to_db.get('traded_qty'))
    epoch = str(dict_to_db.get('epoch'))
    datetime = str(dict_to_db.get('datetime'))

    # conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
    #                       r'Server=localhost\MSSQLSERVER01;'
    #                       'Database=algotrade;'
    #                       'Trusted_Connection=yes;')  # integrated security

    conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                          r'Server=' + server + ';'
                          'Database=' + db + ';'
                          'Trusted_Connection=yes;')  # integrated security

    cursor = conn.cursor()

    SQLCommand = (
            "INSERT INTO " + table + " ([close], [high], last_traded_time, [low], [open], open_interest, quotation_lot, traded_qty, epoch, datetime) VALUES (?,?,?,?,?,?,?,?,?,?);")
    Values = [close, high, last_trade_time, low, open_price, open_interest, quotation_lot, traded_qty, epoch, datetime]

    #     print(SQLCommand)
    # Processing Query
    cursor.execute(SQLCommand, Values)

    conn.commit()
    print("Data Successfully Inserted")
    conn.close()



def update_db_color(table):
    conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                          r'Server=' + server + ';'
                          'Database=' + db + ';'
                          'Trusted_Connection=yes;')  # integrated security

    cur = conn.cursor()
    cur.execute("SELECT * FROM " + table + " Order By datetime Desc")
    full_row = cur.fetchall()

    open = float(full_row[0][0])
    close = float(full_row[0][1])
    epoch = int(full_row[0][8])

    # high = float(full_row[0][2])
    # low = float(full_row[0][3])
    # sma = float(full_row[0][10])
    # smma = float(full_row[0][11])
    # adx = float(full_row[0][12])
    # ema9 = float(full_row[0][13])



    # upper_shadow = high - max(open, close)


    if (close > open):
        cur.execute("update " + table + " SET color = 'G' where epoch = '" + str(epoch) + "'")
        conn.commit()
        print("Color update - G")
        conn.close()
    else:
        cur.execute("update " + table + " SET color = 'R' where epoch = '" + str(epoch) + "'")
        conn.commit()
        print("Color update - R")
        conn.close()


def update_db_doji(table):
    conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                          r'Server=' + server + ';'
                          'Database=' + db + ';'
                          'Trusted_Connection=yes;')  # integrated security

    cur = conn.cursor()
    cur.execute("SELECT * FROM " + table + " Order By datetime Desc")
    full_row = cur.fetchall()

    open = float(full_row[0][0])
    close = float(full_row[0][1])
    epoch = int(full_row[0][8])

    diff_open_close = abs(open - close)

    # high = float(full_row[0][2])
    # low = float(full_row[0][3])
    # sma = float(full_row[0][10])
    # smma = float(full_row[0][11])
    # adx = float(full_row[0][12])
    # ema9 = float(full_row[0][13])



    # upper_shadow = high - max(open, close)


    if (diff_open_close <= 0.05):
        cur.execute("update " + table + " SET doji = 'Y' where epoch = '" + str(epoch) + "'")
        conn.commit()
        print("Doji update - Y")
        conn.close()
    else:
        cur.execute("update " + table + " SET doji = 'N' where epoch = '" + str(epoch) + "'")
        conn.commit()
        print("Doji update - N")
        conn.close()


def update_db_buy(table):
    conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                          r'Server=' + server + ';'
                          'Database=' + db + ';'
                          'Trusted_Connection=yes;')  # integrated security

    cur = conn.cursor()
    cur.execute("SELECT * FROM " + table + " Order By datetime Desc")
    full_row = cur.fetchall()

    epoch = int(full_row[0][8])
    hammer = full_row[0][14]
    color = full_row[0][15]
    doji = full_row[0][16]

    # high = float(full_row[0][2])
    # low = float(full_row[0][3])
    # sma = float(full_row[0][10])
    # smma = float(full_row[0][11])
    # adx = float(full_row[0][12])
    # ema9 = float(full_row[0][13])



    # upper_shadow = high - max(open, close)

    if hammer == "Y" and color == "G" and doji == "N":
        cur.execute("update " + table + " SET buy = 'Y' where epoch = '" + str(epoch) + "'")
        conn.commit()
        print("Color update - Y")
        conn.close()
    else:
        cur.execute("update " + table + " SET buy = 'N' where epoch = '" + str(epoch) + "'")
        conn.commit()
        print("Color update - N")
        conn.close()





if __name__ == '__main__':
    Thread(target=populate_nifty_1m_table).start()
    # Thread(target=populate_nifty_2m_table).start()
    # Thread(target=populate_nifty_3m_table).start()
    # Thread(target=populate_nifty_5m_table).start()
    # Thread(target=populate_nifty_10m_table).start()
    #
    Thread(target=populate_CE_1m_table).start()
    # Thread(target=populate_CE_2m_table).start()
    # Thread(target=populate_CE_3m_table).start()
    # Thread(target=populate_CE_5m_table).start()
    # Thread(target=populate_CE_10m_table).start()

    Thread(target=populate_PE_1m_table).start()
    # Thread(target=populate_PE_2m_table).start()
    # Thread(target=populate_PE_3m_table).start()
    # Thread(target=populate_PE_5m_table).start()
    # Thread(target=populate_PE_10m_table).start()
