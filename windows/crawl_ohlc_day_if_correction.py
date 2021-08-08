from jazzstock_crawl.windows.kiwoom import Kiwoom as kapi, dateManager as dm, apiManager as am
from jazzstock_crawl.wrapper import _check_running_time
from jazzstock_bot.common import connector_db as db
import time
import pandas as pd
import warnings

warnings.filterwarnings("ignore")

itemDic, codeDic = {}, {}
apiObj = None
today, dateA, dateB = None, None, None


@_check_running_time
def get_connection():
    global apiObj
    apiObj = kapi.Kiwoom()
    apiObj.comm_connect()


@_check_running_time
def drop_connection():
    apiObj.destroy()


@_check_running_time
def get_today():
    global dateA, dateB, today
    dateA, dateB = am.api_checkDate(apiObj, dm.todayStr('n'))
    today = dateA


# @_check_running_time
def get_ohlc_min(stockcode, today):
    ret = am.api_get_ohlc_min_test(apiObj, stockcode)
    if isinstance(ret, pd.DataFrame):
        ret = ret[ret['DATE'] == today]
        ret['STOCKCODE'] = stockcode
        return ret[['STOCKCODE', 'DATE', 'TIME', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOLUME']]

    else:
        return None

def get_ohlc_min(stockcode, today):
    ret = am.api_get_ohlc_(apiObj, stockcode)
    if isinstance(ret, pd.DataFrame):
        ret = ret[ret['DATE'] == today]
        ret['STOCKCODE'] = stockcode
        return ret[['STOCKCODE', 'DATE', 'TIME', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOLUME']]

    else:
        return None


if __name__ == "__main__":

    get_ohlc_day("079940", "2021-08-06")
