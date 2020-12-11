from jazzstock_crawl.windows.kiwoom import Kiwoom as kapi, dateManager as dm, apiManager as am
from jazzstock_crawl.wrapper import _check_running_time
from jazzstock_bot.common import connector_db as db
import time
import pandas  as pd
import warnings
warnings.filterwarnings("ignore")


itemDic, codeDic = {},{}
apiObj = None
today, dateA, dateB = None, None, None

def get_stockcode_to_update(dt):
    # DB에서 [종목명,종목코드] 로 구성된 데이터셋을 받아옴.
    # dbUpdateDate = db.selectSingleValue('SELECT max(date) FROM test.t_stock_shares_info')


    query = '''
    
    SELECT STOCKCODE
    FROM
    (
        SELECT STOCKCODE, ROW_NUMBER() OVER (ORDER BY STOCKCODE DESC) AS RN
        FROM jazzdb.T_STOCK_CODE_MGMT A
        WHERE 1=1
        AND A.STOCKCODE IN (
    
            SELECT STOCKCODE
            FROM jazzdb.T_STOCK_OHLC_MIN
            WHERE DATE = '%s'
            GROUP BY STOCKCODE
        )
        AND A.LISTED = 1
    ) A
    
    WHERE RN =3
    
    
    ''' %(dt)



    m = db.selectSingleValue(query)
    print(m)

    if m is None:
        m = '000000'

    query = """

                        SELECT A.STOCKCODE, A.STOCKNAME
                        FROM jazzdb.T_STOCK_CODE_MGMT A
                        WHERE 1=1
                        AND A.LISTED = 1
                        AND A.STOCKCODE > '%s'
                                                        """ % (m)



    for eachRow in db.select(query):
        if (len(eachRow) > 0):
            itemDic[eachRow[1].upper()] = eachRow[0]
            codeDic[eachRow[0]] = eachRow[1].upper()

    print("[INFO] 종목명/종목코드를 메모리에 읽어왔습니다, 남은 종목 수: ", len(itemDic.keys()))


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


    ret = am.api_get_ohlc_min_test(apiObj,stockcode)
    if isinstance(ret, pd.DataFrame):
        print(today)
        ret = ret[ret['DATE']==today]
        ret['STOCKCODE']=stockcode
        return ret[['STOCKCODE','DATE','TIME','OPEN','HIGH', 'LOW', 'CLOSE', 'VOLUME']]

    else:
        return None


# @_check_running_time
def insert_dataframe(df):
    db.insertdf(df, 'jazzdb.T_STOCK_OHLC_MIN')

@_check_running_time
def loop():
    for eachcode in list(codeDic.keys())[:98]:

        ret = db.selectSingleValue('SELECT COUNT(*) FROM jazzdb.T_STOCK_OHLC_MIN WHERE STOCKCODE = "%s" AND DATE = "%s"'%(eachcode, today))

        # print(eachcode, ret)
        if ret == 0:

            rtdf = get_ohlc_min(eachcode, today)
            # print(rtdf)
            if isinstance(rtdf, pd.DataFrame) and len(rtdf)>0:
                insert_dataframe(rtdf)
        time.sleep(0.22)



if __name__=='__main__':

    get_connection()
    get_today()
    get_stockcode_to_update(today)
    loop()

    # CHECK IF API RUN
    # df = get_ohlc_min('079940')
    # print(df)
    drop_connection()