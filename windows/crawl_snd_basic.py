from jazzstock_crawl.windows.kiwoom import Kiwoom as kapi, dateManager as dm, apiManager as am
from jazzstock_crawl.wrapper import _check_running_time
from jazzstock_bot.common import connector_db as db
import sys
import time
from datetime import datetime
import warnings
warnings.filterwarnings("ignore")

itemDic, codeDic = {},{}
apiObj = None
dateA, dateB = None, None

@_check_running_time
def get_stockcode_to_update(dt):
    # DB에서 [종목명,종목코드] 로 구성된 데이터셋을 받아옴.
    # dbUpdateDate = db.selectSingleValue('SELECT max(date) FROM test.t_stock_shares_info')

    query = """

                        SELECT A.STOCKCODE, A.STOCKNAME
                        FROM jazzdb.T_STOCK_CODE_MGMT A
                        WHERE 1=1
                        AND A.STOCKCODE NOT IN (

                            SELECT STOCKCODE
                            FROM jazzdb.T_STOCK_SND_DAY
                            WHERE DATE = '%s'
                            GROUP BY STOCKCODE
                        )
                        AND A.LISTED = 1
                        
                        
                        
                        
                                                        """ % (dt)

    for eachRow in db.select(query):
        if (len(eachRow) > 0):
            itemDic[eachRow[1].upper()] = eachRow[0]
            codeDic[eachRow[0]] = eachRow[1].upper()

    print("[INFO] 종목명/종목코드를 메모리에 읽어왔습니다, 남은 종목 수: ", len(itemDic.keys()), dt)

    return len(itemDic.keys())



@_check_running_time
def get_connection():

    global apiObj
    apiObj = kapi.Kiwoom()
    apiObj.comm_connect()

@_check_running_time
def get_today():

    global dateA, dateB

    dateA, dateB = am.api_checkDate(apiObj, dm.todayStr('n'))
    if (len(sys.argv) > 1):
        dateA = sys.argv[1]
        print("  passed argv : ", dateA) # 과거 수집용
    else:
        print("  passed argv : None") # 일 배치


@_check_running_time
def insert_snd(db=False):
    start = datetime.now()
    for i,eachCode in enumerate(list(codeDic.keys())[:min([len(codeDic),98])]):
        try:


            am.api_getSndDB(apiObj, eachCode, dateA)
            time.sleep(0.2)
            print(i)
            #
            # if (i  !=
            #     print("[INFO]:", i, eachCode, codeDic[eachCode], "inserted", datetime.now() - start)
            #     #print("[INFO]: wait for 30 second")
            #     #time.sleep()

        except Exception as e:
            print(eachCode, e)
            time.sleep(0.2)




@_check_running_time
def drop_connection():
    apiObj.destroy()


if __name__=='__main__':
    get_connection()
    get_today()
    get_stockcode_to_update(dateA)
    insert_snd()
    left = get_stockcode_to_update(dateA)
    drop_connection()
