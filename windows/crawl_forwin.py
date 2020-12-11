from jazzstock_crawl.windows.kiwoom import Kiwoom as kapi, dateManager as dm, apiManager as am
from jazzstock_crawl.wrapper import _check_running_time
from jazzstock_bot.common import connector_db as db
import time
import sys

def db_readAll(dt,winCode):
    # DB에서 [종목명,종목코드] 로 구성된 데이터셋을 받아옴.
    # dbUpdateDate = db.selectSingleValue('SELECT max(date) FROM test.t_stock_shares_info')

    query = """

                        SELECT A.STOCKCODE, A.STOCKNAME
                        FROM jazzdb.T_STOCK_CODE_MGMT A
                        WHERE 1=1
                        AND A.STOCKCODE NOT IN (

                            SELECT STOCKCODE
                            FROM jazzdb.T_STOCK_SND_WINDOW_ISOLATED
                            WHERE DATE = '%s'
                            AND WINCODE = '%s'
                            GROUP BY STOCKCODE
                        )
                        AND A.LISTED = 1
                                                        """ % (dt,winCode)


    for eachRow in db.select(query):
        if (len(eachRow) > 0):
            itemDic[eachRow[1].upper()] = eachRow[0]
            codeDic[eachRow[0]] = eachRow[1].upper()

    print("[INFO] 종목명/종목코드를 메모리에 읽어왔습니다, 남은 종목 수: ", len(itemDic.keys()))
    print("[INFO] 종목명/종목코드를 메모리에 읽어왔습니다, 남은 종목 수: ", len(itemDic.keys()))


def drop_connection():
    apiObj.destroy()


itemDic = {}
codeDic = {}

apiObj = kapi.Kiwoom()
apiObj.comm_connect()

dateA, dateB = am.api_checkDate(apiObj, dm.todayStr('n'))
#target_dt = dateA[:4] + '-' + dateA[4:6] + '-' + dateA[6:]


if len(sys.argv)>1:
    wincode = sys.argv[1]
else:
    wincode = '58'




db_readAll(dateA, wincode)

# for winCode in ['61', '58', '43', '35', '41']:
#     processrunner_forwin('07.crawl_forwin.py',28,winCode)
#
# for winCode in ['36','42', '44', '45', '54']:
#     processrunner_forwin('07.crawl_forwin.py',28,winCode)
#
# for winCode in [ '33', '06', '03', '37']:
#     processrunner_forwin('07.crawl_forwin.py', 28, winCode)

itr = 0
start = time.time()
for itr,eachCode in enumerate(list(codeDic.keys())[:min([len(codeDic),99])]):

    try:
        inserted_data_size = am.api_getSndForWin(apiObj, eachCode, dateA, wincode)
        time.sleep(0.25)
        print("[INFO]:", itr, wincode, eachCode, codeDic[eachCode], inserted_data_size, "rows inserted",
              time.time() - start)

    except Exception as e:
        print('error! :', eachCode, e)
        itr += 1
        time.sleep(0.2)

drop_connection()




