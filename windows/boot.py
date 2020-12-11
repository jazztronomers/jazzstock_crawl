from jazzstock_crawl.windows.kiwoom import Kiwoom as kapi, dateManager as dm
from jazzstock_crawl.wrapper import _check_running_time
from jazzstock_bot.common import connector_db as db


apiObj = None
codeDic ={}
old_list=None
new_list=None



@_check_running_time
def connection():

    global apiObj
    apiObj = kapi.Kiwoom()
    apiObj.comm_connect()

@_check_running_time
def get_original_stockcode():


    global codeDic, old_list
    old_list = db.select('select stockcode, stockname from jazzdb.T_STOCK_CODE_MGMT')
    # jazzdb.t_stock_code_mgmt 테이블
    # 신규상장종목 및 종목명 변경여부 확인,업데이트하는 소스
    # 데이터 출처 : Kiwoom

    for each in old_list:
        codeDic[each[0]] = each[1]


@_check_running_time
def update_stockcode():


    global codeDic, old_list
    new_list = apiObj.get_list('p')
    new_list = new_list + apiObj.get_list('d')

    for eachcode in new_list:
        # 신규 상장 종목 인서트
        if eachcode[0] != '' and eachcode[0] not in codeDic.keys():
            print(eachcode+[dm.todayStr('-')])
            db.insert("INSERT INTO `jazzdb`.`T_STOCK_CODE_MGMT` (`STOCKCODE`, `STOCKNAME`, `MARKET`, `LISTED`, `UPDATEDATE`) VALUES ('%s', '%s', '%s', '%s','%s');" %tuple(eachcode+[dm.todayStr('-')]))
            print("신규상장 종목", eachcode)

        # 종목명 바뀐경우 업데이트
        if eachcode[0] != '' and eachcode[0] in codeDic.keys() and eachcode[1] != codeDic[eachcode[0]]:
            print("종목명 변경", codeDic[eachcode[0]],eachcode)
            db.insert("UPDATE `jazzdb`.`T_STOCK_CODE_MGMT` SET `STOCKNAME` = '%s', `UPDATEDATE` = '%s' WHERE (`STOCKCODE` = '%s')" %(eachcode[1],dm.todayStr('-'),eachcode[0]))

@_check_running_time
def drop_connection():
    apiObj.destroy()

if __name__=='__main__':

    connection()
    get_original_stockcode()
    update_stockcode()
    drop_connection()
