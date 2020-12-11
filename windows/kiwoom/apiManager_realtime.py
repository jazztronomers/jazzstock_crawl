import manager.dateManager as dp
import manager.dbConnector as db
import manager.Kiwoom_realtime as kapi
from datetime import datetime
import time


'''
          [SetInputValue() 함수]
          
          SetInputValue(
          BSTR sID,     // TR에 명시된 Input이름
          BSTR sValue   // Input이름으로 지정한 값
          )
          
          조회요청시 TR의 Input값을 지정하는 함수이며 
          조회 TR 입력값이 많은 경우 이 함수를 반복적으로 호출합니다.
'''



def api_getMinChart_lastrow(apiObj, stockCode):
    apiObj.set_input_value("종목코드", stockCode)
    apiObj.set_input_value("틱범위", 5)
    apiObj.set_input_value("수정주가구분", 1)
    apiObj.set_input_value("기준일자", datetime.today())
    rt = apiObj.comm_rq_data("opt10080_req_r", "opt10080", 0, "0101")
    return rt


def api_getMinChart_wholeday(apiObj, stockCode):
    apiObj.set_input_value("종목코드", stockCode)
    apiObj.set_input_value("틱범위", 5)
    apiObj.set_input_value("수정주가구분", 1)
    apiObj.set_input_value("기준일자", datetime.today())
    rt = apiObj.comm_rq_data("opt10080_req_d", "opt10080", 0, "0101")
    return rt

'''

          [SendOrder() 함수]
          
          SendOrder(
          BSTR sRQName, // 사용자 구분명
          BSTR sScreenNo, // 화면번호
          BSTR sAccNo,  // 계좌번호 10자리
          LONG nOrderType,  // 주문유형 1:신규매수, 2:신규매도 3:매수취소, 4:매도취소, 5:매수정정, 6:매도정정
          BSTR sCode, // 종목코드
          LONG nQty,  // 주문수량
          LONG nPrice, // 주문가격
          BSTR sHogaGb,   // 거래구분(혹은 호가구분)은 아래 참고
          BSTR sOrgOrderNo  // 원주문번호입니다. 신규주문에는 공백, 정정(취소)주문할 원주문번호를 입력합니다.
          )
          
          9개 인자값을 가진 국내 주식주문 함수이며 리턴값이 0이면 성공이며 나머지는 에러입니다.
          1초에 5회만 주문가능하며 그 이상 주문요청하면 에러 -308을 리턴합니다.
          
          [거래구분]
          모의투자에서는 지정가 주문과 시장가 주문만 가능합니다.
          
          00 : 지정가   # 그냥 내가 사고 싶은 가격
          03 : 시장가   # 가격  상관없이 무조건 매수, 반대잔량 긁기
          05 : 조건부지정가 # 정규시장까지는 지정가, 장종료때는 ㅅ ㅣ장가
          06 : 최유리지정가 # 주문 점수시점에 상대방향매매의 최우선호가와 동일한 가격으로 주문
          07 : 최우선지정가 # 주문 접수시점에 동일방향매매의 최우선호가와 동일한 가격으로 주문
          
          IOC : Immediate or cancel 주문즉시 체결 및 잔량 전부취소 조건, 호가접수시점에 체결할 수 있는 물량을 모두 체결하여
          FOK : Fill Or Kill 
          
          
          10 : 지정가IOC 
          13 : 시장가IOC
          16 : 최유리IOC
          20 : 지정가FOK
          23 : 시장가FOK
          26 : 최유리FOK
          61 : 장전시간외종가
          62 : 시간외단일가매매
          81 : 장후시간외종가

'''
def api_sendorder(apiObj, stockcode, action, close, vol, debug=False):


    s_rqname = 'gyb0418'
    s_screenno ='0101'
    s_accno = '5288885510'

    if(action == 'S'):
        s_ordertype=  2
    else:
        s_ordertype = 1

    s_code=stockcode
    s_quantity =int(vol)
    s_price=int(close)

    s_hoga='00'
    s_orderno=''

    # def send_order(self, rqname, screen_no, acc_no, order_type, code, quantity, price, hoga, order_no):
    status = apiObj.send_order(s_rqname, s_screenno, s_accno, s_ordertype, s_code, s_quantity, s_price, s_hoga, s_orderno)


    q = '''
    INSERT INTO `jazzdb`.`T_TRADING_HISTORY_TICK` (`STOCKCODE`, `DATE`, `TIME`, `ACTION`, `PRICE`, `AMOUNT`, `DONE`, `ORDERTYPE`) VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s');
    ''' %(s_code, datetime.now().date(),datetime.now().time(), s_ordertype, s_price, s_quantity, '0', '0')

    print(q)

    db.insert(q)

    return status



def api_checkhoga(apiObj,stockcode):

    pass

def api_account(apiObj, acc):


    apiObj.set_input_value("계좌번호",acc);
    apiObj.set_input_value("비밀번호입력매체구분"	,  "00");
    apiObj.set_input_value("조회구분"	,  "2");
    rt = apiObj.comm_rq_data("opw00018_req", "opw00018", 0, "0366")
    return rt



# CHECK ACCOUNT
if(__name__ == '__main__'):


    apiObj = kapi.Kiwoom()
    apiObj.comm_connect()


    # st = datetime.now()
    # a = api_getMinChart_wholeday(apiObj, '079940')
    # print(a)
    # print(datetime.now()-st)

    for eachacc in ["5288885510","5113098910","5107941510"]:

        account_dic = api_account(apiObj, eachacc)
        for eachstockcode in account_dic:
            print(eachacc, eachstockcode, account_dic[eachstockcode])

    # resp = api_sendorder(apiObj,'223310', 'B', 200, 1, True)
    # time.sleep(0.2)
    #
    # resp = api_sendorder(apiObj, '223310', 'B', 200, 1, True)
    # time.sleep(0.2)
    #
    #
    # # account_dic = api_account(apiObj)
    # # print(account_dic)
    # # time.sleep(1)
    # #
    # #
    # #
    # # account_dic = api_account(apiObj)
    # # print(account_dic)
    # # time.sleep(1)
    #
    # resp = api_sendorder(apiObj, '223310', 'B', 200, 1)
    # time.sleep(0.2)

