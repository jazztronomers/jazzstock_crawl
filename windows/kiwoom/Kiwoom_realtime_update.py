import sys
from PyQt5.QtWidgets import *
from PyQt5.QAxContainer import *
from PyQt5.QtCore import *
import time
import os
import re
import pandas as pd
import manager.dbConnector as db
from datetime import datetime

idx = 0


class Kiwoom(QAxWidget):
    RETURN = ''

    def __init__(self):
        super().__init__()
        self._create_kiwoom_instance()
        self._set_signal_slots()
        self.orderLoop = None

    def _create_kiwoom_instance(self):
        self.setControl("KHOPENAPI.KHOpenAPICtrl.1")

    def _set_signal_slots(self):

        self.OnEventConnect.connect(self._event_connect)
        self.OnReceiveTrData.connect(self._receive_tr_data)
        self.OnReceiveChejanData.connect(self._receive_chejan_data)  # 매수체결후 호출됨
        self.OnReceiveMsg.connect(self.receive_msg)  # 매수시도후 에러발생시

    def parsenumber(self, n):

        flag = False
        rt = ''

        if n[0] == '0':
            rt = ''
        else:
            rt = '-'
        for each in range(1, len(n)):
            if (n[each] == '0'):
                continue
            else:
                rt = rt + n[each:]
                break

        return float(rt)

    def comm_connect(self):
        self.dynamicCall("CommConnect()")
        self.login_event_loop = QEventLoop()
        self.login_event_loop.exec_()

    def _event_connect(self, err_code):
        if err_code == 0:
            print("[INFO] 키움증권 manager 접속완료...")
        else:
            print("[INFO] 키움증권 manager 접속에러 발생...")

        self.login_event_loop.exit()

    def set_input_value(self, id, value):
        self.dynamicCall("SetInputValue(QString, QString)", id, value)

    def comm_rq_data(self, rqname, trcode, next, screen_no):

        try:
            global RETURN
            self.dynamicCall("CommRqData(QString, QString, int, QString", rqname, trcode, next, screen_no)
            self.tr_event_loop = QEventLoop()
            self.tr_event_loop.exec_()
            return RETURN
        except:
            print("exception")

    # REAL-TIME PART ------------------------------------------------------------

    def _set_real_reg(self, screen_no, code_list, fid_list, real_type):

        self.dynamicCall("SetRealReg(QString,QString,QString,QString)", screen_no, code_list, fid_list, real_type)
        self.real_event_loop = QEventLoop()
        self.real_event_loop.exec_()

    def _get_comm_real_data(self, rcode, fid_list):
        ret = self.dynamicCall("GetCommRealData(QString, QString", rcode, fid_list)
        return ret.strip()

    def checktime(self):
        current_time = QTime.currentTime()
        text_time = current_time.toString("hh:mm:ss").split(':')
        return text_time

    # REAL-TIME PART ------------------------------------------------------------

    # BUY & SELL ----------------------------------------------------------------

    def get_login_info(self, tag):
        ret = self.dynamicCall("GetLoginInfo(QString)", tag)
        return ret

    def send_order(self, rqname, screen_no, acc_no, order_type, code, quantity, price, hoga, order_no):

        order_response = self.dynamicCall(
            "SendOrder(QString, QString, QString, int, QString, int, int, QString, QString)",
            [rqname, screen_no, acc_no, order_type, code, int(quantity), int(price), hoga, order_no])

        try:

            self.orderLoop = QEventLoop()
            self.orderLoop.exec_()

            return order_response

        except:

            print('error')

            return 'error'

    def get_chejan_data(self, fid):
        ret = self.dynamicCall("GetChejanData(int)", fid)
        return ret

    def _receive_chejan_data(self, gubun, item_cnt, fid_list):

        '''
        FID	설명
        https://download.kiwoom.com/web/openapi/kiwoom_openapi_plus_devguide_ver_1.1.pdf

        @ 9201 계좌번호
        @ 9001 종목코드


        @ 9203 주문번호
        @ 900	주문수량
        @ 901	주문가격
        @ 904	원주문번호
        @ 908	주문/체결시간 HHMMSSMS

        @ 909	체결번호
        @ 910	체결가
        @ 911	체결량
        '''

        # 주문메타
        acc = self.get_chejan_data(9201).strip()
        stockcode = self.get_chejan_data(9001).strip().replace('A','')
        o_time = self.get_chejan_data(908)  # 115002
        o_date = datetime.now().date()

        # 주문정보
        o_no = self.get_chejan_data(9203)
        o_type_detail = self.get_chejan_data(905)  # +매수 / +매수정정 / -매도/ -매도정정
        o_type = self.get_chejan_data(907)  # 1 매도 / 2 매수
        o_state = self.get_chejan_data(913)  # 접수 / 확인 / 체결

        o_amount = self.get_chejan_data(900)
        o_price = self.get_chejan_data(901)
        o_origin = self.get_chejan_data(904)
        o_datetime = self.get_chejan_data(908)  # 115002

        # 체결정보
        c_no = self.get_chejan_data(909)
        c_price = self.get_chejan_data(910)
        c_amount = self.get_chejan_data(911)

        # '''
        # CREATE TABLE `jazztrade`.`T_TRADE_ORDER` (
        #               `ACC` VARCHAR(11) NOT NULL,
        #               `DATE` DATE NOT NULL,
        #               `TIME` TIME NOT NULL,
        #               `STOCKCODE` VARCHAR(11) NOT NULL,
        #               `O_NO` VARCHAR(11) NOT NULL,
        #               `O_STATE` VARCHAR(1) NOT NULL,
        #               `O_TYPE` VARCHAR(11) NOT NULL,
        #               `O_TYPE_DET` VARCHAR(11) NOT NULL,
        #               `O_PRICE` INT NOT NULL,
        #               `O_AMOUNT` INT NOT NULL,
        #               `O_ORIGIN` VARCHAR(11) NOT NULL,
        #               `C_NO` VARCHAR(11) NULL,
        #               `C_PRICE` INT NULL,
        #               `C_AMOUNT` INT NULL,
        #               PRIMARY KEY (`ACC`, `DATE`, `TIME`, `STOCKCODE`, `O_NO`, `O_TYPE`));
        #
        # '''

        # 매수 / 매도 주문

        global idx

        if o_state == '접수':

            q = '''
            INSERT INTO `jazztrade`.`T_TRADE_ORDER_PROD` 
            (`ACC`, `DATE`, `TIME`, `STOCKCODE`, `O_NO`, `O_STATE`, `O_TYPE`, `O_TYPE_DET`, `O_PRICE`, `O_AMOUNT`, `O_ORIGIN`, `T_DEBUG`) 
            VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', %s, %s, '%s', %s);
            ''' % (
            acc, o_date, o_time, stockcode, o_no, o_state, o_type, o_type_detail, o_price, o_amount, o_origin, idx)

            print(o_state, q)
            db.insert(q)
            idx += 1


        elif o_state == '체결':

            # 매수 / 매도 체결
            q = '''
            INSERT INTO `jazztrade`.`T_TRADE_ORDER_PROD` 
            (`ACC`, `DATE`, `TIME`, `STOCKCODE`, `O_NO`, `O_STATE`, `O_TYPE`, `O_TYPE_DET`, `O_PRICE`, `O_AMOUNT`, `O_ORIGIN`, `C_NO`, `C_PRICE`, `C_AMOUNT`, `T_DEBUG`) 
            VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', %s, %s, '%s', '%s', %s, %s, %s);
            ''' % (
            acc, o_date, o_time, stockcode, o_no, o_state, o_type, o_type_detail, o_price, o_amount, o_origin, c_no,
            c_price, c_amount, idx)

            print(o_state, q)
            idx += 1
            db.insert(q)


        elif o_state == '확인':

            # 매수 / 매도 정정
            q = '''
            INSERT INTO `jazztrade`.`T_TRADE_ORDER_PROD` 
            (`ACC`, `DATE`, `TIME`, `STOCKCODE`, `O_NO`, `O_STATE`, `O_TYPE`, `O_TYPE_DET`, `O_PRICE`, `O_AMOUNT`, `O_ORIGIN`, `T_DEBUG`) 
            VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', %s, %s, '%s', %s);
            ''' % (
            acc, o_date, o_time, stockcode, o_no, o_state, o_type, o_type_detail, o_price, o_amount, o_origin, idx)
            print(o_state, q)
            idx += 1
            db.insert(q)

    # 매수후메세지리턴(성공이든 실패든 호출한다)
    def receive_msg(self, scr_no, rq_name, tr_code, msg):
        print(
            "[INFO] receive_msg 호출됨, scr_no = " + scr_no + ", rq_name = " + rq_name + ", tr_code = " + tr_code + ", msg = " + msg)
        try:
            self.orderLoop.exit()
            # print('\t[ORDER] order loop exit')
        except AttributeError:
            pass

    # BUY & SELL ----------------------------------------------------------------

    def _comm_get_data(self, code, real_type, field_name, index, item_name):
        ret = self.dynamicCall("CommGetData(QString, QString, QString, int, QString", code,
                               real_type, field_name, index, item_name)
        return ret.strip()

    def _get_repeat_cnt(self, trcode, rqname):
        ret = self.dynamicCall("GetRepeatCnt(QString, QString)", trcode, rqname)
        return ret

    def _receive_tr_data(self, screen_no, rqname, trcode, record_name, next, unused1, unused2, unused3, unused4):

        # print('** RECEIVE_TR_DATA :', screen_no, rqname, trcode, record_name, next, unused1, unused2, unused3, unused4)

        global RETURN
        if next == '2':
            self.remained_data = True
        else:
            self.remained_data = False

        if rqname == 'opt10080_req_r':
            rt = self._opt10080_row(rqname, trcode)

            try:
                self.tr_event_loop.exit()
                RETURN = rt
                return rt
            except AttributeError:
                pass

        elif rqname == 'opt10080_req_d':
            rt = self._opt10080_day(rqname, trcode)

            try:
                self.tr_event_loop.exit()
                RETURN = rt
                return rt
            except AttributeError:
                pass

        elif rqname == 'opw00018_req':
            rt = self._opw00018(rqname, trcode)

            try:
                self.tr_event_loop.exit()
                RETURN = rt
                return rt
            except AttributeError:
                pass

    # 분봉 10일치밖에 안줌
    def _opt10080_row(self, rqname, trcode):
        data_cnt = self._get_repeat_cnt(trcode, rqname)
        asint = ['OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOLUME']
        dic = {
            'TIME': '체결시간',
            'OPEN': '시가',
            'HIGH': '고가',
            'LOW': '저가',
            'CLOSE': '현재가',
            'VOLUME': '거래량'
        }

        lastls, ls = [], []

        # 가장 최근 3개봉을 RETURN 함
        for eachrow in range(3):
            ls = []
            for each in dic.keys():
                if each == 'TIME':
                    datetime = self._comm_get_data(trcode, "", rqname, eachrow, dic[each])
                    date = datetime[:8]
                    time = datetime[8:]
                    ls = [date, time]
                else:
                    value = self._comm_get_data(trcode, "", rqname, eachrow, dic[each])
                    ls.append(int(value.replace('-', '').replace('+', '')))
            lastls.append(ls)

        df = pd.DataFrame(data=lastls,
                          columns=['DATE', 'TIME', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOLUME'])

        return df.sort_values(['DATE', 'TIME'], ascending=[True, True]).reset_index(drop=True)

    # 분봉 10일치밖에 안줌
    def _opt10080_day(self, rqname, trcode):
        data_cnt = self._get_repeat_cnt(trcode, rqname)
        asint = ['OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOLUME']
        dic = {
            'TIME': '체결시간',
            'OPEN': '시가',
            'HIGH': '고가',
            'LOW': '저가',
            'CLOSE': '현재가',
            'VOLUME': '거래량'
        }

        lastls = []
        for eachrow in range(60):
            ls = []
            for each in dic.keys():
                if each == 'TIME':
                    datetime = self._comm_get_data(trcode, "", rqname, eachrow, dic[each])
                    date = datetime[:8]
                    time = datetime[8:]
                    ls = [date, time]
                else:
                    value = self._comm_get_data(trcode, "", rqname, eachrow, dic[each])
                    ls.append(int(value.replace('-', '').replace('+', '')))
            lastls.append(ls)

        df = pd.DataFrame(data=lastls,
                          columns=['DATE', 'TIME', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOLUME'])

        return df.head(60).sort_values(['DATE', 'TIME'], ascending=[True, True]).reset_index(drop=True)

        # writetempFile(tempStr)

    def _opw00018(self, rqname, trcode):
        data_cnt = self._get_repeat_cnt(trcode, rqname)
        rtdic = {}
        for i in range(data_cnt):

            t = []
            dic = {
                'stockcode': '종목번호',
                'stockname': '종목명',
                'volume': '보유수량',
                'buycum': '매입금액',
                'price_buying': '매입가',
                'price_now': '현재가',
                'profit': '수익률(%)',
                'netprofit': '평가손익',
            }

            for key in dic.keys():
                if (key in ['stockcode', 'stockname']):
                    dic[key] = self._comm_get_data(trcode, "", rqname, i, dic[key]).replace('A', '')
                else:
                    dic[key] = self.parsenumber(self._comm_get_data(trcode, "", rqname, i, dic[key]))

            eachtuple = tuple(dic.values())
            rtdic[eachtuple[0]] = eachtuple

        return rtdic


class KiwoomProcessingError(Exception):
    """ 키움에서 처리실패에 관련된 리턴코드를 받았을 경우 발생하는 예외 """

    def __init__(self, msg="처리 실패"):
        self.msg = msg

    def __str__(self):
        return self.msg

    def __repr__(self):
        return self.msg


class ReturnCode(object):
    """ 키움 OpenApi+ 함수들이 반환하는 값 """

    OP_ERR_NONE = 0  # 정상처리
    OP_ERR_FAIL = -10  # 실패
    OP_ERR_LOGIN = -100  # 사용자정보교환실패
    OP_ERR_CONNECT = -101  # 서버접속실패
    OP_ERR_VERSION = -102  # 버전처리실패
    OP_ERR_FIREWALL = -103  # 개인방화벽실패
    OP_ERR_MEMORY = -104  # 메모리보호실패
    OP_ERR_INPUT = -105  # 함수입력값오류
    OP_ERR_SOCKET_CLOSED = -106  # 통신연결종료
    OP_ERR_SISE_OVERFLOW = -200  # 시세조회과부하
    OP_ERR_RQ_STRUCT_FAIL = -201  # 전문작성초기화실패
    OP_ERR_RQ_STRING_FAIL = -202  # 전문작성입력값오류
    OP_ERR_NO_DATA = -203  # 데이터없음
    OP_ERR_OVER_MAX_DATA = -204  # 조회가능한종목수초과
    OP_ERR_DATA_RCV_FAIL = -205  # 데이터수신실패
    OP_ERR_OVER_MAX_FID = -206  # 조회가능한FID수초과
    OP_ERR_REAL_CANCEL = -207  # 실시간해제오류
    OP_ERR_ORD_WRONG_INPUT = -300  # 입력값오류
    OP_ERR_ORD_WRONG_ACCTNO = -301  # 계좌비밀번호없음
    OP_ERR_OTHER_ACC_USE = -302  # 타인계좌사용오류
    OP_ERR_MIS_2BILL_EXC = -303  # 주문가격이20억원을초과
    OP_ERR_MIS_5BILL_EXC = -304  # 주문가격이50억원을초과
    OP_ERR_MIS_1PER_EXC = -305  # 주문수량이총발행주수의1%초과오류
    OP_ERR_MIS_3PER_EXC = -306  # 주문수량이총발행주수의3%초과오류
    OP_ERR_SEND_FAIL = -307  # 주문전송실패
    OP_ERR_ORD_OVERFLOW = -308  # 주문전송과부하
    OP_ERR_MIS_300CNT_EXC = -309  # 주문수량300계약초과
    OP_ERR_MIS_500CNT_EXC = -310  # 주문수량500계약초과
    OP_ERR_ORD_WRONG_ACCTINFO = -340  # 계좌정보없음
    OP_ERR_ORD_SYMCODE_EMPTY = -500  # 종목코드없음

    CAUSE = {
        0: '정상처리',
        -10: '실패',
        -100: '사용자정보교환실패',
        -102: '버전처리실패',
        -103: '개인방화벽실패',
        -104: '메모리보호실패',
        -105: '함수입력값오류',
        -106: '통신연결종료',
        -200: '시세조회과부하',
        -201: '전문작성초기화실패',
        -202: '전문작성입력값오류',
        -203: '데이터없음',
        -204: '조회가능한종목수초과',
        -205: '데이터수신실패',
        -206: '조회가능한FID수초과',
        -207: '실시간해제오류',
        -300: '입력값오류',
        -301: '계좌비밀번호없음',
        -302: '타인계좌사용오류',
        -303: '주문가격이20억원을초과',
        -304: '주문가격이50억원을초과',
        -305: '주문수량이총발행주수의1%초과오류',
        -306: '주문수량이총발행주수의3%초과오류',
        -307: '주문전송실패',
        -308: '주문전송과부하',
        -309: '주문수량300계약초과',
        -310: '주문수량500계약초과',
        -340: '계좌정보없음',
        -500: '종목코드없음'
    }


app = QApplication(sys.argv)

if __name__ == "__main__":
    kiwoom = Kiwoom()
    kiwoom.comm_connect()

    s_rqname = 'gyb0418'
    s_screenno = '0101'
    s_accno = '5288885510'

    s_ordertype = 1

    s_code = '079940'
    s_quantity = 1
    s_price = 8000
    s_hoga = '00'
    s_orderno = ''

    rt = kiwoom.send_order(s_rqname, s_screenno, s_accno, s_ordertype, s_code, s_quantity, s_price, s_hoga, s_orderno)
    time.sleep(1)

    rt = kiwoom.send_order(s_rqname, s_screenno, s_accno, s_ordertype, s_code, s_quantity, s_price, s_hoga, s_orderno)
    time.sleep(1)