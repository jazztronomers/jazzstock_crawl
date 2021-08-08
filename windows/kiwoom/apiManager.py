# -*- coding: utf-8 -*-

# import jazzStockBot.stockUtil.plotUtil as pu
from jazzstock_crawl.windows.kiwoom import dateManager as dp
from common import connector_db as db
import time

# TEMP FILE READER
def readTempFile():
    a = open("temp.txt", 'r', encoding='utf-8')
    rtlist = []
    for er in a.readlines():
        rtlist.append(er.replace('\n', '').split('\t'))
    a.close()
    return rtlist

# TEMP FILE WRITER
def writetempFile(tempstr):
    a = open("temp.txt", 'w', encoding='utf-8')
    a.write(tempstr)
    a.close()

# jazzdb.T_STOCK_OHLC_DAY
def api_get_ohlc_day(apiObj, stockCode, date):
    apiObj.set_input_value("종목코드", stockCode)
    apiObj.set_input_value("기준일자", date)
    apiObj.set_input_value("수정주가구분", 1)
    ret = apiObj.comm_rq_data("opt10081_req", "opt10081", 0, "0101")

    return ret



def api_get_ohlc_min_test(apiObj, stockCode, ):
    apiObj.set_input_value("종목코드", stockCode)
    apiObj.set_input_value("틱범위", 5)
    apiObj.set_input_value("수정주가구분", 1)
    apiObj.set_input_value("기준일자", '20201023')
    ret = apiObj.comm_rq_data("opt10080_req", "opt10080", 0, "0101")
    return ret


# jazzdb.T_STOCK_OHLC_MIN
def api_getMinChart(apiObj, stockCode, ):
    apiObj.set_input_value("종목코드", stockCode)
    apiObj.set_input_value("틱범위", 5)
    apiObj.set_input_value("수정주가구분", 1)
    apiObj.set_input_value("기준일자", '20190101')
    apiObj.comm_rq_data("opt10080_req", "opt10080", 0, "0101")


    query = '''

        SELECT DATE_FORMAT(DATE,"%%Y%%m%%d") AS DATE
        FROM jazzdb.T_STOCK_OHLC_MIN
        WHERE 1=1
        AND STOCKCODE = '%s'

    ''' % (stockCode)


    datelist = db.selectSingleColumn(query)
    data = []
    # print(readTempFile())
    # tempStr = tempStr + stockCode + '\t' + date + '\t' + open + '\t' + high + '\t' + low + '\t' + close + '\t' + value + '\t' + str(adjustClass) + '\t' + str(adjustRatio) + '\n'
    for eachLine in readTempFile():
        if eachLine[0] not in datelist:
            # print(eachLine)
            eachLine.insert(0,stockCode)
            data.append(tuple(eachLine))

    insertQuery = '''

        INSERT INTO jazzdb.T_STOCK_OHLC_MIN
        VALUES ''' + str(data)[1:-1]

    # insertQuery= insertQuery.replace('VALUES (','VALUES (\''+stockCode+"',")
    db.insert(insertQuery)
    # print(insertQuery)

    return len(data)

def api_realtime_getMinChart(apiObj, stockCode, ):
    print('HERE')
    apiObj.set_input_value("종목코드", stockCode)
    apiObj.set_input_value("틱범위", 1)
    apiObj.set_input_value("수정주가구분", 1)
    apiObj.set_input_value("기준일자", '20190601')
    apiObj.comm_rq_data("opt10080_req", "opt10080", 0, "0101")


    query = '''

        SELECT DATE_FORMAT(DATE,"%%Y%%m%%d") AS DATE
        FROM jazzdb.T_STOCK_OHLC_MIN
        WHERE 1=1
        AND STOCKCODE = '%s'

    ''' % (stockCode)

    print(query)

    datelist = db.selectSingleColumn(query)
    # data = []
    # print(readTempFile())
    # tempStr = tempStr + stockCode + '\t' + date + '\t' + open + '\t' + high + '\t' + low + '\t' + close + '\t' + value + '\t' + str(adjustClass) + '\t' + str(adjustRatio) + '\n'
    for eachLine in readTempFile():
        if eachLine[0] in '20191107' :
            print(eachLine)
            # eachLine.insert(0,stockCode)



# jazzdb.T_STOCK_SND_DAY
def api_getSndDB(apiObj, stockCode, date):
    apiObj.set_input_value("일자", date)
    apiObj.set_input_value("종목코드", stockCode)
    apiObj.set_input_value("금액수량구분", '2')
    apiObj.set_input_value("매매구분", '0')
    apiObj.set_input_value("단위구분", '1')
    apiObj.comm_rq_data("opt10060_req", "opt10060", 0, "0796")

    query = '''

        SELECT DATE_FORMAT(DATE,"%%Y%%m%%d") AS DATE
        FROM jazzdb.T_STOCK_SND_DAY
        WHERE 1=1
        AND STOCKCODE = '%s'

    ''' % (stockCode)

    datelist = db.selectSingleColumn(query)
    data = []


    for eachLine in readTempFile():
        if eachLine[0] not in datelist:
            data.append(tuple([stockCode] + eachLine))
    insertQuery = '''INSERT INTO jazzdb.T_STOCK_SND_DAY
                     VALUES ''' + str(data)[1:-1]


    db.insert(insertQuery)

    return len(data)



# jazzdb.T_STOCK_SND_DAY
def api_getSndForWin(apiObj, stockCode, date, winCode):


    apiObj.set_input_value("회원사코드", winCode)
    apiObj.set_input_value("종목코드", stockCode)
    apiObj.set_input_value("종료일자", date)
    apiObj.comm_rq_data("opt10078_req", "opt10078", 0, "0127")


    query = '''

        SELECT DATE_FORMAT(DATE,"%%Y%%m%%d") AS DATE
        FROM jazzdb.T_STOCK_SND_WINDOW_ISOLATED
        WHERE 1=1
        AND STOCKCODE = '%s' AND WINCODE = '%s'

    ''' % (stockCode, winCode)

    datelist = db.selectSingleColumn(query)

    data = []


    # tempStr = tempStr + date + '\t' + price + '\t' + volume + '\t' + forSum + '\t' + insSum + '\t' + per + '\t' + finan + '\t' + samo + '\t' + yg + '\t' + tusin + '\t' + insur + '\t' + nation + '\t' + bank + '\t' + otherfinan + '\t' + othercorpor + '\t' + otherfor + '\n'
    # 해당종목의 특정일자 DATE 가 없으면 INSERT 함

    for eachLine in readTempFile():
        if eachLine[0] not in datelist:
            data.append(tuple([stockCode,winCode] + eachLine))
    insertQuery = '''INSERT INTO jazzdb.T_STOCK_SND_WINDOW_ISOLATED
                     VALUES ''' + str(data)[1:-1]



    db.insert(insertQuery)
    return len(data)


# CHECK WORKING DAY
def api_checkDate(apiObj, date):
    apiObj.set_input_value("종목코드", '079940')
    apiObj.set_input_value("기준일자", date)
    apiObj.set_input_value("수정주가구분", 1)
    ret = apiObj.comm_rq_data("opt10081_req", "opt10081", 0, "0101")

    # workingDateA = readTempFile()[0][1]
    # workingDateB = readTempFile()[1][1]


    workingDateA = ret.DATE.values[0]
    workingDateB = ret.DATE.values[1]

    return [workingDateA, workingDateB]



def api_index_ohlc_day(apiObj, indexcode='001', date='20200514'):

    apiObj.set_input_value("업종코드", indexcode)
    apiObj.set_input_value("기준일자", date)            # YYYYMMDD
    rt = apiObj.comm_rq_data("opt20006_req", "opt20006", 0, "0214")
    return rt



# 종합수급
@DeprecationWarning
def api_getSndSummary(apiObj, stockName, stockCode, date, share):
    foreignCode = {36: "모건", 42: "CS", 44: "메릴", 45: "골드만"}
    eachDic = {}
    sumDic = {}

    # print('[debug] api get snd summray input object ', apiObj,stockName,stockCode,date,share)

    for eachCode in foreignCode.keys():

        apiObj.set_input_value("회원사코드", eachCode)
        apiObj.set_input_value("종목코드", stockCode)
        apiObj.set_input_value("시작일자", '20170701')
        apiObj.set_input_value("종료일자", date)

        apiObj.comm_rq_data("opt10078_req", "opt10078", 0, "0127")

        for eachLine in readTempFile():
            eachDic[eachLine[0]] = eachLine[1]

        dateList = []
        valueList = []
        for eachDate in eachDic.keys():
            valueList.insert(0, eachDic[eachDate])
            dateList.insert(0, eachDate)
        sumDic[foreignCode[eachCode]] = valueList
        if ("date" not in sumDic.keys()): sumDic["date"] = dateList

    nplist = []
    for x in sumDic.keys():
        tempList = []
        tempList.append(x)
        tempList = tempList + sumDic[x]
        nplist.append(tempList)

    apiObj.set_input_value("일자", date)
    apiObj.set_input_value("종목코드", stockCode)
    apiObj.set_input_value("금액수량구분", '2')
    apiObj.set_input_value("매매구분", '0')
    apiObj.set_input_value("단위구분", '1')

    # tempStr = tempStr + date + '\t' + price + '\t' + forSum + '\t' + insSum + '\t' + finan + '\t' + samo + '\t' + yg + '\t' + tusin + '\t' + insur + '\t' + nation + '\n'
    dateL = []
    priceL = []
    forSumL = []
    insSumL = []
    finanL = []
    samoL = []
    ygL = []
    tusinL = []
    insurL = []
    nationL = []
    perL = []
    apiObj.comm_rq_data("opt10060_req", "opt10060", 0, "0796")

    for eachLine in readTempFile():
        dateL.append(eachLine[0])
        priceL.append(eachLine[1].replace('+', '').replace('-', ''))
        forSumL.append(eachLine[2])
        insSumL.append(eachLine[3])
        finanL.append(eachLine[4])
        samoL.append(eachLine[5])
        ygL.append(eachLine[6])
        tusinL.append(eachLine[7])
        insurL.append(eachLine[8])
        nationL.append(eachLine[9])
        perL.append(eachLine[11])

    dateL.append("date")
    priceL.append("주가")
    forSumL.append("외인")
    insSumL.append("기관")
    finanL.append("금투")
    samoL.append("사모")
    ygL.append("연기")
    tusinL.append("투신")
    insurL.append("보험")
    nationL.append("국가")
    perL.append("개인")

    list2 = [dateL[::-1], priceL[::-1], perL[::-1], insSumL[::-1], finanL[::-1], samoL[::-1], ygL[::-1], tusinL[::-1],
             insurL[::-1], nationL[::-1], forSumL[::-1]]

    time.sleep(1)
    apiObj.set_input_value("시작일자", '20170601')
    apiObj.set_input_value("종료일자", date)
    apiObj.set_input_value("종목코드", stockCode)
    apiObj.set_input_value("전체구분", '0')

    apiObj.comm_rq_data("opt20068_req", "opt20068", 0, "1061")

    # 대차잔고
    sbL = []

    for eachLine in readTempFile():
        sbL.append(eachLine[1])

    sbL.append("대차")
    listSb = [sbL[::-1]]

    nplist = list2 + nplist + listSb
    # pu.sndPlot(nplist, stockName + '/' + dp.todayStr('n'), share)

@DeprecationWarning
def api_getSndWindow(apiObj, stockCode, date, eachcode):
    apiObj.set_input_value("회원사코드", eachcode)
    apiObj.set_input_value("종목코드", stockCode)
    apiObj.set_input_value("시작일자", '20170701')
    apiObj.set_input_value("종료일자", date)

    apiObj.comm_rq_data("opt10078_reqdb", "opt10078", 0, "0127")

    return readTempFile()

@DeprecationWarning
def api_getSndSpec(apiObj, stockName, stockCode, date, share):
    foreignCode = {2: "신금투", 3: "한투", 6: "신영", 33: "모간서울", 71: "KTB", 54: "노무라", 61: "다이와", 58: "도이치", 43: "유비에스",
                   35: "맥쿼리", 41: "CLSA"}
    eachDic = {}
    sumDic = {}

    # print('[debug] api get snd summray input object ', apiObj,stockName,stockCode,date,share)

    for eachCode in foreignCode.keys():
        print(eachCode, foreignCode[eachCode])
        time.sleep(0.5)
        apiObj.set_input_value("회원사코드", eachCode)
        apiObj.set_input_value("종목코드", stockCode)
        apiObj.set_input_value("시작일자", '20170701')
        apiObj.set_input_value("종료일자", date)

        apiObj.comm_rq_data("opt10078_req", "opt10078", 0, "0127")

        for eachLine in readTempFile():
            eachDic[eachLine[0]] = eachLine[1]

        dateList = []
        valueList = []
        for eachDate in eachDic.keys():
            valueList.insert(0, eachDic[eachDate])
            dateList.insert(0, eachDate)
        sumDic[foreignCode[eachCode]] = valueList
        if ("date" not in sumDic.keys()): sumDic["date"] = dateList

    nplist = []
    for x in sumDic.keys():
        tempList = []
        tempList.append(x)
        tempList = tempList + sumDic[x]
        nplist.append(tempList)

    apiObj.set_input_value("일자", date)
    apiObj.set_input_value("종목코드", stockCode)
    apiObj.set_input_value("금액수량구분", '2')
    apiObj.set_input_value("매매구분", '0')
    apiObj.set_input_value("단위구분", '1')

    # tempStr = tempStr + date + '\t' + price + '\t' + forSum + '\t' + insSum + '\t' + finan + '\t' + samo + '\t' + yg + '\t' + tusin + '\t' + insur + '\t' + nation + '\n'
    dateL = []
    priceL = []
    forSumL = []
    insSumL = []
    finanL = []
    samoL = []
    ygL = []
    tusinL = []
    insurL = []
    nationL = []
    perL = []
    apiObj.comm_rq_data("opt10060_req", "opt10060", 0, "0796")

    for eachLine in readTempFile():
        dateL.append(eachLine[0])
        priceL.append(eachLine[1].replace('+', '').replace('-', ''))
        forSumL.append(eachLine[2])
        insSumL.append(eachLine[3])
        finanL.append(eachLine[4])
        samoL.append(eachLine[5])
        ygL.append(eachLine[6])
        tusinL.append(eachLine[7])
        insurL.append(eachLine[8])
        nationL.append(eachLine[9])
        perL.append(eachLine[11])

    dateL.append("date")
    priceL.append("주가")
    forSumL.append("외인")
    # insSumL.append("기관")
    # # finanL.append("금투")
    # # samoL.append("사모")
    # # ygL.append("연기")
    # # tusinL.append("투신")
    # # insurL.append("보험")
    # # nationL.append("국가")
    perL.append("개인")

    list2 = [dateL[::-1], priceL[::-1], perL[::-1]]

    #
    # time.sleep(1)
    # apiObj.set_input_value("시작일자", '20170601')
    # apiObj.set_input_value("종료일자", date)
    # apiObj.set_input_value("종목코드", stockCode)
    # apiObj.set_input_value("전체구분", '0')
    #
    # apiObj.comm_rq_data("opt20068_req", "opt20068", 0, "1061")
    #
    #
    # # 대차잔고
    # sbL = []
    #
    # for eachLine in readTempFile():
    #     sbL.append(eachLine[1])
    #
    # sbL.append("대차")
    # listSb = [sbL[::-1]]

    nplist = list2 + nplist
    #pu.sndPlot(nplist, stockName + '/' + dp.todayStr('n'), share)

@DeprecationWarning
def api_getGathering(apiObj):
    for itr in range(0, 2):

        apiObj.set_input_value("시작일자", '20170601')
        apiObj.set_input_value("종료일자", '20180302')
        apiObj.set_input_value("매매구분", '2')
        apiObj.set_input_value("시장구분", '000')
        apiObj.comm_rq_data("opt10044_req", "opt10044", 0, "0257")

        for x in readTempFile():
            print(itr, '/', x)

@DeprecationWarning
def api_shortBalance(apiObj, stockName, stockCode, date):
    # apiObj.set_input_value("일자", date)
    apiObj.set_input_value("시작일자", '20170601')
    apiObj.set_input_value("종료일자", date)
    apiObj.set_input_value("종목코드", stockCode)
    apiObj.set_input_value("전체구분", '0')
    apiObj.comm_rq_data("opt20068_req", "opt20068", 0, "1061")












if __name__ == '__main__':

    import manager.Kiwoom as kapi
    apiObj = kapi.Kiwoom()
    apiObj.comm_connect()

    api_getMinChart(apiObj,'079940')

