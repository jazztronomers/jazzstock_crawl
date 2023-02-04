from jazzstock_bot.common import connector_db as db
import requests
import pandas as pd
import time
import re
import sys
from datetime import datetime as dt
from lxml import html



headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
today = dt.now().date()


def crawl_stake_info(code):
    
    global today, headers
    url = "http://companyinfo.stock.naver.com/v1/company/c1010001.aspx?cmp_cd=%s&target=finsum_more" %(code)
    html = requests.get(url, headers=headers).text

    try:
        df_list = pd.read_html(html)#, index_col='주요재무정보')

        # print(df_list[1])

        distriPerc = df_list[1].iloc[6][1].split('/')[1]

        shares = df_list[1].iloc[6][1].split('/')[0]
        # for i in range (0,len(df_list[4])):
        #     if(str(df_list[4].iloc[i][0]) != 'nan'):
        #         print([df_list[4].iloc[i][0].split('외')[0].strip(),int(round(df_list[4].iloc[i][1]))])

        obj = []
        obj.append(["발행주식수",int(shares.replace('주 ','').replace(',',''))])
        obj.append(['유통주식수',int(int(shares.replace('주 ','').replace(',',''))*float(distriPerc.replace('%',''))*0.01)])
        for i in range(0, len(df_list[4])):

            if(str(df_list[4].iloc[i][0]) != 'nan'):
                obj.append([df_list[4].iloc[i][0].split('외')[0].strip(),df_list[4].iloc[i][1]])


        querylist = []
        for eachObj in obj:

            #데이터 db insert
            query = 'INSERT INTO jazzdb.T_STOCK_SHARES_INFO VALUES("%s","%s","%s","%s")' %(code,eachObj[0],eachObj[1],dt.now().date())
            querylist.append(query)

        return querylist

    except Exception as e:
        print(code, e)
        return []
        # db.insert(query)


def crawl_ohlc(code):
    
    global today, headers
    url = "https://finance.naver.com/item/coinfo.nhn?code=%s" % (code)

    page = requests.get(url, headers=headers).content.decode('euc-kr', 'ignore')
    tree = html.fromstring(page)

    obj = tree.xpath('//dl[@class="blind"]//text()')

    target = ['현재가', '시가', '고가', '저가', '거래량', '거래대금']
    targetDic = {

        '현재가': 'CLOSE',
        '시가': 'OPEN',
        '고가': 'HIGH',
        '저가': 'LOW',
        '거래량': 'VOLUME',
        '거래대금': 'AMOUNT'

    }

    targetDicValue = {}
    for each in obj:
        if (len(each.strip()) > 0):
            content = each.split(' ')
            if (content[0] in target):
                # print(targetDic[content[0]], content[1])
                targetDicValue[targetDic[content[0]]] = int(re.search(r'\d+', content[1].replace(',', '')).group())
    
    # 데이터가 있는지 체크.
    if len(targetDicValue) > 0:
        query = f"INSERT INTO `jazzdb`.`T_STOCK_OHLC_DAY` (`STOCKCODE`, `DATE`, `OPEN`, `HIGH`, `LOW`, `CLOSE`, `VALUE`, `ADJCLASS`, `ADJRATIO`) VALUES ('{code}', '{today}', '{targetDicValue['OPEN']}', '{targetDicValue['HIGH']}', '{targetDicValue['LOW']}', '{targetDicValue['CLOSE']}', '{targetDicValue['AMOUNT']}', '0', '-1');"
        return [query]
    
    else:
        return []
    # db.insert(query)


def get_stockcode_to_crawl():

    query = """

                        SELECT A.STOCKCODE
                        FROM jazzdb.T_STOCK_CODE_MGMT A
                        WHERE 1=1
                        AND A.STOCKCODE NOT IN (

                            SELECT STOCKCODE
                            FROM jazzdb.T_STOCK_OHLC_DAY
                            WHERE DATE = '%s'
                            GROUP BY STOCKCODE
                        )
                        AND A.LISTED = 1
                                                        """ % (today)

    return db.selectSingleColumn(query)

def get_stockcode_to_crawl_shares():

    query = """

                        SELECT A.STOCKCODE
                        FROM jazzdb.T_STOCK_CODE_MGMT A
                        WHERE 1=1
                        AND A.STOCKCODE NOT IN (

                            SELECT STOCKCODE
                            FROM jazzdb.T_STOCK_SHARES_INFO
                            WHERE DATE = '%s'
                            GROUP BY STOCKCODE
                        )
                        AND A.LISTED = 1
                                                        """ % (today)

    return db.selectSingleColumn(query)


if __name__ == '__main__':
    
    stockcodes = get_stockcode_to_crawl()
    # stockcodes = ["999999", "079940"]
    
    for i, stockcode in enumerate(stockcodes):
        
        
        print(0, i, stockcode)
       
        try: 
            query_to_execute = []
            query_to_execute = query_to_execute + crawl_ohlc(stockcode)
            time.sleep(0.3)
            query_to_execute = query_to_execute + crawl_stake_info(stockcode)
       
        except Exception as e:
            
            print("ERROR: %s"%(e))
            time.sleep(0.5)
            query_to_execute = []
            query_to_execute = query_to_execute + crawl_ohlc(stockcode)
            time.sleep(0.3)
            query_to_execute = query_to_execute + crawl_stake_info(stockcode)
        
        for j, each_query in enumerate(query_to_execute):
            try:
                db.insert(each_query)
            except Exception as e:
                print(i, stockcode, j, e, each_query)
	
        
        time.sleep(0.2)
        
    stockcodes = get_stockcode_to_crawl_shares()
    # stockcodes = ["999999", "079940"]
    
    for i, stockcode in enumerate(stockcodes):
        
        
        print(1, i, stockcode)
        try: 
            query_to_execute = []
            query_to_execute = query_to_execute + crawl_stake_info(stockcode)

        except Exception as e:

            print("ERROR: %s"%(e))
            query_to_execute = []
            query_to_execute = query_to_execute + crawl_stake_info(stockcode)

        
        
        
        for j, each_query in enumerate(query_to_execute):
            try:
                db.insert(each_query)
            except Exception as e:
                print(i, stockcode, j, e, each_query)
	
        
        time.sleep(0.2)
        




