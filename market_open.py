import requests
import sys
from lxml import html

def is_market_open(debug=False):

    url = "https://finance.naver.com/"
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
    page = requests.get(url, headers=headers).content.decode('euc-kr','ignore')


    tree = html.fromstring(page)
    obj = tree.xpath('//dl[@class="blind"]//text()')

    

    marketopen= False

    # 금일 개장전 : 장전대기중
    # 전 거래일 장마감 : 장종료

    for each in obj:
        line = each.replace('\n','').strip()
        if(len(line)>0):
            if('년' in line and '월' in line and '일' in line):
                if debug:
                    print("DEBUG: ", line)

                if '장마감' in line:
                    marketopen = False
                else:
                    marketopen = True

    return marketopen


if __name__=='__main__':

    debug = False
    if len(sys.argv)>1:
        debug=True
    
    print(is_market_open(debug))
