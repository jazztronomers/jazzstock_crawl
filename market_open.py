import requests
from lxml import html

def is_market_open():

    url = "https://finance.naver.com/"
    page = requests.get(url).content.decode('euc-kr','ignore')
    tree = html.fromstring(page)
    obj = tree.xpath('//dl[@class="blind"]//text()')

    marketopen= False

    # 금일 개장전 : 장전대기중
    # 전 거래일 장마감 : 장종료

    for each in obj:
        line = each.replace('\n','').strip()
        if(len(line)>0):
            if('년' in line and '월' in line and '일' in line):
                if '장마감' in line:
                    marketopen = False
                else:
                    marketopen = True

    return marketopen


if __name__=='__main__':
    
    print(is_market_open())