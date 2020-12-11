from selenium import webdriver
import time
import re
import pandas as pd
# 내려받은 chromedriver의 위치
driver = webdriver.Chrome('C://chromedriver.exe')

# 웹 자원 로드를 위해 3초까지 기다린다.
driver.implicitly_wait(1)



# 거시경제
# url = 'https://blog.naver.com/PostList.nhn?blogId=ionia17&categoryNo=15&parentCategoryNo=15&skinType=&skinId=&from=menu'

# 기업분석
url = 'https://blog.naver.com/PostList.nhn?blogId=ionia17&categoryNo=9&parentCategoryNo=9&skinType=&skinId=&from=menu'


txt = driver.get(url)
time.sleep(2)


# ==========================================================================
# 과거 수집할때만 필요하지, daily로는 필요없음
# ==========================================================================
# # 몇줄보기 셀렉트박스 클릭
# element = driver.find_element_by_xpath('//*[@id="listCountToggle"]/i[1]').click()
# time.sleep(2)
#
# # 30줄보기로 변경
# element_per_page = driver.find_elements_by_xpath('/html/body/div[6]/div[1]/div[2]/div[2]/div[2]/div[1]/div[1]/div/div[3]/div/table[2]/tbody/tr/td[2]/div/div/div[1]/div/div/div/a[5]')
# element_per_page[0].click()
# time.sleep(2)

# ==========================================================================
row_per_page = 5



def main():
    '''
    페이지 전환없이 첫번째 페이지 5종목을 순차적으로 insert함
    '''
    df= pd.DataFrame(columns=['STOCKCODE', 'DATE', 'CONTENT'])

    for i in range(1, row_per_page+1, 1):

        try:

            '''//*[@id="listTopForm"]/table/tbody/tr[2]/td[2]/div/span'''
            '''//*[@id="listTopForm"]/table/tbody/tr[2]/td[2]/div/span'''

            # 페이지 변경후
            '''//*[@id="listTopForm"]/table/tbody/tr[1]/td[1]/div/span/a'''

            # 상단메뉴 looping
            time.sleep(1)
            x_path_for_row='//*[@id="listTopForm"]/table/tbody/tr[%s]/td[1]/div/span/a'%(i)
            x_path_for_date='//*[@id="listTopForm"]/table/tbody/tr[%s]/td[2]/div/span'%(i)



            row = driver.find_element_by_xpath(x_path_for_row)
            date = driver.find_element_by_xpath(x_path_for_date).text
            title = row.text
            row.click()
            time.sleep(2)
            content = driver.find_element_by_xpath('//*[@id="postViewArea"]').text



            outer = re.compile("\(([^)]+)\)")
            m = outer.findall(title)

            for stockcode in m:
                print(stockcode, date, title, content.strip()[:120])
                # df.loc[len(df)] = [stockcode, date, content.strip()[:120]]
                ret = insert(stockcode, date, content)

            if not ret:
                return False

            else:
               time.sleep(1)

        except Exception as e:
            print(' * ERROR :', e)

    return df

def insert(stockcode, date, content):
    import jazzstock_bot.common.connector_db as db
    from datetime import datetime


    content = content[:200]

    if len(date.split('. ')) ==3:
        date = date.split('. ')
        yyyy = date[0]
        mm = date[1].zfill(2)
        dd = date[2].replace('.','').zfill(2)

    else:
        today = datetime.now().date()
        yyyy= today.year
        mm = str(today.month).zfill(2)
        dd = str(today.day).zfill(2)
    try:
        query = f"INSERT INTO `jazzdb`.`T_STOCK_TEXT` (`STOCKCODE`, `DATE`, `CONT_TYPE`, `AUTHOR`, `AUTHOR2`, `CONTENT`) VALUES ('{stockcode}', '{'%s%s%s' % (yyyy, mm, dd)}', 'B', 'jameslee', '', '{content}');"
        print(query)
        db.insert(query)
        return True
    except Exception as e:
        print(e)
        return False


def next_page():
    '''
    물론 다음페이지로 넘어갈 필요도 없다, 매일 돌릴꺼니까


    '''
    from bs4 import BeautifulSoup


    '''//*[@id="toplistWrapper"]/div[2]/div/a[9]'''
    '''//*[@id="toplistWrapper"]/div[2]/div/a[1]'''
    '''//*[@id="toplistWrapper"]/div[2]/div/a[2]'''
    '''//*[@id="toplistWrapper"]/div[2]/div/a[9]'''
    '''//*[@id="toplistWrapper"]/div[2]/div/a[10]''' # next page

    # 정석은 html parser로 object 형태로 파싱하는것
    html = driver.find_element_by_xpath('//*[@id="toplistWrapper"]/div[2]').get_attribute('innerHTML')
    soup = BeautifulSoup(html, 'html.parser')
    for each_strong in soup.find_all('strong'):
        if '현재 페이지' in each_strong.text:
            outer = re.compile("\((.+)\)")
            m = outer.search(str(each_strong))
            current_page_idx = m.group(1)
            next_page = driver.find_element_by_xpath('//*[@id="toplistWrapper"]/div[2]/div/a[%s]' % (current_page_idx))
            next_page.click()

# def change_specific_page(page_idx):
#     from bs4 import BeautifulSoup
#
#
#     '''//*[@id="toplistWrapper"]/div[2]/div/a[9]'''
#     '''//*[@id="toplistWrapper"]/div[2]/div/a[1]'''
#     '''//*[@id="toplistWrapper"]/div[2]/div/a[2]'''
#     '''//*[@id="toplistWrapper"]/div[2]/div/a[9]'''
#     '''//*[@id="toplistWrapper"]/div[2]/div/a[10]''' # next page
#
#     # 정석은 html parser로 object 형태로 파싱하는것
#     html = driver.find_element_by_xpath('//*[@id="toplistWrapper"]/div[2]').get_attribute('innerHTML')
#     soup = BeautifulSoup(html, 'html.parser')
#     for each_strong in soup.find_all('strong'):
#         if '현재 페이지' in each_strong.text:
#             outer = re.compile("\((.+)\)")
#             m = outer.search(str(each_strong))
#             current_page_idx = m.group(1)
#             next_page = driver.find_element_by_xpath('//*[@id="toplistWrapper"]/div[2]/div/a[%s]' % (current_page_idx))
#             next_page.click()



# ==========================================================================
# 과거수집용
# ==========================================================================
# itr = 0
# while True:
#     print("PAGE:", itr)
#     print('-'*50)
#     df = looping_row()
#     df.to_csv('%s_%s.csv'%(itr, str(int(time.time()))), encoding='euc-kr')
#     itr +=1
#     next_page()
#     time.sleep(5)


if __name__=='__main__':
    main()
    driver.close()