from jazzstock_bot.common import connector_db as db
import pandas as pd
import time



def db_readAll():
    # DB에서 [종목명,종목코드] 로 구성된 데이터셋을 받아옴.
    # dbUpdateDate = db.selectSingleValue('SELECT max(date) FROM test.t_stock_shares_info')

    query = """

                        SELECT A.STOCKCODE, A.STOCKNAME
                        FROM jazzdb.T_STOCK_CODE_MGMT A
                        WHERE 1=1
                        AND A.LISTED = 1
                                                        """

    for eachRow in db.select(query):
        if (len(eachRow) > 0):
            itemDic[eachRow[1].upper()] = eachRow[0]
            codeDic[eachRow[0]] = eachRow[1].upper()

    print("[INFO] 종목명/종목코드를 메모리에 읽어왔습니다, 남은 종목 수: ", len(itemDic.keys()))


def get_bb_description(self, bbp=None, bbw=None, threshold= 0, dev= False, verbose=False):
        '''
        볼린저밴드를 해석해준다
        return
            1) 특이점 x,y 좌표, 상태값

        '''
        

        if self.verbose or verbose:
            print('='*100)
            print(' * BOLINGER_BAND_DESCRIPTION')
            print('='*100)
        
        if bbp is None:
            bbp = self.df_snd_previous_period.BBP.values
        if bbw is None:
            bbw = self.df_snd_previous_period.BBW.values

        if isinstance(bbp, list):
            bbp = np.asarray(bbp)
        if isinstance(bbp, list):
            bbw = np.asarray(bbw)


        ret_df = pd.DataFrame(columns=['PREV_IDX', 'DATE_IDX', 'INTERVAL', 'EVENT', 'PREVENT', 'GRAD', 'BBW'])

        bbp_grad = np.gradient(bbp).round(2)

        # BBP 그리기
        if dev:
            plt.plot(np.arange(len(bbp)), bbp)
            plt.plot(np.arange(len(bbp)), np.asarray([threshold]*len(bbp)))
            plt.plot(np.arange(len(bbp)), np.asarray([1-threshold]*len(bbp)))

        prev_point = 0
        prev_event = '-'
        mr = []
        
        '''
        LD: BB LOWERBAND DOWNWARD BREAKTHROUGH, 볼린저밴드 하단 하향돌파
        LU: BB LOWERBAND UPWARD BREAKTHROUGH, 볼린저밴드 하단 상향돌파
        MD: 20MA BAND DOWNWARD BREAKTHROUGH, 20일이평선 상향 돌파
        MU: 20MA BAND DOWNWARD BREAKTHROUGH, 20일이평선 하향 돌파
        UU: BB UPPERBAND DOWNWARD BREAKTHROUGH, 볼린저밴드 상단 상향돌파
        UD: BB UPPERBAND UPWARD BREAKTHROUGH,볼린저밴드 상단 하향돌파
        
        LD, UU
        
        '''
        for i in range(0, len(bbp)-1):
            if bbp[i] > threshold >= bbp[i+1]:
                ret_df.loc[len(ret_df)] = [prev_point, i, i-prev_point, 'LD', prev_event, bbp_grad[i+1], bbw[i+1]]
                prev_point = i
                prev_event = 'LD'
                
            elif bbp[i] < threshold <= bbp[i+1]:
                ret_df.loc[len(ret_df)] = [prev_point, i, i-prev_point, 'LU', prev_event, bbp_grad[i+1], bbw[i+1]]
                prev_point = i
                prev_event = 'LU'
                
                
            elif bbp[i] > 0.5 >= bbp[i+1]:
                ret_df.loc[len(ret_df)] = [prev_point, i, i-prev_point, 'MD', prev_event, bbp_grad[i+1], bbw[i+1]]
                prev_point = i
                prev_event = 'MD'
                
            elif bbp[i] < 0.5 <= bbp[i+1]:
                ret_df.loc[len(ret_df)] = [prev_point, i, i-prev_point, 'MU', prev_event, bbp_grad[i+1], bbw[i+1]]
                prev_point = i
                prev_event = 'MU'

            elif bbp[i] < 1-threshold <= bbp[i+1]:
                ret_df.loc[len(ret_df)] = [prev_point, i, i-prev_point, 'UU', prev_event, bbp_grad[i+1], bbw[i+1]]
                prev_point = i
                prev_event = 'UU'

            elif bbp[i] > 1-threshold >= bbp[i+1]:
                ret_df.loc[len(ret_df)] = [prev_point, i, i-prev_point, 'UD', prev_event, bbp_grad[i+1], bbw[i+1]]
                prev_point = i
                prev_event = 'UD'
                
        ret_df.loc[len(ret_df)] = [prev_point, i, i-prev_point, '--', prev_event, bbp_grad[i+1], bbw[i+1]]

        if dev:
            spoint = [0]    
            for i in range(len(bbp)-1):
                change_size = (bbp_grad[i]-bbp_grad[i+1]).round(2)
                sign_change = change_size > 0
                if change_size > 0.09 and sign_change:
                    spoint.append(i)
                    plt.scatter(i, bbp[i])

            spoint.append(len(bbp)-1)
            prev_slope = 0
            for i in range(len(spoint)-1):
                x1 = spoint[i]
                x2 = spoint[i+1]
                y1 = bbp[spoint[i]]
                y2 = bbp[spoint[i+1]]

                x_values = [x1, x2]
                y_values = [y1, y2]
                plt.plot(x_values, y_values)
                
        self.df_bb_desc = ret_df.copy()
        
        if self.verbose or verbose:
            print(ret_df[['PREV_IDX', 'DATE_IDX', 'INTERVAL', 'PREVENT', 'EVENT', 'GRAD', 'BBW']])
        
        
        
        return ret_df[['PREV_IDX', 'DATE_IDX', 'INTERVAL', 'PREVENT', 'EVENT', 'GRAD', 'BBW']]


    insertQuery = '''

        INSERT INTO jazzdb.T_STOCK_BB
        VALUES ''' + str(data)[1:-1]


    db.insert(insertQuery)




def gettoday():
    td = db.selectSingleValue('SELECT cast(DATE AS CHAR) AS DATE FROM jazzdb.T_DATE_INDEXED WHERE CNT = 0')
    return td


    
    
codeDic, itemDic = {}, {}
db_readAll()
today = gettoday()

if __name__ == '__main__':

    for i, eachcode in enumerate(codeDic.keys()):
        try:
            makebb(eachcode, today)
            if i%500 == 0:
                print(i, eachcode, 'DONE')
        except Exception as e:
            print(i, eachcode, e)
            
        
        
            # print('error %s' % (each))


