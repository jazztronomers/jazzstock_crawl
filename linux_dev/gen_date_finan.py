import jazzstock_bot.common.connector_db as db
import pandas as pd

pd.options.display.max_rows = 1000
pd.options.display.max_columns= 500
pd.options.display.expand_frame_repr=False


'''

QUARTER 와 DATE를 조인할 수 있는 컬럼을 생성하는 스크립트
매 거래일데이터가 추가될때 마다 실행되어야함

'''




q_due = {'09':('11-15', '03-01'),  # 이기간 동안은 작년 3Q를
         '12':('03-01', '05-15'),  # 이기간 동안은 작년 4Q를
         '03':('05-15', '08-14'),  # 이기간 동안은 올해 1Q를
         '06':('08-14', '11-15')}  # 이기간 동안은 올해 2Q를

query = '''

SELECT CAST(DATE AS CHAR) AS DATE, CAST(SUBSTRING(DATE, 3,2) AS UNSIGNED) AS YY , SUBSTRING(DATE, 6,5) AS MM_DD
FROM
(
	SELECT DATE, CNT, DATE_FINAN
	FROM jazzdb.T_DATE_INDEXED
	LEFT JOIN jazzdb.T_DATE_FINAN USING (DATE)
	ORDER BY DATE DESC
) A
WHERE 1=1
AND A.DATE_FINAN IS NULL
AND A.CNT < 500


'''

df = db.selectpd(query)

if len(df)>0:

    for q in q_due.keys():
        if q in ['03', '06']:
            df.loc[(df['MM_DD'] > q_due[q][0]) & (df['MM_DD'] <= q_due[q][1]), 'PY'] = (df['YY'])
            df.loc[(df['MM_DD'] > q_due[q][0]) & (df['MM_DD'] <= q_due[q][1]), 'QQ'] = q
        elif q == '12':
            df.loc[(df['MM_DD'] > q_due[q][0]) & (df['MM_DD'] <= q_due[q][1]), 'PY'] = (df['YY'] - 1)
            df.loc[(df['MM_DD'] > q_due[q][0]) & (df['MM_DD'] <= q_due[q][1]), 'QQ'] = q
        else:
            df.loc[(df['MM_DD'] > q_due[q][0]) | (df['MM_DD'] <= q_due[q][1]), 'PY'] = (df['YY'] - 1)
            df.loc[(df['MM_DD'] > q_due[q][0]) | (df['MM_DD'] <= q_due[q][1]), 'QQ'] = q

    df.PY = df.PY.astype(int)
    df["DATE_FINAN"] = df["PY"].astype(str) + df["QQ"]



    is_df = df[['DATE', 'DATE_FINAN']].copy()

    if len(is_df) > 0 :
        print(is_df)
        db.insertdf(df[['DATE', 'DATE_FINAN']],'jazzdb.T_DATE_FINAN')

else:
    print("DONE~")