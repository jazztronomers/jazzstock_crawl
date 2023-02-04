import common.connector_db as db
import sys
from datetime import datetime



if len(sys.argv) == 3:
    the_date = str(sys.argv[2])

else:
    the_date = str(datetime.now().date())



arg_dic = {"snd_day":"snd_day",
       "ohlc_min":"ohlc_min"}



if sys.argv[1] in arg_dic.keys():
    command_key = arg_dic[str(sys.argv[1])]
    what = '000'    
    
else:
    command_key = 'snd_win'
    what = str(sys.argv[1])

    
# print(command_key, the_date)

command_dic = {'snd_day':'SELECT DISTINCT STOCKCODE FROM jazzdb.T_STOCK_SND_DAY WHERE DATE = "%s"'%(the_date),
       'ohlc_min':'SELECT DISTINCT STOCKCODE FROM jazzdb.T_STOCK_OHLC_MIN WHERE DATE = "%s"'%(the_date),
       'snd_win':'SELECT DISTINCT STOCKCODE FROM jazzdb.T_STOCK_SND_WINDOW_ISOLATED WHERE DATE = "%s" AND WINCODE = "%s"'%(the_date, what)}

print(len(db.selectpd(command_dic[command_key])))
