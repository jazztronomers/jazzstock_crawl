from jazzstock_bot.common import connector_db as db
from datetime import datetime
from jazzstock_bot.common.telegram import JazzstockTelegram

todays_date = str(datetime.now().date())
recent_date = str(db.selectpd("SELECT * FROM jazzdb.T_DATE_INDEXED WHERE CNT = 0").DATE.values[0])


trial_seconds = 0
while True:
    
    if not todays_date == recent_date:
        sleep(10)
        trial_seconds += 10
    
        if trial_seconds > 1500:
            JazzstockTelegram.send_message_telegram("waiting over 1500 seconds...")
            break
            
    else:
        break
        
        
    
