# RUNS ON 15:00 EVERY DAY, WILL RUN LINUX BATCH SCRIPT IF MARKET IS OPEN

MARKETOPEN=`python3 /workspace/jazzstock_crawl/market_open.py`

if [ "$MARKETOPEN" = "False" ]; then

    echo "MARKET CLOSED"
    
else

    sleep 2400
    python3 -u /workspace/jazzstock_crawl/telegram_alert.py "LINUX CRAWL_NAVER START"
    python3 -u /workspace/jazzstock_crawl/linux/01.crawl_naver_ohlc_day_and_shares.py
    sleep 1200
    python3 -u /workspace/jazzstock_crawl/telegram_alert.py "LINUX BATCH START"
    python3 -u /workspace/jazzstock_crawl/linux/02.batch_gen_bb.py
    python3 -u /workspace/jazzstock_crawl/linux/02.batch_gen_bb_event.py
    python3 -u /workspace/jazzstock_crawl/linux/02.batch_gen_futureprice.py
    python3 -u /workspace/jazzstock_crawl/linux/02.batch_gen_ma.py
    python3 -u /workspace/jazzstock_crawl/linux/02.batch_gen_ccr.py
    python3 -u /workspace/jazzstock_crawl/linux/02.batch_gen_mc.py
    python3 -u /workspace/jazzstock_crawl/linux/03.batch_gen_day_smar.py 
    python3 -u /workspace/jazzstock_crawl/linux/02.batch_gen_snd_analysis_shortterm.py &
    python3 -u /workspace/jazzstock_crawl/linux/02.batch_gen_snd_analysis_longterm.py
    python3 -u /workspace/jazzstock_crawl/linux/04.alerting.py
    # python3 -u /workspace/jazzstock_crawl/telegram_alert.py "LINUX END"
    
fi
