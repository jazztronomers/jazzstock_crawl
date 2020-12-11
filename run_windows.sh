# RUNS ON 15:00 EVERY DAY, WILL RUN LINUX BATCH SCRIPT IF MARKET IS OPEN

option=$1


MARKETOPEN=`python3 /workspace/jazzstock_crawl/market_open.py`

if [ "$MARKETOPEN" = "False" ] && [ "$option" != "k" ]; then

    echo "MARKET CLOSED"

else


    sleep 2400 # 40MIN



    ## CHECK MARKET OPEN DAY OR NOT
    ## SLEEP UNTIL MARKET CLOSE

    echo " * BOOT"
    timeout 200 ssh Administrator@astronomers.cafe24.com python -u c://workspace/jazzstock_crawl/windows/boot.py
    echo "EXIT CODE: $?"
    echo " * BOOTED"

    # ==========================================================================================================================
    ## CRAWL_WINDOWS_KIWOOM_SND_DAY

    before_cnt=9999
    after_cnt=0
    while [ "$after_cnt" != "$before_cnt" ] || [ "$after_cnt" -lt 2200 ] || [ "$exit_code" != "0" ]
    do
        before_cnt=`python3 /workspace/jazzstock_crawl/counter.py snd_day`
        timeout 200 ssh Administrator@astronomers.cafe24.com python -u c://workspace/jazzstock_crawl/windows/crawl_snd_basic.py
        exit_code=$?
        echo "EXIT CODE: $exit_code"
        after_cnt=`python3 /workspace/jazzstock_crawl/counter.py snd_day`
        echo " * SND_DAY AFTER: $before_cnt, $after_cnt"
        sleep 5
     done


    timeout 200 ssh Administrator@astronomers.cafe24.com python -u c://workspace/jazzstock_crawl/windows/date_idx_update.py
    sleep 2

    # # ==========================================================================================================================
    # # CRAWL_WINDOWS_KIWOOM_OHLC_MIN

    before_cnt=9999
    after_cnt=0

    while [ "$after_cnt" != "$before_cnt" ] || [ "$after_cnt" -lt 2200 ] || [ "$exit_code" != "0" ]
    do
        before_cnt=`python3 /workspace/jazzstock_crawl/counter.py ohlc_min`
        timeout 200 ssh Administrator@astronomers.cafe24.com python -u c://workspace/jazzstock_crawl/windows/crawl_ohlc_5min.py
        exit_code=$?
        echo "EXIT CODE: $exit_code"
        after_cnt=`python3 /workspace/jazzstock_crawl/counter.py ohlc_min`
        echo "OHLC_MIN: $before_cnt, $after_cnt"
        sleep 2
    done



    echo " * CRAWL_INDEX"
    timeout 200 ssh Administrator@astronomers.cafe24.com python -u c://workspace/jazzstock_crawl/windows/crawl_index.py
    echo "EXIT CODE: $?"
    echo " * CRAWL_INDEX DONE"
    
    
    for WINCODE in 61 58 43 35 41 36 42 44 45 54 33 06 03 37 
    do

    before_cnt=9999
    after_cnt=0

    while [ "$after_cnt" != "$before_cnt" ] || [ "$after_cnt" -lt 2200 ] || [ "$exit_code" != "0" ]
    do
        before_cnt=`python3 /workspace/jazzstock_crawl/counter.py $WINCODE`
        timeout 200 ssh Administrator@astronomers.cafe24.com python -u c://workspace/jazzstock_crawl/windows/crawl_forwin.py $WINCODE
        exit_code=$?
        echo "EXIT CODE: $exit_code"
        after_cnt=`python3 /workspace/jazzstock_crawl/counter.py $WINCODE`
        echo "FORWIN: $WINCODE, $before_cnt, $after_cnt"
        sleep 2
    done

    echo " * MERGE WINDOWS"
    timeout 200 ssh Administrator@astronomers.cafe24.com python -u c://workspace/jazzstock_crawl/windows/crawl_merge_forwin.py
    echo "EXIT CODE: $?"
    echo " * CRAWL_INDEX DONE"

done

fi
