# # ==========================================================================================================================
# # CRAWL_SND_WINDOW_ISOLATED




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


done






