before_cnt=9999
after_cnt=0


for THEDATE in "2022-08-05"
do
    echo $THEDATE

    before_cnt=9999
    after_cnt=0
    while [ "$after_cnt" != "$before_cnt" ] || [ "$after_cnt" -lt 2500 ] || [ "$exit_code" != "0" ]
    do
        before_cnt=`python3 /workspace/jazzstock_crawl/counter.py snd_day $THEDATE`
        timeout 200 ssh Administrator@astronomers.cafe24.com python -u c://workspace/jazzstock_crawl/windows/crawl_snd_basic.py
        exit_code=$?
        echo "EXIT CODE: $exit_code"
        after_cnt=`python3 /workspace/jazzstock_crawl/counter.py snd_day $THEDATE`
        echo " * SND_DAY AFTER: $before_cnt, $after_cnt"
        sleep 5
     done


done












