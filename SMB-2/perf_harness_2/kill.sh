#for i in `seq 15 25`; do echo red$i; ssh red$i "ps -ef|grep -i stream | grep -v grep | awk '{print $2} {print $3}' | xargs kill -9"; done
ps -ef|grep -i stream | grep -v grep | awk '{print $2} {print $3}' | xargs kill -9
ps -ef | grep -i spark | grep -v grep | awk '{print $2}' | xargs kill -9
