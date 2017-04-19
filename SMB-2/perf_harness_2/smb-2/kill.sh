ps -ef|grep stream | grep -v grep | awk '{print $2} {print $3}' | xargs kill -9
