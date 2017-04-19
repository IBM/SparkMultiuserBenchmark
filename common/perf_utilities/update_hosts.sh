HOSTS=`cat /perf_test/perf_harness/agent_list`

for LINE in $HOSTS
do
ssh root@$LINE "cp /etc/hosts /etc/hosts.bak"
scp /perf_test/perf_utilities/hosts root@$LINE:/etc/ 
done
