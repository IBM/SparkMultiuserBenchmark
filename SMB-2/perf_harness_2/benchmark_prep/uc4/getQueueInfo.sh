source /opt/profile.yarn

while true; do
echo "============================="
date
yarn queue -status BATCH
yarn queue -status INTERACTIVE
sleep 180
done

