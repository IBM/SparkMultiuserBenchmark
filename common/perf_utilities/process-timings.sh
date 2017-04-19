#!/bin/bash
# ------------------------------------------------------------------
# [amatevos] Process Timing Data
#          This will process timing data file
#
#           They typically look like this:
            # Starting run 0
            # 08:45:26 Timing info for run 0:  real 0m26.352s user 0m11.907s sys 0m1.843s
            # Starting run 1
            # 08:45:54 Timing info for run 1:  real 0m23.445s user 0m11.883s sys 0m1.744s
# ------------------------------------------------------------------

if [ "$#" -eq "0" ]
then
     echo "Usage ./process-timings.sh <DIRECTORY with timings data>"
     exit 2
fi

DIRECTORY=$1
PROCESSED_STREAM_RESULTS=$DIRECTORY/processed-stream-results.csv

#set up columns in our csv file
echo "start_time,runtime" > $PROCESSED_STREAM_RESULTS

for filename in $DIRECTORY/single-stream-results*.txt; do
    #echo "$filename"
    cat $filename | while read -r line ; do
        #echo "Processing $line"
        # your code goes here

        # start_time=`echo $line | awk -F" " '{print $1}'`
        # echo "*** $start_time"

        # echo " . "
        timing=`echo $line |  awk -F" " '{print $2}'`
        #echo $timing

        #if line contains 'Timing', then process the timing data
        if [ "$timing" = 'Timing' ]
        then

            # echo -n "start_time"
            # echo $line |  egrep -o '[0-9]{2}:[0-9]{2}:[0-9]{2}'

            # #get start time of run
            # #08:52:26
            start_time=`echo $line |  awk -F" " '{print $1}'`

            # get minute portion of run
            #1
            runtime_minute=`echo $line |  awk '{print $8}' | awk -F"m" '{print $1}' `

            #8.326s
            #runtime_seconds=`grep Timing $fn | head -1 |  awk -F" " '{print $8}' | awk -F"m" '{print $2}' `; echo $p

            #8.326
            #runtime_seconds=`echo $line |  awk -F" " '{print $8}' | awk -F"m" '{print $2}' | awk -F"s" '{print $1}' `
            runtime_seconds=`echo $line | awk '{print $8}' | grep -o "[0-9]\+\.[0-9]\+"`

            if (( $runtime_minute > 0 ))
            then
            	#total_seconds = "$runtime_seconds" + "$runtime_minute" * 60
                runtime_seconds=`echo "$runtime_seconds + $runtime_minute * 60" | bc`
            fi

            # echo $line
            #echo "$start_time,$runtime_seconds"
            echo "$start_time,$runtime_seconds" >> $PROCESSED_STREAM_RESULTS

        fi #END if [ "$timing" = 'Timing' ]

    done #END cat $filename | while read -r line ; do

done #END for filename in single-stream-results*.txt; do
