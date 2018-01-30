#!/bin/bash
# declare number
COUNTER=100
END=0
while [ $COUNTER -gt $END ]; do
    echo EOF >& /dev/pts/19
    echo $COUNTER >& /dev/pts/19
    let COUNTER=COUNTER-1
done
