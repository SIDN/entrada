#!/usr/bin/env bash

echo "Processing:: [$(date)] :: Starting pcap processing"

while :
do
    cd /home/hadoop
    sudo cat entrada-latest/scripts/run/config.sh | source /dev/stdin

    echo "Processing:: [$(date)] :: Importing pcap files"
    sh entrada-latest/scripts/run/run_01_import_pcaps.sh

    if [ $? -eq 1 ]; then
        exit 0 #import will exit with code 1 if no files where downloaded
    fi

    echo "Processing:: [$(date)] :: Processing imported files"
    sh entrada-latest/scripts/run/run_02_partial_loader_bootstrap.sh

    if [ $(python3 entrada-latest/scripts/run/countInput.py $S3_BUCKET $S3_PATH"input/" 5) -lt 5 ]
    then
        echo "Processing:: [$(date)] :: Pcap processing complete"
        break
    fi
done

exit 0
