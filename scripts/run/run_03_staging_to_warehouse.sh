#!/usr/bin/env bash

# info about
# this script

# get values for today's date
y=$(date -u "+%Y")
m=$(date -u "+%m")
m=${m#0}
d=$(date -u "+%d")

# Move all files in staging (except for the current day) to queries
s3-dist-cp /
    --src s3://dnspcaps/staging/ /
    --dest s3://dnspcaps/queries /
    # below regex merges and copies all files for every partition but ignores the current day
    --groupBy '.*/(?!year=$y/month=$m/day=$d/)year=([0-9]+)/month=([0-9]+)/day=([0-9]+)/server=(.*)/.*(\.parquet)' /
    --deleteOnSuccess

allObjs=$(hdfs dfs -find $S3_DNS_STAGING)
# remove the start of all paths
allObjs=${allObjs//$S3_DNS_STAGING\//}
# remove the source path itself
allObjs=${allObjs//$S3_DNS_STAGING/}
# finds any parquet files and returns their full path
files=$(hdfs dfs -find $S3_HOME/staging/ -name *.parquet)

pathCount=$(echo $allObjs | wc -w)
# go through backwards to avoid trying deleting a folder that's a subdirectory of a folder removed in a previous iteration
for ((i=$pathCount;i>=1;i--))
do
    # get a path from $allObjs
    path=$(echo $allObjs | cut -d" " -f $i)
    # check if the path is a part of any file's path
    if [[ $files != *$path* ]]
    then
        # if not, remove it
        echo "Removing $path"
        aws s3 rm $S3_HOME/staging/$path
    fi
done

# Drop all partitions and then repair metadata by finding those that still exist
# in the filesystem.
hive -e "
ALTER TABLE staging DROP PARTITION(year>'0', month>'0', day>'0', server>'0');
MSCK REPAIR TABLE staging;
"
