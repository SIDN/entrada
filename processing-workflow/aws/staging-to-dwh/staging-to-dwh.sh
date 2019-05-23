#!/usr/bin/env bash

#    This script is made to move data from Staging to DWH for Entrada-AWS
#
#    Copyright (C) 2019 Internetstiftelsen [https:/internetstiftelsen.se/en]
#
#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with this program.  If not, see <https://www.gnu.org/licenses/>.

# Note this script needs to be used in an EMR cluster with Hive to work
S3_STAGING=$1
S3_QUERIES=$2
STAGING_TABLE=$3
QUERIES_TABLE=$4
DATABASE=$5

# get values for today's date
y=$(date -u "+%Y")
m=$(date -u "+%m")
m=${m#0}
d=$(date -u "+%d")

# Move all files in staging (except for the current day) to queries
echo "Starting data migration"
s3-dist-cp --src $S3_STAGING --dest $S3_QUERIES --groupBy '.*/(?!year=$y/month=$m/day=$d/)(y)ear=([0-9]+)/(m)onth=([0-9]+)/(d)ay=([0-9]+)/server=(.*)/.*(\.parquet)' --deleteOnSuccess
# The above groupBy regex merges and copies all files for every partition but ignores the current day
echo "Data migration complete"

# allObjs=$(hdfs dfs -find $S3_DNS_STAGING)
# # remove the start of all paths
# allObjs=${allObjs//$S3_DNS_STAGING\//}
# # remove the source path itself
# allObjs=${allObjs//$S3_DNS_STAGING/}
# # finds any parquet files and returns their full path
# files=$(hdfs dfs -find $S3_HOME/staging/ -name *.parquet)
#
# pathCount=$(echo $allObjs | wc -w)
# # go through backwards to avoid trying deleting a folder that's a subdirectory of a folder removed in a previous iteration
# for ((i=$pathCount;i>=1;i--))
# do
#     # get a path from $allObjs
#     path=$(echo $allObjs | cut -d" " -f $i)
#     # check if the path is a part of any file's path
#     if [[ $files != *$path* ]]
#     then
#         # if not, remove it
#         echo "Removing $path"
#         aws s3 rm $S3_HOME/staging/$path
#     fi
# done

# Drop all partitions and then repair metadata by finding those that still exist
# in the filesystem.
echo "Updating partitions"
hive -e "
ALTER TABLE $DATABASE.$STAGING_TABLE DROP PARTITION(year>'0', month>'0', day>'0', server>'0');
MSCK REPAIR TABLE $DATABASE.$STAGING_TABLE;
MSCK REPAIR TABLE $DATABASE.$QUERIES_TABLE;
"
echo "Done"
