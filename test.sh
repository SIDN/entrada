#!/usr/bin/env bash

# gather paths to all files and folders in source
allObjs=$(hdfs dfs -find s3://markus-emr-entrada/entrada/staging/)
# remove the start of all paths
allObjs=${allObjs//s3:\/\/markus-emr-entrada\/entrada\/staging\//}
# remove the source path itself
allObjs=${allObjs//s3:\/\/markus-emr-entrada\/entrada\/staging/}

files=$(hdfs dfs -find s3://markus-emr-entrada/entrada/staging/ -name *.parquet)# finds any parquet files and returns their full path

pathCount=$(echo $allObjs | wc -w)
for ((i=$pathCount;i>=1;i--))
do
  path=$(echo $allObjs | cut -d" " -f $i)
  if [[ $files != *$path* ]]
  then
    echo "$path is not files"
  else
    echo $i
  fi
done
