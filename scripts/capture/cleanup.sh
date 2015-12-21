#!/usr/bin/env bash

SCRIPT_DIR=$(dirname "$0")
source $SCRIPT_DIR/config.sh

find $DUMPDIR -ctime $DAYS_TO_KEEP -exec rm {} \;