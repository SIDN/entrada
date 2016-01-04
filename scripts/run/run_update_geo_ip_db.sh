#!/usr/bin/env bash

############################################################
#
# Update the Maxmind GEO-IP databases
# 
############################################################

MAXMIND_DIR=$TMP_DIR/maxmind
#database are updated on the first Tuesday of each month. 
COUNTRY_URL=http://geolite.maxmind.com/download/geoip/database/GeoLite2-Country.mmdb.gz
ASN_URL=http://download.maxmind.com/download/geoip/database/asnum/GeoIPASNum.dat.gz
ASN_V6_URL=http://download.maxmind.com/download/geoip/database/asnum/GeoIPASNumv6.dat.gz

echo "[$(date)] : start update of Maxmind databases"
if [ ! -d $MAXMIND_DIR ];
then
    echo "[$(date)] : create directory $MAXMIND_DIR"
    mkdir $MAXMIND_DIR
fi

#goto download location
cd $MAXMIND_DIR

#remove old databases
rm -f *.dat
rm -f *.mmdb

echo "[$(date)] : fetch $COUNTRY_URL"
curl -sH 'Accept-encoding: gzip' $COUNTRY_URL | gunzip - > GeoLite2-Country.mmdb
echo "[$(date)] : fetch $ASN_URL"
curl -sH 'Accept-encoding: gzip' $ASN_URL | gunzip - > GeoIPASNum.dat
echo "[$(date)] : fetch $ASN_V6_URL"
curl -sH 'Accept-encoding: gzip' $ASN_V6_URL | gunzip - > GeoIPASNumv6.dat

echo "[$(date)] : Maxmind update done"
