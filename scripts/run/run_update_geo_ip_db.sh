#!/usr/bin/env bash

# ENTRADA, a big data platform for network data analytics
# 
# Copyright (C) 2016 SIDN [https://www.sidn.nl]
#  
# This file is part of ENTRADA.
# 
# ENTRADA is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# ENTRADA is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#  
# You should have received a copy of the GNU General Public License
# along with ENTRADA.  If not, see [<http://www.gnu.org/licenses/].

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
    mkdir -p $MAXMIND_DIR
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
