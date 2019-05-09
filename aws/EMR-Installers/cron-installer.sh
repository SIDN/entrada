
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

crontab=$1

cd /home/hadoop

source entrada/scripts/run/config.sh

# download crontab
aws s3 cp $crontab crontab.txt

# create the cron table for hadoop
sudo bash -c "cat crontab.txt > /var/spool/cron/hadoop"

#create log dir and set up logrotate
sudo mkdir -p /var/log/entrada
sudo chown hadoop:hadoop /var/log/entrada
sudo cat > /etc/logrotate.d/entrada << EOF
/var/log/entrada/*.log {
  size 10k
  daily
  maxage 10
  compress
  missingok
}
EOF
