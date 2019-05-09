
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

echo "[$(date)] : Starting Processing bootstrap"

cd /home/hadoop
#load config
echo "[$(date)] : Loading config"
source entrada-latest/scripts/run/config.sh 2> /dev/null

#create directories for processing
echo "[$(date)] : Making pcap directories"
sudo mkdir -p $PROCESSING_DIR
sudo mkdir $DATA_DIR/processed
sudo ln -s $PROCESSING_DIR $DATA_DIR/processing

# Turns out aws does mounts disks asigned before startup, so no need for below code
# (it won't do anything and it's for an old version of the config)
#
# # Find an unpartitioned disk and mount it on $DATA_DIR. Will use whichever unpartitioned
# # disk it finds first, so if more than 2 EBS volumes are attached it's possible it might
# # pick any device which isnt root.
# fsNames=$(sudo lsblk -f -o NAME -l)
# fsNames=${fsNames#NAME}
# for name in fsNames
# do
#   # check that $name is a disk not a partition, sfdisk -d gives the same output (none) and
#   # error when the target is a partition as it does when the target is an unpartitioned disk
#   # therefore first check that target is not named like a partition
#   if [[ $name != *p*([0-9]) ]] && [ $(sudo sfdisk -d /dev/$name 2>&1) == "" ];
#   then
#     sudo mkfs -t xfs /dev/$name
#     sudo mount /dev/$name $DATA_DIR
#     break
#     # just to make sure
#   fi
# done

# for some reason below code did not work when giving a relative path (for the source), wont debug further for now since absolute path works
sudo sh -c 'source /home/hadoop/entrada-latest/scripts/run/config.sh && sh ./entrada-latest/scripts/run/run_update_geo_ip_db.sh'

sudo chown -R hadoop:hadoop ./
sudo chmod -R 770 ./
sudo chown -R hadoop:hadoop $PROCESSING_DIR
sudo chmod -R 770 $PROCESSING_DIR
