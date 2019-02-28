crontab=$1

cd /home/hadoop

source entrada/scripts/run/config.sh

# download crontab
aws s3 cp $crontab crontab.txt

# create the cron table for hadoop
sudo bash -c "cat crontab.txt > /var/spool/cron/hadoop"
