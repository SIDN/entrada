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
