#!/bin/bash
yum update -y
yum install git -y
git clone https://github.com/yesports/yesports-marketplace-indexer.git
cd yesports-marketplace-indexer
curl -sL https://rpm.nodesource.com/setup_16.x | bash -
yum install -y nodejs
amazon-linux-extras enable postgresql10
yum install -y postgresql
cp .env.example .env
sed -i 's/<DBUSER>/xxxxx/' ./.env
sed -i 's/<DBPASS>/xxxxx/' ./.env
sed -i 's/<DBHOST>/xxxxx/' ./.env
sed -i 's/<DBNAME>/xxxxx/' ./.env
npm i
(crontab -l 2>/dev/null; echo "0 * * * * /home/ec2-user/yesports-marketplace-indexer/get_collections.sh") | crontab -
npm install -g pm2
npm config set strict-ssl=false
export NODE_TLS_REJECT_UNAUTHORIZED='0'
pm2 install pm2-logrotate
pm2 set pm2-logrotate:max_size 50M
pm2 set pm2-logrotate:retain 10
pm2 set pm2-logrotate:compress true
pm2 start indexer.js -- --chain=polygon
pm2 start indexer.js -- --chain=ethereum
mkdir deletions
mkdir old-deletions
touch deletions.txt
(crontab -l 2>/dev/null; echo "30 * * * * /home/ec2-user/yesports-marketplace-indexer/cleanup/listingCleanup.sh") | crontab -
echo STARTUP COMPLETE