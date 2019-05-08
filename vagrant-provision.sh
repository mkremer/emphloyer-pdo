#!/usr/bin/env bash

export DEBIAN_FRONTEND=noninteractive

set -e

apt-get update
apt-get install -y php curl php-mysql mysql-server php-xml php-mbstring php-zip unzip

if [ ! -f /usr/local/bin/composer ]; then
  curl -sS https://getcomposer.org/installer | php -- --install-dir=/usr/local/bin
  chmod +x /usr/local/bin/composer.phar
  ln -s /usr/local/bin/composer.phar /usr/local/bin/composer
fi

cd /vagrant
su -c "cd /vagrant && composer install" vagrant

mysql -uroot -e 'CREATE DATABASE IF NOT EXISTS emphloyer_test'
