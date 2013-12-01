#!/usr/bin/env bash

export DEBIAN_FRONTEND=noninteractive

set -e

apt-get update
apt-get -y upgrade

apt-get install -y php5 curl php5-mysql mysql-server uuid uuid-dev build-essential php-pear

curl -sS https://getcomposer.org/installer | php -- --install-dir=/usr/local/bin
chmod +x /usr/local/bin/composer.phar
ln -s /usr/local/bin/composer.phar /usr/local/bin/composer

cd /usr/local/bin
wget https://phar.phpunit.de/phpunit.phar
chmod +x phpunit.phar
ln -s phpunit.phar phpunit

printf "\n" | pecl install uuid
echo 'extension=uuid.so' > /etc/php5/conf.d/uuid.ini

cd /vagrant
composer install

mysql -uroot -e 'CREATE DATABASE IF NOT EXISTS emphloyer_test'
