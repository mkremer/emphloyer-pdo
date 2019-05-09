## 0.6.0 (May 9, 2019)
   
  - Vagrant configuration updated to use bento/ubuntu-18.04 box with PHP 7.2
    If you are developing Emphloyer-PDO with the including configuration you will 
    need to: vagrant destroy && vagrant up
  - Updated to PHPUnit 8  
  - Added the Doctrine coding standard as a development dependency. You can now 
    use phpcs and phpcbf using the included configuration.
    Updated a large portion of the source to adhere to the Doctrine coding 
    standard
    This includes the following BC breaks:
    - More strict use of types
    - Internal exceptions have been renamed
  - Moved the source from library to src
  
## 0.5.0 (December 19, 2018)

  - Updated to work with Emphloyer 0.5.0
  - Switch to the Hashicorp Ubuntu 12.04 Vagrant box (due to composer issues
    with the official box).

## 0.4.1 (September 28, 2014)

  - Allow setting table names so you can run multiple backends on the same
    database, but with different database tables
  - Use the official Ubuntu 12.04 Vagrant box

## 0.4.0 (May 18, 2014)

  - Updates to work with Emphloyer 0.4.0 to support management of Scheduler

## 0.3.0 (May 14, 2014)

  - Updates to work with Emphloyer 0.3.0, including Scheduler functionality

## 0.2.0 (May 11, 2014)

  - Updates to work with Emphloyer 0.2.0

## 0.1.1 (November 8, 2013)

  - When resetting a job also save attributes in case they have changed

## 0.1.0 (November 7, 2013)

Initial release.
