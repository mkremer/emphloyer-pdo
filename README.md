# Emphloyer-PDO

This extension provides a PDO backend for
[Emphloyer](https://github.com/mkremer/emphloyer). This extension has only
been tested with MySQL but likely works with other SQL databases as well.

## Installation

You can install Emphloyer-PDO through composer with:

    composer require mkrmr/employer-pdo

To use Employer-PDO you need to install the [UUID pecl
extension](http://pecl.php.net/package/uuid).

## Usage

To use the PDO extension specify it as the backend in your configuration file
like so:

```php
$pipelineBackend = new
\Emphloyer\Pdo\PipelineBackend("mysql:dbname=emphloyer_example;host=localhost",
"user", "password");
```

You also need to create the emphloyer_jobs table, in a MySQL database you would
create the table like so:

```sql
CREATE table emphloyer_jobs (
   uuid VARCHAR(36) PRIMARY KEY, 
   created_at TIMESTAMP, 
   locked_at TIMESTAMP, 
   lock_uuid VARCHAR(36) UNIQUE, 
   status VARCHAR(20), 
   class_name VARCHAR(255), 
   type VARCHAR(100),
   attributes TEXT
);
```

## Contributing

1. Fork it
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create new Pull Request

