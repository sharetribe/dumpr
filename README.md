# dumpr

A Clojure library for consuming MySQL contents as a stream of updates.

This library is work in progress.

## Usage

FIXME

## Running tests locally

Tests require a working MySQL instance that is setup for
replication. This requires that you enable binlog and specify a server
id. For example:

```bash
$ mysqld --log-bin --server-id=5 --binlog_format=ROW
```

The server-id has to be something other than what the tests use. By
default tests use server-id `123`.

Tests also need a test database to use. By default this database is
`dumpr_test_db_123`. Create a test db and user:

```bash
$ mysql -u root
mysql> create database dumpr_test_db_123;
mysql> grant all privileges on dumpr_test_db_123.* to 'dumpr_test'@'localhost' identified by 'dumpr_test';
mysql> grant replication client on *.* to 'dumpr_test'@'localhost';
```

You can also do this by running the included script:

```bash
mysql -u root < setup_test_db.sql
```

These default settings are stored in
config/dumpr-lib-configuration.end. If you wish to use different
database connection parameters, test database name or database user
you can override any of the default settings by creating a
config/dumpr-test-configuration.edn and defining the configurations
you wish to override there.

Finally, run the tests with: `lein test`

## License

Copyright © 2015 Sharetribe Ltd.

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
