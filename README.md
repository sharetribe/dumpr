<img src="dumpr.png" title="Dumpr" alt="Dumpr logo"/>

Dumpr is a Clojure library for live replicating data from a MySQL
database. Data is made available as a stream of upsert and deletes of
table rows. These streams are exposed as
[Manifold](https://github.com/ztellman/manifold) sources which can be
consumed directly or easily coerced into core.async channels et al.

The API consists of two main operations, initial table load and live
streaming starting from a given binary log position. Both operations
expose the same data abstraction, an ordered stream of upserts and
deletes (described in more detail below).

This library is work in progress and has bugs. All interfaces must be
considered subject to change.

## Usage

TODO Add a leiningen dependency vector once alpha-x published.

### Initial load

In order to replicate a data set from MySQL we first start by loading the contents fof a set of desired tables.

```clojure
> (require '[dumpr.core :as dumpr])
nil

;; Create dumpr configuration. See docstring for other options.
> (def conf (dumpr/create-conf {:user "root" :password "not-root" :host "127.0.0.1" :port 3306 :db "sharetribe_development" :server-id 111}))
#'user/conf

;; Create a table load stream. Tables are loaded and contents returned
;; in given order.
> (def load (dumpr/create-table-stream [:people :addresses] conf))
#'user/load

;; Grab the Manifold source that will receive the results.
> (def source (dumpr/source table-stream))
#'user/source

;; Starts the table load operation. Have source consumer setup before
;; calling this point to avoid unnecessary backpressure.
> (dumpr/start-stream! table-stream)
true
```

### Binary log streaming

Binary log streaming is started from the supplied binary log
position. A binary log position can be grabbed from the table load
stream to continue streaming new updates that happen after the initial
load operation. Normally you would then persist the binary log
position from the stream as you consume updates and use that latest
position to restart streaming after process restarts. A stored
position can be validated against the source database to make sure
it's still available. Binary log retention policy for MySQL is
configurable.


```clojure
;; Grab the binary log position. It's a plain clojure map.
> (def binlog-pos (dumpr/next-position table-stream))
#'user/binlog-pos
> binlog-pos
{:file "host-bin.000001", :position 1259353}

;; Log position can be validated
> (dumpr/valid-binlog-pos? conf binlog-pos)
true

;; Create a stream using previous position
> (def binlog-stream (dumpr/create-binlog-stream conf binlog-pos))
#'user/binlog-stream
> (def source (dumpr/source binlog-stream))
#'user/source
> (dumpr/start-stream! binlog-stream)
true

;; Unlike table stream binlog stream can be closed as part of clean
;; shutdown.
(dumpr/stop-stream! binlog-stream)
true
```


### Row format

The output of both stream operations is a stream of upserts and
deletes of database table rows. The rows are represented as tuples (vectors): `[op-type table id content metadata]`.

| Field       | Description |
--------------|-------------|
| **op-type** | :upsert or :delete |
| **table**   | The database table of the row as keyword, example :people |
| **id**      | Id of the row. By default this is the value of row primary key that the logic can be overridden by passing per table id functions via dumpr/create-conf. See docstring for full explanation. |
| **content** | The full content of the row as map that is inserted or updated after the operation, or that was just deleted in case of :delete |
| **metadata** | Only used by binlog stream (`nil` for table stream rows). This is a map like: `{:ts #inst "2015-08-03T10:32:53.000-00:00" :next-position 123 :next-file "host-bin.00001"}`. The ts is a timestamp from MySQL when the binlog event was created. The next-filename and next-position are the binlog position for continuing streaming after this row. |

These tuples are fairly convenient to consume either using vector
destructuring or then using the
[core.match](https://github.com/clojure/core.match) library.

## Development

Testing and development require a working MySQL instance that is setup
for replication. This requires that you enable binlog and specify a
server id. Binlog format has to be set to ROW for streaming to
work. For example:

```bash
$ mysqld --log-bin --server-id=5 --binlog_format=ROW
```

The server-id has to be something other than what the tests use. By
default tests use server-id `123`.

### Dev environment

For development purposes the dev code includes a
[Component](https://github.com/stuartsierra/component) based
system. The configuration for which server and database to connect
must be defined in conf/dumpr-dev-configuration.edn. You can create
your own setup by copying the default configuration,
conf/dumpr-lib-configuration.edn and overriding the keys you want to
change.

When the configuration is in order you can start the dev system by running `(reset)` in user namespace.

### Running tests locally

Tests need a test database to use. By default this database is
`dumpr_test_db_123`. Create a test db and user:

```bash
$ mysql -u root
mysql> create database dumpr_test_db_123;
mysql> grant all privileges on dumpr_test_db_123.* to 'dumpr_test'@'localhost' identified by 'dumpr_test';
mysql> grant replication client on *.* to 'dumpr_test'@'localhost';
mysql> grant replication slave on *.* to 'dumpr_test'@'localhost';
```

You can also do this by running the included script:

```bash
mysql -u root < setup_test_db.sql
```

These default settings are stored in
config/dumpr-lib-configuration.edn. If you wish to use different
database connection parameters, test database name or database user
you can override any of the default settings by creating a
config/dumpr-test-configuration.edn and defining the configurations
you wish to override there.

Finally, run the tests with: `lein test`

## License and Copyright

Copyright © 2015 Sharetribe Ltd.

Logo contributed by Janne Koivistoinen.

Distributed under [The Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)

