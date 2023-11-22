### 1.0.0-alpha1

* Update dependency mysql-connector-java to 8.0.33.
  NOTE: This is a breaking change, if the library configuration uses `:subname`
  to override the MySQL connection string. No change is necessary, if you use
  the `DB_*` environment variables for configuring the connection. For details,
  see [MySQL Connector/J configuration
  reference](https://dev.mysql.com/doc/connector-j/en/connector-j-reference-configuration-properties.html).
  At least, replace `useLegacyDatetimeCode=false` with
  `preserveInstants=true&connectionTimeZone=SERVER` in your connection strings.
* Replace dependency `com.github.shyiko/mysql-binlog-connector-java` with
  [successor](https://github.com/osheroff/mysql-binlog-connector-java)
  `com.zendesk/mysql-binlog-connector-java` 0.28.3.
* Add support for MySQL server 8.0.

### 0.2.2

* Fix issue with binlog streaming missing events when queries affect multiple
  rows

### 0.2.1

* Fix concurrency bug with multiple dumpr instances by fixing
  ->table-spec argument specs

### 0.2.0

* update dependency mysql-connector-java to 5.1.39
* update dependency mysql-binlog-connector-java to 0.4.1
* update default database connection settings to better work with MySQL 5.7

### 0.1.3

* update dependency java.jdbc to 0.6.1
* remove joplin as unecessary

### 0.1.2

* update main dependency mysql-binlog-connector-java to 0.3.1
* update other dependencies to latest versions (manifold and schema)


### 0.1.1-alpha1

* initial release
