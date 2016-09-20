DROP DATABASE IF EXISTS dumpr_test_db_123;
CREATE DATABASE dumpr_test_db_123;
GRANT ALL PRIVILEGES on dumpr_test_db_123.* to 'dumpr_test'@'%' identified by 'dumpr_test';
GRANT REPLICATION CLIENT on *.* to 'dumpr_test'@'%';
GRANT REPLICATION SLAVE on *.* to 'dumpr_test'@'%';
Set GLOBAL binlog_format = 'ROW';
