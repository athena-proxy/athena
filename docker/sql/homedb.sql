-- home database
CREATE DATABASE IF NOT EXISTS athena;
-- seed table for GlobalId
CREATE TABLE IF NOT EXISTS athena.dal_sequences (
  id bigint(20) NOT NULL AUTO_INCREMENT COMMENT 'physical id',
  seq_name varchar(255) NOT NULL COMMENT 'physical seed name',
  last_value bigint(20) NOT NULL DEFAULT 0 COMMENT 'current seed value',
  is_deleted tinyint(4) NOT NULL DEFAULT 0 COMMENT 'soft delete flag',
  created_at datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'create time',
  updated_at datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'update time',
  PRIMARY KEY(id),
  UNIQUE KEY uk_seq_name(seq_name),
  KEY ix_created_at(created_at),
  KEY ix_updated_at(updated_at)
) DEFAULT CHARACTER SET utf8 COMMENT 'GlobalId seed table';
-- insert init seed for develop
INSERT INTO athena.dal_sequences(seq_name, last_value)
VALUES
  ('common_seq_example_id_dev', 1),
  ('composed_seq_example_id_dev', 976562500000000),
  ('common_seq_example_id_az1', 1000000000000000000),
  ('composed_seq_example_id_az1', 1),
  ('common_seq_example_id_az2', 2000000000000000000),
  ('composed_seq_example_id_az2', 1953125000000000);