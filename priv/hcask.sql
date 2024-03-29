--
--   Copyright 2012 Dmitry Kolesnikov, All Rights Reserved
--
--   Licensed under the Apache License, Version 2.0 (the "License");
--   you may not use this file except in compliance with the License.
--   You may obtain a copy of the License at
--
--       http://www.apache.org/licenses/LICENSE-2.0
--
--   Unless required by applicable law or agreed to in writing, software
--   distributed under the License is distributed on an "AS IS" BASIS,
--   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
--   See the License for the specific language governing permissions and
--   limitations under the License.
--
SET storage_engine=InnoDB;

--
--
DROP   DATABASE hcask;
CREATE DATABASE hcask;

--
--
CREATE TABLE IF NOT EXISTS hcask.kv(
   `key` INT NOT NULL,
   `val` VARCHAR(1024),
   PRIMARY KEY(`key`)
) ROW_FORMAT=DYNAMIC;

--
--
CREATE TABLE IF NOT EXISTS hcask.mkv(
      `a`  INT NOT NULL,
      `b`  INT NOT NULL,
      `c`  INT NOT NULL,  
      `val` VARCHAR(1024),
      PRIMARY KEY(`a`, `b`, `c`)
) ROW_FORMAT=DYNAMIC;

--
--
CREATE TABLE IF NOT EXISTS hcask.kv0(
   `key` INT NOT NULL,
   `val` VARCHAR(1024),
   PRIMARY KEY(`key`)
) ROW_FORMAT=DYNAMIC;

--
--
CREATE TABLE IF NOT EXISTS hcask.kv1(
   `key` INT NOT NULL,
   `val` VARCHAR(1024),
   PRIMARY KEY(`key`)
) ROW_FORMAT=DYNAMIC;

--
-- test type serialization
CREATE TABLE IF NOT EXISTS hcask.types(
   uid        int NOT NULL,
   `bit`      bit(8),
   `bool`     boolean,
   u8         tinyint unsigned,
   u16        smallint unsigned,
   u24        mediumint unsigned,
   u32        int unsigned,
   u64        bigint unsigned,
   flt        float,
   dbl        double,
   chr        char(30),
   vchr      varchar(30),
   bin        binary(22),
   vbin       varbinary(30),   
   blb        blob,

   PRIMARY KEY(uid)
) ROW_FORMAT=DYNAMIC


