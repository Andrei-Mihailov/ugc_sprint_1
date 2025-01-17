-- Нода 3
CREATE DATABASE shard;
CREATE TABLE shard.test (id Int64, event_time DateTime) Engine=ReplicatedMergeTree('/clickhouse/tables/shard2/test', 'replica_1') PARTITION BY toYYYYMMDD(event_time) ORDER BY id;
CREATE TABLE default.test (id Int64, event_time DateTime) ENGINE = Distributed('company_cluster', '', test, rand());

-- Нода 4
CREATE DATABASE replica;
CREATE TABLE replica.test (id Int64, event_time DateTime) Engine=ReplicatedMergeTree('/clickhouse/tables/shard2/test', 'replica_2') PARTITION BY toYYYYMMDD(event_time) ORDER BY id;

