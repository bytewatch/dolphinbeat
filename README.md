
# DolphinBeat [![Build Status](https://travis-ci.org/bytewatch/dolphinbeat.svg?branch=master)](https://travis-ci.org/bytewatch/dolphinbeat) 

![](https://img.shields.io/github/license/bytewatch/dolphinbeat.svg)

> Other languages: [中文](./README.zh-cn.md)

This is a high available server that pulls MySQL binlog, parses binlog and pushs incremental update data into different sinks.

The types of sink supported currently and officially are [Kafka](#Kafka) and [Stdout](#Stdout).

Features:
* Supports MySQL and MariaDB.
* Supports GTID and not GTID.
* Supports MySQL failover: if using GTID, `dolphinbeat` can work smoothly even if MySQL failover.
* Supports MySQL DDL: `dolphinbeat` can parse DDL statement and replay DDL upon it's own schema data in memory.
* Supports breakpoint resume: `dolphinbeat` has persistent metadata, it can resume to work after crash recover.
* Supports standalone and election mode: if election enabled, `dolphinbeat` follower will take over dead leader.
* Supports filter rules base on database and table for each sink.
* Supports HTTP API to inspect `dolphinbeat`.
* Supports metrics in Prometheus style.

> The types of sink are scalable, you can implement your own sink if need, but I recommend you to use Kafka sink and let business consumes data from Kafka.

# Quick start
Prepare your MySQL source, trun on binlog with ROW format, and type following commands and you will see JSON printed by `dolphinbeat`'s Stdout sink.
```
docker run -e MYSQL_ADDR='8.8.8.8:3306' -e MYSQL_USER='root' -e MYSQL_PASSWORD='xxx' bytewatch/dolphinbeat
{
  "header": {
    "server_id": 66693,
    "type": "rotate",
    "timestamp": 0,
    "log_pos": 0
  },
  "next_log_name": "mysql-bin.000008",
  "next_log_pos": 4
}
...
...
```
The docker image above is for MySQL with GTID and only with Stdout sink enabled.
> If your source database is not GTID, please add `-e GTID_ENABLED='false'` arg.
> If your source database is MariaDB, please add `-e FLAVOR='mariadb'` arg.

If you want to have a deep test, type following commands and you will get a shell:
```
docker run -e MYSQL_ADDR='8.8.8.8:3306' -e MYSQL_USER='root' -e MYSQL_PASSWORD='xxx' sh
``` 
In this shell, you can modify configurations in /data directory, and then start `dolphinbeat` manually. 

Configuration description is presented in [Wiki](https://github.com/bytewatch/dolphinbeat/wiki/Configuration).

# Compile from source
Type following commands and you will get builded binary distribution at build/dolphinbeat directory:
```
go get github.com/bytewatch/dolphinbeat
make 
```
# Documents
* [Configuration](https://github.com/bytewatch/dolphinbeat/wiki/Configuration)
* [HTTP API](github.com/bytewatch/dolphinbeat/wiki/HTTP-API)

# Sink
## Kafka
This is a sink used for production. `Dolphinbeat` write data encoded with Protobuf into Kafka and business consumes data from Kafka. 

Business need use [client library](sink/kafka/client) to decode data in Kafka message, do stream processing on the binlog stream. 

The Protobuf protocol is presented in [protocol.proto ](sink/kafka/protocol). 

Kafka sink has following features:
* Strong-ordered delivery: business will receive events in the same order with MySQL binlog. 
* Exactly-once delivery: client library can dedup duplicated message with same sequence number which may caused by producer retry or Kafka failover.
* Unlimited event size: `dolphinbeat` use fragments algorithm like IPV4 if the binlog event is bigger than Kafka's max message size.

A small example using client library is presented in [kafka-consumer](cmd/tools/kafka-consumer).

`kafka-consumer` is a command tool to  decode data in Kafka message and print out with JSON.


## Stdout
This is a sink used for demonstration. `Dolphinbeat` write data encoded with JSON to Stdout.

> Stdout sink doesn't support breakpoint resume.

# Special thanks
Thank siddontang for his popular and powerful `go-mysql` library!

# License
[Apache License 2.0](https://github.com/bytewatch/dolphinbeat/blob/master/LICENSE)
