# DolphinBeat [![Build Status](https://travis-ci.org/bytewatch/dolphinbeat.svg?branch=master)](https://travis-ci.org/bytewatch/dolphinbeat)
 
![](https://img.shields.io/github/license/bytewatch/dolphinbeat.svg)

DolphinBeat是从MySQL拉取binlog、解析binlog并将增量内容发送到下游系统（sink）的高可用后台程序。

目前官方支持的sink是[Kafka](#Kafka)和[Stdout](#Stdout)。

功能：
* 支持MySQL和MariaDB。
* 支持GTID和非GTID。
* 支持容忍MySQL failover：如果启用了GTID，当MySQL发生failover，`dolphinbeat`也可以平滑运行。
* 支持MySQL DDL：`dolphinbeat`能够解析DDL语句，能够基于内存中的schema结构重放DDL构造新的schema结构。
* 支持断点续传：`dolphinbeat`具有持久化的元信息，所以崩溃重启之后能够断点续传继续从中断的位点继续拉取binlog。
* 支持standalone模式和election模式：如果开启了election模式，`dolphinbeat` follower会取代故障的leader，达到高可用的目的。
* 支持过滤规则：基于database和table名字，每个sink可以有不同的过滤规则。
* 支持HTTP管理API：用于查看、管理`dolphinbeat`的状态。
* 支持Prometheus风格的metrics。

> 支持的下游系统（sink）的类型是可以扩展的，所以你可以实现你自己的sink，比如写到ES、MySQL。但是推荐你使用Kafka sink，将MySQL增量更新写入到Kafka并让业务从Kafka 消费数据。

# 快速开始
首先，准备好你的MySQL源数据库，打开binlog并且设置为ROW格式。
然后，执行下面的命令，你就会看到`dolphinbeat`通过Stdout sink打印出来的JSON格式数据：
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
上面的docker镜像是默认以GTID方式拉取binlog的，并且只开启了Stdout sink。

> 如果你的源数据库没有开启GTID， 那么请添加`-e GTID_ENABLED='false'` 参数。
> 如果你的源数据库是MariaDB分之， 那么请添加`-e FLAVOR='mariadb'` 参数.

如果你想深度试用一下，执行下面的命令，然后你会获得一个终端:
```
docker run -e MYSQL_ADDR='8.8.8.8:3306' -e MYSQL_USER='root' -e MYSQL_PASSWORD='xxx' sh
``` 
在这个终端里面，你可以修改/data目录下的配置文件，然后手工启动`dolphinbeat`。

配置项描述可以在 [Wiki](https://github.com/bytewatch/dolphinbeat/wiki/Configuration)中查看。

# 源码编译
执行如下命令之后，你就可以在build/dolphinbeat目录中看到二进制文件：

```
go get github.com/bytewatch/dolphinbeat
make 
```

# 文档
* [Configuration](https://github.com/bytewatch/dolphinbeat/wiki/Configuration)
* [HTTP API](github.com/bytewatch/dolphinbeat/wiki/HTTP-API)

 

# Sink
## Kafka
这是用于生产级别的sink。`Dolphinbeat`会将数据通过Protobuf编码之后写入到Kafka中，然后业务可以从Kafka中消费数据。

业务需要使用 [client library](sink/kafka/client) 来解析Kafka消息，并可以基于此做流式处理。
Protobuf 协议的定义可以查看 [protocol.proto ](sink/kafka/protocol)。
 
Kafka sink 有以下特性:
* 顺序投递：业务从Kafka消费得到的binlog事件顺序与MySQL binlog中的事件顺序是一致的。
* Exactly-once 投递：通过client library 可以对重复的消息去重（重复消息的产生一般是因为producer重试或者Kafka failover导致的）。
* 无限大的事件：`dolphinbeat` 运用了类似与 IPV4 的分包算法，如果binlog事件超过了Kafka的最大消息限制，将会触发分包。
 
使用client library 的一个小例子可以查看 [kafka-consumer](cmd/tools/kafka-consumer)。

`kafka-consumer` 是一个命令行工具，用于解析Kafka中的数据，解析之后以JSON格式打印到终端。

## Stdout
这是一个用于示范的sink. `Dolphinbeat` 以JSON格式写数据到Stdout。
 
> Stdout sink不支持断点续传。
 
# 特别鸣谢
非常感谢siddontang的功能强大的 `go-mysql` 库!
 
# License
[Apache License 2.0](https://github.com/bytewatch/dolphinbeat/blob/master/LICENSE)

