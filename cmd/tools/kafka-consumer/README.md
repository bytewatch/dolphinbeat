# About
`kafka-consumer` is a command tool to decode data in Kafka message which is written by `dolphinbeat`'s Kafka sink.
`kafka-consumer` decode the data from Protobuf, encode it with JSON and print it out.

# Usage
```
./kafka-consumer -b <broker_list>  -t <topic>  -p <partition> -o <offset> [ -scn -so --pretty ]

Options:
  -b string
        Kafka broker list
  -c int
        Kafka message count to consume
  -o int
        Kafka partition offset start to consume (default -1)
  -p int
        Kafka topic partition
  -pretty
        Print pretty json in results
  -s uint
        The last seq number until kafka message at offset-1
  -scn  
        Don't print column names in results
  -so   
        Don't print kafka offset info in results
  -t string
        Kafka topic name
```

# Example
The following example shows 4 operations(events) decoded from Kafka: GTID, BEGIN, WRITE_ROWS and COMMIT.
```
./kafka-consumer -b 172.17.0.4:9092  -t my_topic  -p 0 -o -2 -scn -so --pretty
{
  "header": {
    "server_id": 66693,
    "type": 1,
    "timestamp": 1545490602,
    "log_pos": 803732
  },
  "gtid": "e9d33d55-0e0f-11e8-827b-487b6b459a96:43156"
}
{
  "header": {
    "server_id": 66693,
    "type": 2,
    "timestamp": 1545490602,
    "log_pos": 803800
  }
}
{
  "header": {
    "server_id": 66693,
    "type": 4,
    "timestamp": 1545490602,
    "log_pos": 803916
  },
  "table": {
    "database": "test",
    "name": "test1",
    "columns": [
      {
        "name": "id",
        "sql_type": "int(10) unsigned",
        "inner_type": 3,
        "unsigned": true,
        "key": "PRI"
      },
      {
        "name": "name",
        "sql_type": "longtext",
        "inner_type": 251,
        "charset": "utf8mb4"
      },
      {
        "name": "addr",
        "sql_type": "varchar(255)",
        "inner_type": 15,
        "key": "MUL",
        "charset": "utf8mb4"
      },
      {
        "name": "phone",
        "sql_type": "bigint(20)",
        "inner_type": 8
      }
    ]
  },
  "rows": [
    {
      "after": [
        {
          "value": "255"
        },
        {
          "value": "xiaoluobo"
        },
        {
          "value": "beijing"
        },
        {
          "value": "123456789"
        }
      ]
    }
  ]
}
{
  "header": {
    "server_id": 66693,
    "type": 3,
    "timestamp": 1545490602,
    "log_pos": 803943
  },
  "progress": {
    "flavor": "mysql",
    "server_id": 66693,
    "log_name": "mysql-bin.000008",
    "log_pos": 803943,
    "gset": "e9d33d55-0e0f-11e8-827b-487b6b459a96:1-43156"
  }
}

```
