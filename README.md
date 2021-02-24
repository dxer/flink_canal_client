# flink_canal_client
flink_canal_client


```yaml
app.name: example
thread.num: 5
flink.parallelism.num: 1
flink.checkpoint.interval: 30000

# source
source.kafka.bootstrap.servers: ip1:9092,ip2:9092,ip3:9092
source.kafka.group.id: example
source.kafka.topics:
  - { topic: test1, startup.mode: earliest }
  - { topic: test2, startup.mode: latest }
  - { topic: test3, startup.mode: group }
  - { topic: test4, startup.mode: specific, startup.specific-offsets: 'partition0:42,partition1:300' }



# sink
sink.rdb.jdbc.url: jdbc:mysql://127.0.0.1:3306?useUnicode=true&characterEncoding=UTF-8&serverTimezone=GMT%2B8&useSSL=false&autoReconnect=true&connectTimeout=120000&socketTimeout=12000
sink.rdb.jdbc.driver: com.mysql.jdbc.Driver
sink.rdb.jdbc.username: root
sink.rdb.jdbc.password: root

# task
tasks:
  - { source: { db: a, table: test1 }, target: { db: b, table: test1 } }
  - { source: { db: a, table: test2 }, target: { db: b, table: test2 } }
  - { source: { db: a, table: test3 }, target: { db: b, table: test3 } }

```