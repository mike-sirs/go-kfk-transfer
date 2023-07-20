Kafka backup and trancfer solution 

## Build

`go build -o go-kfk-transfer ./cmd`

## Backup

`./go-kfk-transfer -s kafka-kafka-brokers.kafka.svc.cluster.local:9092 -st test.go-kfk-transfer-1 -b`

|Name|Flag|Default|Descr|
|------|---|------|-----|
|backup|b|false|Backup|
|backup_path|bp|/tmp|Backup path|


## Restore
`./go-kfk-transfer -d kafka-kafka-brokers.kafka.svc.cluster.local:9092 -dt test.go-kfk-transfer-1 -r -rp /tmp/test.go-kfk-transfer-1.gob `

|Name|Flag|Default|Descr|
|------|---|------|-----|
|restore|      r| false| Restore|
|restore_path| rp| /tmp| Restore path to file|

## Transfer

`./go-kfk-transfer -d kafka-kafka-brokers.kafka.svc.cluster.local:9092 -dt test.go-kfk-transfer-1 -s kafka-kafka-brokers.kafka.svc.cluster.local:9092 -st test.go-kfk-transfer`

|Name|Flag|Default|Descr|
|------|---|------|-----|
|src_host|    s| ""| "Source kafka brocker address with port
|dst_host|    d| ""| "Destination kafka brocker address with port.
|src_topic|    st| ""| "Source topic name
|dst_topic|    dt| ""| "Destination topic name