# Fraud producer, consumer, topic/s and event/s
## Topic configs
kubectl exec -it kafka-0 -- kafka-topics \
  --bootstrap-server kafka-service:9092 \
  --create \
  --topic <topic name> \
  --partitions <partion number> \
  --replication-factor <replication factor> \
  --config retention.ms= <retention period> \
  --config cleanup.policy= <cleanup policy> \
  --config min.insync.replicas= <minimum in-sync replicas> \
  --config compression.type= <compression algorithm> \
  --config max.message.bytes= <max message size>

  ### Consumer configs

kubectl exec -it kafka-0 -- kafka-console-consumer \
  --bootstrap-server kafka-service:9092 \
  --topic digitalpayments.payment.initiated \
  --property parse.key=true \
  --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
  --property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
  --group fraud.scoring.service \
  --property max.poll.records= <value> \
  --property session.timeout.ms=<value>  \ 
  --property heartbeat.interval.ms=<value>  \
  --property auto.offset.reset=<value>  \ 
  --property enable.auto.commit=<value>  \
  --property auto.commit.interval.ms=<value>  \
  --property max.poll.interval.ms=<value>  \
  --property fetch.min.bytes=<value>  \  
  --property fetch.max.wait.ms=<value>  \  
  --property max.partition.fetch.bytes=<value>  \ 
  --property partition.assignment.strategy=<value> 


### Producer configs

kubectl exec -it kafka-0 -- kafka-console-producer \
  --bootstrap-server kafka-service:9092 \
  --topic <topic name> \
  --property parse.key=true \
  --property key.separator=: \
  --property key.serializer=org.apache.kafka.common.serialization.StringSerializer \
  --property value.serializer=org.apache.kafka.common.serialization.StringSerializer \
  --property acks=<value> \
  --property retries=<value>  \
  --property max.in.flight.requests.per.connection=<value>  \
  --property enable.idempotence=<value>  \
  --property compression.type= <value>  \
  --property linger.ms=<value>  \
  --property batch.size=<value>  \
  --property delivery.timeout.ms=<value>  \
  --property request.timeout.ms=<value> 
