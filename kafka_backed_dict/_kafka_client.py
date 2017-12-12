from confluent_kafka import Producer, Consumer, KafkaError
from uuid import uuid4


class KafkaClient(object):

    def __init__(self, kafka_bootstrap_servers, kafka_topic, guid=None, partition=None):
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.kafka_topic = kafka_topic
        if partition:
            raise NotImplementedError("multiple partitions not supported yet")
        self.guid = guid
        if not self.guid:
            self.guid = str(uuid4())

        self.p = None
        self.c = None

    def produce(self, key, val):
        try:
            if not self.p:
                self.p = Producer({'bootstrap.servers': self.kafka_bootstrap_servers, 'api.version.request': True})
            if not isinstance(key, bytes) or not isinstance(val, bytes):
                raise TypeError('producing to kafka requires key/val to both be raw bytes')
            self.p.produce(topic=self.kafka_topic, value=val, key=key)
        except BufferError:
            self.p.flush()
            self.p.produce(topic=self.kafka_topic, value=val, key=key)

    def flush_producer(self):
        if self.p:
            self.p.flush()

    def consume(self):
        if not self.c:
            self.c = Consumer({'bootstrap.servers': self.kafka_bootstrap_servers,
                               'group.id': self.guid,
                               'api.version.request': True,
                               'log.connection.close': False,
                               'socket.keepalive.enable': True,
                               'session.timeout.ms': 6000,
                               'default.topic.config': {'auto.offset.reset': 'smallest'}})
            self.c.subscribe([self.kafka_topic])
            
        # must perform an initial poll to get partition assignments
        first_message = True
        msg = self.c.poll(timeout=10.0)
        
        # grab watermarks from partition
        partitionobjs = self.c.assignment()
        partitions = {}
        for prt in partitionobjs:
            partition = prt.partition
            last_offset = self.c.get_watermark_offsets(prt)[1] - 1
            if last_offset < 0:  # if nothing in partition then this will be -1
                continue
            position = max(self.c.position([prt])[0].offset - 1, 0)  # if never read before then position=-1001 for some stupid reason
            if last_offset > position:
                partitions[partition] = last_offset

        while len(partitions) > 0:
            if not first_message:
                msg = self.c.poll(timeout=10.0)
            else:
                first_message = False
            if not msg or msg.error():
                continue  # ignore errors
            partition = msg.partition()
            if partition in partitions and msg.offset() >= partitions[partition]:  # first check is because race conditions might happen
                del partitions[partition]
            yield msg.key(), msg.value(), msg.timestamp()[1]

    def __del__(self):
        self.flush_producer()
        if self.c:
            self.c.close()
