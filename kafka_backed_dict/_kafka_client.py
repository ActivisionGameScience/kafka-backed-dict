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
            if not isinstance(key, bytes):
                raise TypeError('producing to kafka requires key to be raw bytes')
            if not isinstance(val, bytes) and val is not None:
                raise TypeError('producing to kafka requires val to be raw bytes or None')
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
            position = max(self.c.position([prt])[0].offset - 1, -1)  # if never read before then call returns -1001 for some reason
            if last_offset > position:
                partitions[partition] = last_offset

        # process partitions up to watermarks (but remember that we already consumed a message, so need to yield that)
        while first_message or len(partitions) > 0:
            if not first_message:
                msg = self.c.poll(timeout=10.0)
            else:
                first_message = False
            if msg == None or msg.error():  # NOTE:  "if not msg" checks if message len = 0, which is different from checking "if msg == None"
                continue  # ignore errors
            partition = msg.partition()
            if partition in partitions and msg.offset() >= partitions[partition]:  # first check is because we might read past the watermark 
                                                                                   # for a partition that we're already done with... but that's ok
                del partitions[partition]
            yield msg.key(), msg.value(), msg.timestamp()[1]

    def __del__(self):
        self.flush_producer()
        if self.c:
            self.c.close()
