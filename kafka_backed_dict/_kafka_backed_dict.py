from ._kafka_client import KafkaClient
from . import _settings

import os
import ujson
from base64 import b64encode, b64decode
import rocksdb

import time
from uuid import uuid4
import logging


logger = logging.getLogger(__name__)
logger.setLevel(_settings.LOG_LEVEL)
logger.addHandler(logging.StreamHandler())


class KafkaBackedDict(object):

    def __init__(self, 
                 kafka_bootstrap_servers,
                 kafka_topic,
                 partition=None,
                 use_rocksdb=True,
                 db_dir=None,
                 catchup_delay_seconds=30,
                 guid=None):
        self._kafka_bootstrap_servers = kafka_bootstrap_servers
        self._kafka_topic = kafka_topic
        if partition:
            raise RuntimeError("multiple partitions not supported yet")
        self._use_rocksdb = use_rocksdb
        self._db_dir = db_dir
        self._catchup_delay_seconds = catchup_delay_seconds

        # if not specified then use cwd
        if not self._db_dir:
            self._db_dir = os.getcwd()

        # this guid will be used everywhere
        self.guid = guid
        if not guid:
            self.guid = str(uuid4())

        # open kafka
        self._kafka = KafkaClient(self._kafka_bootstrap_servers,
                                 self._kafka_topic,
                                 self.guid)

        # open db (either ordinary dict or rocksdb)
        if self._use_rocksdb:
            self._db_path = os.path.join(self._db_dir, "rocksdb-" + self.guid)
            self._db = rocksdb.DB(self._db_path, rocksdb.Options(create_if_missing=True))
        else:
            self._db = {}

        # we will read from kafka every few seconds
        self._last_catchup = 0

    def __getitem__(self, key):
        if not isinstance(key, bytes):
            key = str(key).encode()

        self._catchup()

        if self._use_rocksdb:
            val = self._db.get(key)
            if not val:
                raise KeyError(key)
            return self._decode_val(val)
        else:
            val = self._db[key]
            return self._decode_val(val)

    def _decode_val(self, val):
        val = ujson.loads(val.decode('utf-8'))
        # has format [timestamp_ms=int, base64_encoded=0 or 1, data]
        if val[1] == 1:
            return b64decode(val[2])
        else:
            return val[2]

    def __setitem__(self, key, val):
        if not isinstance(key, bytes):
            key = str(key).encode()
        val = self._encode_val(val)

        # produce to kafka
        self._kafka.produce(key, val)

        # write locally
        if self._use_rocksdb:
            self._db.put(key, val)
        else:
            self._db[key] = val

    def _encode_val(self, val):
        if isinstance(val, bytes):
            val = [int(time.time()*1000), 1, b64encode(val)]
        else:
            val = [int(time.time()*1000), 0, val]
        val = ujson.dumps(val, ensure_ascii=False).encode('utf-8')
        return val

    def __delitem__(self, key):
        if not isinstance(key, bytes):
            key = str(key).encode()

        # produce tombstone to kafka
        self._kafka.produce(key, b'__delete_key__')

        # delete locally
        if self._use_rocksdb:
            self._db.delete(key)
        else:
            del(self._db[key])

    def keys(self):
        self._catchup()

        if self._use_rocksdb:
            it = self._db.iterkeys()
            it.seek_to_first()

            return it
        else:
            return self._db.keys()

    def __iter__(self):
        for k in self.keys():
            yield k

    def _catchup(self):
        if time.time() - self._last_catchup < self._catchup_delay_seconds:
            return
        self._last_catchup = time.time()
        self._kafka.flush_producer()
        for key, val, ts_millis in self._kafka.consume():
            #print("Received %s=%s at ts=%d" % (key, val, ts_millis))
            if self._use_rocksdb:
                if val != b'__delete_key__':
                    self._db.put(key, val)
                else:  # received tombstone
                    self._db.delete(key)
            else:
                if val != b'__delete_key__':
                    self._db[key] = val
                else:
                    del(self._db[key])
